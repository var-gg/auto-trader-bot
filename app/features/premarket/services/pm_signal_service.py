# app/features/premarket/services/pm_signal_service.py
"""
Pre-market Best Signal Service

핵심 비즈니스 로직:
1. Config 조회
2. OHLCV 데이터 조회
3. Shape/Context 벡터 생성
4. pgvector ANN 검색
5. Softmax + Odds Ratio 신호 계산
6. DB UPSERT
"""
from __future__ import annotations
import json
import logging
import time
import numpy as np
from datetime import date, datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import text, func, select, desc, literal
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import array

from app.features.premarket.utils.vector_builder import (
    build_shape_vector,
    build_context_vector,
    cosine_similarity,
    pgvector_to_numpy
)
from app.features.premarket.models.pm_signal_models import (
    UpdatePMSignalsRequest,
    UpdatePMSignalsResponse,
    SignalSample,
    GetPMSignalsResponse,
    PMSignalItem,
    TestPMSignalRequest,
    TestPMSignalResponse,
    ANNMatchItem
)


logger = logging.getLogger(__name__)


class PMSignalService:
    """PM Best Signal 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def update_signals(self, request: UpdatePMSignalsRequest) -> UpdatePMSignalsResponse:
        """
        장전 신호 계산 및 업데이트
        
        Args:
            request: 업데이트 요청
            
        Returns:
            업데이트 응답
        """
        start_time = time.time()
        
        # 1. Config 조회
        config_id = self._get_latest_config_id()
        config = self._load_config(config_id)
        
        logger.info(f"Using config_id: {config_id}")
        
        # 2. 기준 날짜
        if request.anchor_date:
            anchor_date = datetime.strptime(request.anchor_date, '%Y-%m-%d').date()
        else:
            anchor_date = date.today()

        run_id: Optional[int] = None
        if not request.dry_run:
            run_id = self._insert_signal_run_header(
                anchor_date=anchor_date,
                config_id=config_id,
                country=request.country,
                executed_at=datetime.now(timezone.utc)
            )
        
        # 3. 티커 목록 조회
        logger.info(f"Request params: tickers={request.tickers}, country={request.country}")
        tickers = self._get_tickers(request.tickers, request.country)
        
        logger.info(f"Processing {len(tickers)} tickers for date {anchor_date}")
        
        # 4. Context Vector 한 번만 생성 (모든 티커 공유)
        logger.info("Building context vector (shared for all tickers)...")
        try:
            q_ctx = build_context_vector(self.db, anchor_date, config)
            logger.info(f"Context vector created: dim={len(q_ctx)}")
        except Exception as e:
            logger.error(f"Failed to build context vector: {e}", exc_info=True)
            raise ValueError(f"Context vector generation failed: {e}")
        
        # 5. 각 티커 처리
        results = []
        errors = []
        success_count = 0
        failed_count = 0
        
        for ticker_id, symbol, company_name in tickers:
            try:
                result = self._process_ticker(
                    ticker_id, symbol, company_name,
                    config_id, config, anchor_date, q_ctx, request.dry_run, run_id
                )
                
                if result['success']:
                    results.append(result)
                    success_count += 1
                else:
                    errors.append(result)
                    failed_count += 1
            except Exception as e:
                logger.error(f"Error processing ticker {symbol}: {e}", exc_info=True)
                errors.append({'symbol': symbol, 'error': str(e)})
                failed_count += 1
        
        elapsed = time.time() - start_time
        
        # ✅ 적재 완료 후 프로시저 호출 (dry_run=false일 때만)
        procedure_executed = False
        if not request.dry_run and success_count > 0:
            try:
                logger.info(f"Calling stored procedure: update_pm_best_signal({config_id})")
                
                # ⚠️ Stored procedure가 내부에서 COMMIT을 수행하므로
                # 별도의 연결에서 autocommit 모드로 실행
                connection = self.db.connection()
                connection.execute(text("COMMIT"))  # 현재 트랜잭션 종료
                
                # Autocommit 모드로 stored procedure 호출
                connection.execute(text("CALL trading.update_pm_best_signal(:config_id)"), 
                                 {"config_id": config_id})
                
                procedure_executed = True
                logger.info(f"✅ Stored procedure completed successfully")
            except Exception as e:
                logger.error(f"❌ Stored procedure call failed: {e}", exc_info=True)
                # ⚠️ 프로시저 실패는 치명적 - Cloud Scheduler가 재시도하도록 예외 발생
                raise RuntimeError(f"Stored procedure execution failed: {e}")
        
        # ✅ 실제 작업 검증: 데이터가 적재되었는지 확인
        if not request.dry_run and success_count > 0 and not procedure_executed:
            logger.error("❌ No trading executed - procedure was not called")
            raise RuntimeError("PM signal update incomplete - procedure not executed")
        
        return UpdatePMSignalsResponse(
            success=True,
            config_id=config_id,
            anchor_date=str(anchor_date),
            results={
                'total': len(tickers),
                'success': success_count,
                'failed': failed_count,
                'no_signal': 0
            },
            elapsed_seconds=elapsed,
            samples=[SignalSample(**r) for r in results[:5]],  # 샘플 5개
            errors=errors if errors else None
        )
    
    def get_signals(
        self,
        limit: int = 100,
        min_signal: Optional[float] = None,
        max_signal: Optional[float] = None,
        order: str = "signal_desc"
    ) -> GetPMSignalsResponse:
        """
        저장된 신호 조회
        
        Args:
            limit: 개수 제한
            min_signal: 최소 신호값
            max_signal: 최대 신호값
            order: 정렬 방식
            
        Returns:
            조회 응답
        """
        where_clauses = []
        params = {}
        
        if min_signal is not None:
            where_clauses.append("signal_1d >= :min_signal")
            params['min_signal'] = min_signal
        
        if max_signal is not None:
            where_clauses.append("signal_1d <= :max_signal")
            params['max_signal'] = max_signal
        
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        order_map = {
            'signal_desc': 'signal_1d DESC',
            'signal_asc': 'signal_1d ASC',
            'updated_desc': 'updated_at DESC'
        }
        order_sql = order_map.get(order, 'signal_1d DESC')
        
        params['limit'] = limit
        
        result = self.db.execute(text(f"""
            SELECT ticker_id, symbol, company_name, signal_1d, best_target_id, updated_at
            FROM trading.pm_best_signal
            {where_sql}
            ORDER BY {order_sql}
            LIMIT :limit
        """), params)
        
        rows = result.fetchall()
        
        signals = [
            PMSignalItem(
                ticker_id=r[0],
                symbol=r[1],
                company_name=r[2],
                signal_1d=float(r[3]),
                best_target_id=r[4],
                updated_at=r[5].isoformat() if r[5] else ""
            )
            for r in rows
        ]
        
        return GetPMSignalsResponse(
            success=True,
            count=len(signals),
            signals=signals
        )
    
    def _get_latest_config_id(self) -> int:
        """최신 config_id 조회 (promoted 우선, 없으면 최신 config)"""
        result = self.db.execute(text("""
            SELECT id FROM trading.optuna_vector_config
            WHERE status IN ('promoted', 'draft')
            ORDER BY 
                CASE WHEN status = 'promoted' THEN 0 ELSE 1 END,
                promoted_at DESC NULLS LAST, 
                created_at DESC
            LIMIT 1
        """))
        
        row = result.fetchone()
        if not row:
            raise ValueError("No config found in optuna_vector_config table")
        
        return row[0]
    
    def _load_config(self, config_id: int) -> dict:
        """Config 파라미터 로드"""
        result = self.db.execute(text("""
            SELECT lookback, m, w_price, w_volume, w_candle, w_meta,
                   candle_mode, include_candle_meta,
                   alpha, beta, tau_softmax, threshold, topn,
                   macro_window, macro_cols, macro_lag_days
            FROM trading.optuna_vector_config
            WHERE id = :config_id
        """), {"config_id": config_id})
        
        row = result.fetchone()
        if not row:
            raise ValueError(f"Config {config_id} not found")
        
        config = {
            'lookback': row[0],
            'm': row[1],
            'w_price': row[2],
            'w_volume': row[3],
            'w_candle': row[4],
            'w_meta': row[5],
            'candle_mode': row[6],
            'include_candle_meta': row[7],
            'alpha': row[8],
            'beta': row[9],
            'tau_softmax': row[10],
            'threshold': row[11],
            'topN': row[12],
            'macro_window': row[13],
            'macro_cols': row[14] if row[14] else [],  # JSON array
            'macro_lag_days': row[15] if row[15] is not None else 1,  # ✅ 배치 기본값: 1
            'tail_weight': 0.0  # ⚠️ DB에 컬럼 없음 - 강제 설정
        }
        
        # 라이브러리 차원 조회 (검증용)
        cfg_table = f"trading.target_vecidx_cfg_{config_id}"
        
        try:
            result = self.db.execute(text(f"""
                SELECT 
                    vector_dims(shape_embedding) AS shape_dim,
                    vector_dims(ctx_vec) AS ctx_dim
                FROM {cfg_table}
                LIMIT 1
            """))
            
            row = result.fetchone()
            if row:
                config['shape_dim'] = int(row[0])
                config['ctx_dim'] = int(row[1])
                logger.info(f"Library dimensions: shape={row[0]}, ctx={row[1]}")
            else:
                logger.warning(f"No data in {cfg_table}, skipping dimension check")
                config['shape_dim'] = None
                config['ctx_dim'] = None
        except Exception as e:
            logger.warning(f"Could not query library dimensions: {e}")
            config['shape_dim'] = None
            config['ctx_dim'] = None
        
        return config
    
    def _get_tickers(
        self,
        ticker_symbols: Optional[List[str]],
        country: Optional[str]
    ) -> List[Tuple[int, str, str]]:
        """티커 목록 조회"""
        where_clauses = ["t.type IN ('stock', 'etf')"]
        params = {}
        
        # 🔍 디버깅 로그
        logger.info(f"_get_tickers called: ticker_symbols={ticker_symbols}, country={country}")
        
        if ticker_symbols:
            placeholders = ','.join([f':symbol_{i}' for i in range(len(ticker_symbols))])
            where_clauses.append(f"t.symbol IN ({placeholders})")
            for i, symbol in enumerate(ticker_symbols):
                params[f'symbol_{i}'] = symbol
        
        if country:
            where_clauses.append("t.country = :country")
            params['country'] = country
            logger.info(f"Country filter applied: {country}")
        else:
            logger.info("No country filter - querying all countries")
        
        where_sql = " AND ".join(where_clauses)
        
        sql = f"""
            SELECT t.id, t.symbol,
                   COALESCE(
                       (SELECT name FROM trading.ticker_i18n 
                        WHERE ticker_id = t.id AND lang_code = 'ko' LIMIT 1),
                       (SELECT name FROM trading.ticker_i18n 
                        WHERE ticker_id = t.id AND lang_code = 'en' LIMIT 1),
                       ''
                   ) AS company_name
            FROM trading.ticker t
            WHERE {where_sql}
            ORDER BY t.id
        """
        
        # 🔍 디버깅: 실행될 SQL 로그
        logger.info(f"Executing SQL: {sql}")
        logger.info(f"Parameters: {params}")
        
        result = self.db.execute(text(sql), params)
        tickers = result.fetchall()
        
        # 🔍 디버깅: 조회된 티커 통계
        logger.info(f"Retrieved {len(tickers)} tickers")
        if len(tickers) > 0:
            # 국가별 통계
            from collections import Counter
            # 실제 ticker 객체에서 country 확인
            countries_query = self.db.execute(text("""
                SELECT t.country, COUNT(*) 
                FROM trading.ticker t
                WHERE t.id IN :ticker_ids
                  AND t.type IN ('stock', 'etf')
                GROUP BY t.country
            """), {"ticker_ids": tuple([t[0] for t in tickers])})
            
            country_stats = dict(countries_query.fetchall())
            logger.info(f"Country distribution: {country_stats}")
        
        return tickers
    
    def _fetch_ohlcv(
        self,
        ticker_id: int,
        until_date: date,
        lookback: int
    ) -> np.ndarray:
        """
        OHLCV 데이터 조회
        
        Returns:
            (N, 5) array: [open, high, low, close, volume]
        """
        result = self.db.execute(text("""
            SELECT open, high, low, close, volume
            FROM trading.ohlcv_daily
            WHERE ticker_id = :ticker_id
              AND trade_date < :until_date
              AND is_final = true
            ORDER BY trade_date DESC
            LIMIT :lookback
        """), {
            "ticker_id": ticker_id,
            "until_date": until_date,
            "lookback": lookback
        })
        
        rows = result.fetchall()
        if len(rows) < lookback:
            raise ValueError(f"Insufficient data: {len(rows)} < {lookback}")
        
        # 역순 정렬 (오래된 것 → 최근)
        data = np.array(rows[::-1], dtype=np.float32)
        return data
    
    def _ann_search(
        self,
        config_id: int,
        direction: str,
        q_shape: np.ndarray,
        until_date: date,
        topN: int
    ) -> List[dict]:
        """
        pgvector ANN 검색 + CPU Re-ranking
        
        Args:
            config_id: Config ID
            direction: 'UP' or 'DOWN'
            q_shape: Shape vector (normalized)
            until_date: 룩어헤드 방지 날짜
            topN: 반환 개수
            
        Returns:
            List of {symbol, anchor_date, cos, ctx_vec, tb_label, iae_1_3}
        """
        cfg_table = f"trading.target_vecidx_cfg_{config_id}"
        
        # pgvector 파라미터
        limit = topN
        probe = 525
        
        # pgvector 설정
        self.db.execute(text("SET LOCAL enable_seqscan = off;"))
        self.db.execute(text(f"SET LOCAL ivfflat.probes = {probe};"))
        
        # ✅ numpy array → list로 변환 (배치와 동일)
        vec_param = q_shape.astype(np.float32).tolist()
        
        # SQL 쿼리
        sql = f"""
            SELECT 
                symbol, anchor_date, 
                shape_embedding <=> %s::vector AS dist,
                shape_embedding,
                ctx_vec, tb_label, iae_1_3
            FROM {cfg_table}
            WHERE direction = %s
              AND anchor_date < %s
            ORDER BY shape_embedding <=> %s::vector ASC
            LIMIT %s
        """
        
        # psycopg2로 직접 실행 (SQLAlchemy text() 대신)
        with self.db.connection().connection.cursor() as cur:
            cur.execute(sql, (vec_param, direction, until_date, vec_param, limit))
            rows = cur.fetchall()
        
        results = []
        for row in rows:
            # shape_embedding을 numpy array로 변환 (배치와 동일)
            shape_vec = pgvector_to_numpy(row[3])
            shape_vec = shape_vec / (np.linalg.norm(shape_vec) + 1e-12)
            
            # 정확한 코사인 계산 (CPU)
            cos_exact = float(np.dot(q_shape, shape_vec))
            
            # ctx_vec도 동일하게 처리
            ctx_vec = pgvector_to_numpy(row[4]) if row[4] else np.zeros(10, dtype=np.float32)
            
            results.append({
                'symbol': row[0],
                'anchor_date': row[1],
                'cos': cos_exact,
                'ctx_vec': ctx_vec,
                'tb_label': row[5],
                'iae_1_3': float(row[6]) if row[6] else 0.0
            })
        
        # Re-ranking (정확한 코사인 순)
        results.sort(key=lambda r: r['cos'], reverse=True)
        return results[:topN]
    
    def _full_scan_search(
        self,
        config_id: int,
        direction: str,
        q_shape: np.ndarray,
        until_date: date,
        topN: int
    ) -> List[dict]:
        """
        전체 스캔 검색 (ANN 없이 정확한 코사인 거리 계산)
        
        Args:
            config_id: Config ID
            direction: 'UP' or 'DOWN'
            q_shape: Shape vector (normalized)
            until_date: 룩어헤드 방지 날짜
            topN: 반환 개수
            
        Returns:
            List of {symbol, anchor_date, cos, ctx_vec, tb_label, iae_1_3}
        """
        logger.info(f"🔍 Full scan search: config_id={config_id}, direction={direction}, topN={topN}")
        
        cfg_table = f"trading.target_vecidx_cfg_{config_id}"
        
        # ✅ numpy array → list로 변환
        vec_param = q_shape.astype(np.float32).tolist()
        
        # ✅ SQLAlchemy로 코사인 거리 직접 계산 (전체 스캔)
        # cosine_distance는 PostgreSQL 함수 (pgvector extension)
        # similarity = 1.0 - cosine_distance
        
        sql = f"""
            SELECT 
                symbol, anchor_date,
                (1.0 - cosine_distance(shape_embedding, %s::vector)) AS similarity,
                shape_embedding,
                ctx_vec, tb_label, iae_1_3
            FROM {cfg_table}
            WHERE direction = %s
              AND anchor_date < %s
            ORDER BY similarity DESC
            LIMIT %s
        """
        
        # psycopg2로 직접 실행
        with self.db.connection().connection.cursor() as cur:
            cur.execute(sql, (vec_param, direction, until_date, topN))
            rows = cur.fetchall()
        
        logger.info(f"✅ Full scan found {len(rows)} results")
        
        results = []
        for row in rows:
            # shape_embedding을 numpy array로 변환
            shape_vec = pgvector_to_numpy(row[3])
            shape_vec = shape_vec / (np.linalg.norm(shape_vec) + 1e-12)
            
            # 정확한 코사인 계산 (CPU)
            cos_exact = float(np.dot(q_shape, shape_vec))
            
            # ctx_vec도 동일하게 처리
            ctx_vec = pgvector_to_numpy(row[4]) if row[4] else np.zeros(10, dtype=np.float32)
            
            results.append({
                'symbol': row[0],
                'anchor_date': row[1],
                'cos': cos_exact,
                'ctx_vec': ctx_vec,
                'tb_label': row[5],
                'iae_1_3': float(row[6]) if row[6] else 0.0
            })
        
        return results
    
    def _logsumexp_tau(self, scores: np.ndarray, tau: float) -> float:
        """
        Temperature-scaled log-sum-exp (배치와 동일)
        
        Args:
            scores: 점수 배열
            tau: Temperature
            
        Returns:
            log(sum(exp(scores / tau)))
        """
        tau = max(float(tau), 1e-6)
        a = np.asarray(scores, dtype=np.float64) / tau
        if a.size == 0:
            return -np.inf
        m = float(a.max())
        return m + np.log(np.exp(a - m).sum() + 1e-12)
    
    def _compute_signal(
        self,
        up_ranked: List[dict],
        dn_ranked: List[dict],
        q_ctx: np.ndarray,
        config: dict
    ) -> Tuple[float, dict, str]:
        """
        신호 계산 (Log-Sum-Exp 방식 - 배치와 동일)
        
        배치 로직:
        1. UP/DOWN 각각 재랭킹 (70개씩)
        2. 전체 70개의 점수로 log-sum-exp 계산 (증거량)
        3. margin = logU - logD
        4. p_raw = sigmoid(margin)
        5. sign * (2*p_raw - 1)
        
        Returns:
            (signal_1d, best_target_info, reason)
        """
        alpha = config['alpha']
        beta = config['beta']
        tau = config['tau_softmax']
        threshold = config['threshold']
        
        # 가중치 정규화
        sum_w = alpha + beta
        if sum_w <= 0.0:
            sum_w = 1.0
        
        alpha_norm = alpha / sum_w
        beta_norm = beta / sum_w
        
        # 재랭킹 (Shape + Context)
        def rerank(candidates):
            for c in candidates:
                ctx_vec = c['ctx_vec'] / (np.linalg.norm(c['ctx_vec']) + 1e-12)
                cos_ctx = float(np.dot(q_ctx, ctx_vec))
                cos_shape = c['cos']
                # ⚠️ 배치와 동일: alpha는 shape, beta는 context
                c['score'] = alpha_norm * cos_shape + beta_norm * cos_ctx
                c['cos_ctx'] = cos_ctx
            candidates.sort(key=lambda c: c['score'], reverse=True)
            return candidates
        
        # 방향 태그 추가
        for r in up_ranked:
            r['direction'] = 'UP'
        for r in dn_ranked:
            r['direction'] = 'DOWN'
        
        up_ranked = rerank(up_ranked)
        dn_ranked = rerank(dn_ranked)
        
        # ========================================================================
        # Log-Sum-Exp 방식 (배치와 동일 - 70개 전체 사용)
        # ========================================================================
        sU = np.array([r['score'] for r in up_ranked], dtype=np.float32)
        sD = np.array([r['score'] for r in dn_ranked], dtype=np.float32)
        
        # 양측 증거량 (공유 스케일)
        logU = self._logsumexp_tau(sU, tau)
        logD = self._logsumexp_tau(sD, tau)
        
        # 증거량이 모두 -inf면 신호 무효
        if np.isinf(logU) and np.isinf(logD):
            raise ValueError("No valid candidates")
        
        # Margin 기반 계산
        margin = logU - logD
        margin = np.clip(margin, -50, 50)  # 안정성
        
        # 확률 계산
        p_up = float(1.0 / (1.0 + np.exp(-margin)))  # sigmoid(margin)
        p_down = 1.0 - p_up
        
        # ✅ p_raw: 승자의 확률 (배치와 동일)
        p_raw = max(p_up, p_down)
        
        # 오즈비
        odds_ratio = float(np.exp(margin))
        
        # 방향 결정 및 신호값 계산
        sign = +1.0 if p_up > p_down else -1.0
        best_dir = 'UP' if p_up > p_down else 'DOWN'
        
        # 신호값: sign * p_raw
        # UP 우세: p_raw=0.7, sign=+1 → +0.7 (매수 신호)
        # DOWN 우세: p_raw=0.7, sign=-1 → -0.7 (매도 신호)
        # p_raw 범위: 0.5~1.0 → signal_1d 범위: -1.0~+1.0
        signal_1d = sign * p_raw
        
        # Best target 선택 (승자 방향의 top-1)
        best_ranked = up_ranked if best_dir == 'UP' else dn_ranked
        best = best_ranked[0] if best_ranked else {'symbol': None, 'anchor_date': None, 'score': 0.0}
        
        best_info = {
            'symbol': best['symbol'],
            'anchor_date': best['anchor_date'],
            'direction': best_dir,
            'score': best['score']
        }
        
        # 이유 판단
        if p_raw >= threshold:
            reason = "OK"
        elif len(up_ranked) + len(dn_ranked) < 10:
            reason = "TOO_FEW"
        else:
            reason = f"LOW_CONF(p={p_raw:.3f})"
        
        return float(signal_1d), best_info, reason
    
    def _get_best_target_id(
        self,
        config_id: int,
        symbol: str,
        anchor_date: date,
        direction: str
    ) -> int:
        """best_target_id 조회"""
        result = self.db.execute(text("""
            SELECT id
            FROM trading.optuna_target_vectors
            WHERE config_id = :config_id
              AND symbol = :symbol
              AND anchor_date = :anchor_date
              AND direction = :direction
            LIMIT 1
        """), {
            "config_id": config_id,
            "symbol": symbol,
            "anchor_date": anchor_date,
            "direction": direction
        })
        
        row = result.fetchone()
        if not row:
            raise ValueError(f"Target not found: {symbol} {anchor_date} {direction}")
        
        return row[0]
    
    def _process_ticker(
        self,
        ticker_id: int,
        symbol: str,
        company_name: str,
        config_id: int,
        config: dict,
        anchor_date: date,
        q_ctx: np.ndarray,  # ⚠️ Context vector는 미리 생성된 것을 받음
        dry_run: bool,
        run_id: Optional[int]
    ) -> dict:
        """
        단일 티커 처리
        
        Args:
            q_ctx: 미리 생성된 context vector (모든 티커 공유)
        
        Returns:
            {success, ticker_id, symbol, company_name, best_target_id, signal_1d, reason, error}
        """
        try:
            # 1. OHLCV 조회
            lookback = config['lookback']
            ohlcv = self._fetch_ohlcv(ticker_id, anchor_date, lookback)
            
            # 2. Shape 벡터만 생성 (Context는 이미 있음)
            q_shape = build_shape_vector(ohlcv, config)
            
            # 3. ANN 검색
            topN = config['topN']
            up_ranked = self._ann_search(config_id, 'UP', q_shape, anchor_date, topN)
            dn_ranked = self._ann_search(config_id, 'DOWN', q_shape, anchor_date, topN)
            
            if len(up_ranked) == 0 or len(dn_ranked) == 0:
                if not dry_run:
                    self._insert_candidate_decision_history(
                        run_id=run_id,
                        ticker_id=ticker_id,
                        symbol=symbol,
                        action_code="SKIP",
                        passed_gate=False,
                        excluded_reason_code="NO_ANN",
                        explanation_short="Gate blocked: no ANN candidates"
                    )
                    self.db.commit()
                return {
                    'success': False,
                    'symbol': symbol,
                    'error': 'No ANN results'
                }
            
            # 4. 신호 계산
            signal_1d, best_info, reason = self._compute_signal(
                up_ranked, dn_ranked, q_ctx, config
            )
            
            # 5. best_target_id 조회
            best_target_id = self._get_best_target_id(
                config_id, 
                best_info['symbol'], 
                best_info['anchor_date'],
                best_info['direction']
            )
            
            # 6. DB UPSERT + history write
            if not dry_run:
                self.db.execute(text("""
                    INSERT INTO trading.pm_best_signal 
                        (ticker_id, symbol, company_name, best_target_id, signal_1d, updated_at)
                    VALUES (:ticker_id, :symbol, :company_name, :best_target_id, :signal_1d, NOW())
                    ON CONFLICT (ticker_id) 
                    DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        company_name = EXCLUDED.company_name,
                        best_target_id = EXCLUDED.best_target_id,
                        signal_1d = EXCLUDED.signal_1d,
                        updated_at = NOW()
                """), {
                    "ticker_id": ticker_id,
                    "symbol": symbol,
                    "company_name": company_name,
                    "best_target_id": best_target_id,
                    "signal_1d": signal_1d
                })

                self._insert_signal_snapshot_history(
                    run_id=run_id,
                    ticker_id=ticker_id,
                    symbol=symbol,
                    company_name=company_name,
                    signal_1d=signal_1d,
                    best_target_id=best_target_id,
                    best_direction=best_info['direction'],
                    reason_code=reason,
                    reason_text=f"signal={signal_1d:.4f}, reason={reason}",
                    payload={
                        "threshold": float(config['threshold']),
                        "abs_signal": float(abs(signal_1d)),
                        "best_target_symbol": best_info['symbol'],
                        "best_target_anchor_date": str(best_info['anchor_date']) if best_info['anchor_date'] else None,
                        "best_score": float(best_info['score'])
                    }
                )

                action_code, passed_gate, excluded_reason_code, explanation_short = self._build_candidate_decision(
                    reason=reason,
                    signal_1d=signal_1d,
                    threshold=float(config['threshold'])
                )

                self._insert_candidate_decision_history(
                    run_id=run_id,
                    ticker_id=ticker_id,
                    symbol=symbol,
                    action_code=action_code,
                    passed_gate=passed_gate,
                    excluded_reason_code=excluded_reason_code,
                    explanation_short=explanation_short
                )

                self.db.commit()
            
            return {
                'success': True,
                'ticker_id': ticker_id,
                'symbol': symbol,
                'company_name': company_name,
                'best_target_id': best_target_id,
                'signal_1d': signal_1d,
                'reason': reason
            }
            
        except Exception as e:
            self.db.rollback()
            if not dry_run:
                try:
                    self._insert_candidate_decision_history(
                        run_id=run_id,
                        ticker_id=ticker_id,
                        symbol=symbol,
                        action_code="SKIP",
                        passed_gate=False,
                        excluded_reason_code="ERROR",
                        explanation_short=f"Gate blocked by error: {str(e)[:120]}"
                    )
                    self.db.commit()
                except Exception:
                    self.db.rollback()
            return {
                'success': False,
                'symbol': symbol,
                'error': str(e)
            }
    
    def test_signal(self, request: TestPMSignalRequest) -> TestPMSignalResponse:
        """
        테스트용 신호 계산 (DB 저장 없음, 상세 결과 리턴)
        
        Args:
            request: 테스트 요청
            
        Returns:
            상세 매칭 결과
        """
        # 1. Config 조회
        config_id = self._get_latest_config_id()
        config = self._load_config(config_id)
        
        # topN 오버라이드
        if request.topN:
            config['topN'] = request.topN
        
        logger.info(f"Test signal: ticker_id={request.ticker_id}, config_id={config_id}")
        
        # 2. 기준 날짜
        if request.anchor_date:
            anchor_date = datetime.strptime(request.anchor_date, '%Y-%m-%d').date()
        else:
            anchor_date = date.today()
        
        try:
            # 3. 티커 정보 조회
            result = self.db.execute(text("""
                SELECT t.id, t.symbol, t.country,
                       COALESCE(
                           (SELECT name FROM trading.ticker_i18n 
                            WHERE ticker_id = t.id AND lang_code = 'ko' LIMIT 1),
                           (SELECT name FROM trading.ticker_i18n 
                            WHERE ticker_id = t.id AND lang_code = 'en' LIMIT 1),
                           ''
                       ) AS company_name
                FROM trading.ticker t
                WHERE t.id = :ticker_id
            """), {"ticker_id": request.ticker_id})
            
            row = result.fetchone()
            if not row:
                return TestPMSignalResponse(
                    success=False,
                    ticker_id=request.ticker_id,
                    symbol="UNKNOWN",
                    company_name=None,
                    country=None,
                    config_id=config_id,
                    anchor_date=str(anchor_date),
                    signal_1d=0.0,
                    p_up=0.0,
                    p_down=0.0,
                    best_direction="NONE",
                    best_target={},
                    up_matches=[],
                    down_matches=[],
                    up_reranked_top10=[],
                    down_reranked_top10=[],
                    stats={},
                    error="Ticker not found"
                )
            
            ticker_id, symbol, country, company_name = row[0], row[1], row[2], row[3]
            
            # 4. OHLCV 조회
            lookback = config['lookback']
            ohlcv = self._fetch_ohlcv(ticker_id, anchor_date, lookback)
            
            # 5. 벡터 생성
            q_shape = build_shape_vector(ohlcv, config)
            q_ctx = build_context_vector(self.db, anchor_date, config)
            
            # 6. 검색 (ANN 또는 Full Scan)
            topN = config['topN']
            
            if request.use_ann:
                logger.info(f"🚀 Using ANN search (pgvector index)")
                up_ranked = self._ann_search(config_id, 'UP', q_shape, anchor_date, topN)
                dn_ranked = self._ann_search(config_id, 'DOWN', q_shape, anchor_date, topN)
                search_mode = "ANN (pgvector index)"
            else:
                logger.info(f"🐌 Using Full Scan (exact cosine distance)")
                up_ranked = self._full_scan_search(config_id, 'UP', q_shape, anchor_date, topN)
                dn_ranked = self._full_scan_search(config_id, 'DOWN', q_shape, anchor_date, topN)
                search_mode = "Full Scan (exact)"
            
            # 7. 국가 정보 조회 (배치로 한 번에)
            all_symbols = [r['symbol'] for r in up_ranked + dn_ranked]
            country_map = self._get_country_map(all_symbols)
            
            # 8. 재랭킹 및 신호 계산
            alpha = config['alpha']
            beta = config['beta']
            tau = config['tau_softmax']
            
            # 가중치 정규화
            sum_w = alpha + beta
            if sum_w <= 0.0:
                sum_w = 1.0
            alpha_norm = alpha / sum_w
            beta_norm = beta / sum_w
            
            # 재랭킹 함수 (동일)
            def rerank_with_details(candidates):
                for c in candidates:
                    ctx_vec = c['ctx_vec'] / (np.linalg.norm(c['ctx_vec']) + 1e-12)
                    cos_ctx = float(np.dot(q_ctx, ctx_vec))
                    cos_shape = c['cos']
                    c['score'] = alpha_norm * cos_shape + beta_norm * cos_ctx
                    c['cos_ctx'] = cos_ctx
                candidates.sort(key=lambda c: c['score'], reverse=True)
                return candidates
            
            up_ranked = rerank_with_details(up_ranked)
            dn_ranked = rerank_with_details(dn_ranked)
            
            # 9. 신호 계산 (동일 로직)
            sU = np.array([r['score'] for r in up_ranked], dtype=np.float32)
            sD = np.array([r['score'] for r in dn_ranked], dtype=np.float32)
            
            logU = self._logsumexp_tau(sU, tau)
            logD = self._logsumexp_tau(sD, tau)
            
            margin = logU - logD
            margin = np.clip(margin, -50, 50)
            
            p_up = float(1.0 / (1.0 + np.exp(-margin)))
            p_down = 1.0 - p_up
            
            best_dir = 'UP' if p_up > p_down else 'DOWN'
            signal_1d = float(p_up - p_down)  # -1 ~ +1 범위
            
            best_ranked = up_ranked if best_dir == 'UP' else dn_ranked
            best = best_ranked[0] if best_ranked else {'symbol': None, 'anchor_date': None, 'score': 0.0}
            
            # 10. 응답 생성 (국가 정보 포함)
            def to_match_item(r):
                return ANNMatchItem(
                    symbol=r['symbol'],
                    country=country_map.get(r['symbol']),
                    anchor_date=str(r['anchor_date']),
                    cos_shape=r['cos'],
                    cos_ctx=r.get('cos_ctx'),
                    score=r.get('score'),
                    tb_label=r.get('tb_label'),
                    iae_1_3=r.get('iae_1_3')
                )
            
            # 국가별 통계
            up_countries = [country_map.get(r['symbol'], 'UNKNOWN') for r in up_ranked]
            dn_countries = [country_map.get(r['symbol'], 'UNKNOWN') for r in dn_ranked]
            
            from collections import Counter
            up_country_stats = dict(Counter(up_countries))
            dn_country_stats = dict(Counter(dn_countries))
            
            return TestPMSignalResponse(
                success=True,
                ticker_id=ticker_id,
                symbol=symbol,
                company_name=company_name,
                country=country,
                config_id=config_id,
                anchor_date=str(anchor_date),
                signal_1d=signal_1d,
                p_up=p_up,
                p_down=p_down,
                best_direction=best_dir,
                best_target={
                    'symbol': best['symbol'],
                    'anchor_date': str(best['anchor_date']),
                    'score': best['score'],
                    'country': country_map.get(best['symbol'])
                },
                up_matches=[to_match_item(r) for r in up_ranked],
                down_matches=[to_match_item(r) for r in dn_ranked],
                up_reranked_top10=[to_match_item(r) for r in up_ranked[:10]],
                down_reranked_top10=[to_match_item(r) for r in dn_ranked[:10]],
                stats={
                    'search_mode': search_mode,
                    'up_count': len(up_ranked),
                    'down_count': len(dn_ranked),
                    'up_country_stats': up_country_stats,
                    'down_country_stats': dn_country_stats,
                    'logU': float(logU),
                    'logD': float(logD),
                    'margin': float(margin),
                    'alpha': alpha,
                    'beta': beta,
                    'tau': tau
                }
            )
            
        except Exception as e:
            logger.error(f"Test signal error: {e}", exc_info=True)
            return TestPMSignalResponse(
                success=False,
                ticker_id=request.ticker_id,
                symbol="ERROR",
                company_name=None,
                country=None,
                config_id=config_id,
                anchor_date=str(anchor_date),
                signal_1d=0.0,
                p_up=0.0,
                p_down=0.0,
                best_direction="NONE",
                best_target={},
                up_matches=[],
                down_matches=[],
                up_reranked_top10=[],
                down_reranked_top10=[],
                stats={},
                error=str(e)
            )
    
    def _insert_signal_run_header(
        self,
        anchor_date: date,
        config_id: int,
        country: Optional[str],
        executed_at: datetime
    ) -> int:
        """신호 실행 run_header 생성"""
        country_upper = (country or "ADHOC").upper()
        if country_upper == "KR":
            session_type = "KR_OPEN"
        elif country_upper == "US":
            session_type = "US_OPEN"
        else:
            session_type = "ADHOC"

        timestamp_key = executed_at.strftime('%Y%m%d%H%M%S')
        run_key = f"pm:{anchor_date.isoformat()}:{config_id}:{country_upper}:{timestamp_key}"

        result = self.db.execute(text("""
            INSERT INTO trading.pm_signal_run_header
                (run_key, session_type, anchor_date, config_id, country, executed_at)
            VALUES
                (:run_key, :session_type, :anchor_date, :config_id, :country, :executed_at)
            RETURNING run_id
        """), {
            "run_key": run_key,
            "session_type": session_type,
            "anchor_date": anchor_date,
            "config_id": config_id,
            "country": country,
            "executed_at": executed_at
        })
        row = result.fetchone()
        if not row:
            raise RuntimeError("Failed to create pm_signal_run_header")

        self.db.commit()
        return int(row[0])

    def _insert_signal_snapshot_history(
        self,
        run_id: Optional[int],
        ticker_id: int,
        symbol: str,
        company_name: str,
        signal_1d: float,
        best_target_id: Optional[int],
        best_direction: str,
        reason_code: Optional[str],
        reason_text: Optional[str],
        payload: Optional[Dict[str, Any]]
    ) -> None:
        """티커별 신호 스냅샷 이력 적재"""
        if run_id is None:
            return

        self.db.execute(text("""
            INSERT INTO trading.pm_signal_snapshot_history
                (run_id, ticker_id, symbol, company_name, signal_1d,
                 best_target_id, best_direction, reason_code, reason_text, payload)
            VALUES
                (:run_id, :ticker_id, :symbol, :company_name, :signal_1d,
                 :best_target_id, :best_direction, :reason_code, :reason_text, CAST(:payload AS JSONB))
        """), {
            "run_id": run_id,
            "ticker_id": ticker_id,
            "symbol": symbol,
            "company_name": company_name,
            "signal_1d": signal_1d,
            "best_target_id": best_target_id,
            "best_direction": best_direction,
            "reason_code": reason_code,
            "reason_text": reason_text,
            "payload": json.dumps(payload) if payload is not None else None
        })

    def _build_candidate_decision(
        self,
        reason: str,
        signal_1d: float,
        threshold: float
    ) -> Tuple[str, bool, Optional[str], str]:
        """candidate decision history 최소 필드 산출"""
        if reason == "OK" and abs(signal_1d) >= threshold:
            return "BUY", True, None, f"Gate passed: |signal|={abs(signal_1d):.4f} >= {threshold:.4f}"

        if reason == "No ANN results":
            excluded_reason_code = "NO_ANN"
        elif reason == "ERROR":
            excluded_reason_code = "ERROR"
        else:
            excluded_reason_code = "LOW_SIGNAL"
        return "SKIP", False, excluded_reason_code, f"Gate blocked: reason={reason}, signal={signal_1d:.4f}"

    def _insert_candidate_decision_history(
        self,
        run_id: Optional[int],
        ticker_id: int,
        symbol: str,
        action_code: str,
        passed_gate: bool,
        excluded_reason_code: Optional[str],
        explanation_short: str
    ) -> None:
        """티커별 candidate decision history 적재"""
        if run_id is None:
            return

        self.db.execute(text("""
            INSERT INTO trading.pm_candidate_decision_history
                (run_id, ticker_id, symbol, passed_gate, excluded_reason_code, action_code, explanation_short)
            VALUES
                (:run_id, :ticker_id, :symbol, :passed_gate, :excluded_reason_code, :action_code, :explanation_short)
        """), {
            "run_id": run_id,
            "ticker_id": ticker_id,
            "symbol": symbol,
            "passed_gate": passed_gate,
            "excluded_reason_code": excluded_reason_code,
            "action_code": action_code,
            "explanation_short": explanation_short
        })

    def _get_country_map(self, symbols: List[str]) -> Dict[str, str]:
        """심볼 리스트에 대한 국가 정보 조회"""
        if not symbols:
            return {}
        
        placeholders = ','.join([f':symbol_{i}' for i in range(len(symbols))])
        params = {f'symbol_{i}': symbol for i, symbol in enumerate(symbols)}
        
        result = self.db.execute(text(f"""
            SELECT symbol, country
            FROM trading.ticker
            WHERE symbol IN ({placeholders})
        """), params)
        
        return {row[0]: row[1] for row in result.fetchall()}

