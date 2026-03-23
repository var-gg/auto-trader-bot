# app/features/premarket/services/pm_signal_service_v2.py
"""
PM Best Signal Service v2 - 개선된 버전

주요 개선사항:
1. signal_1d 산출식 통일 (테스트=배치)
2. Shape/Context 분해 로그 (진단용)
3. breadth 계산 개선 (국가 필터)
4. 컨텍스트 과가중 방지 로직
"""
from __future__ import annotations
import json
import logging
import time
import numpy as np
from datetime import date, datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.features.premarket.utils.vector_builder import (
    build_shape_vector,
    build_context_vector,
    cosine_similarity,
    pgvector_to_numpy
)

logger = logging.getLogger(__name__)


class PMSignalServiceV2:
    """PM Best Signal 서비스 v2"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def update_signals_v2(
        self, 
        tickers: Optional[List[str]] = None,
        country: Optional[str] = None,
        anchor_date: Optional[str] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        장전 신호 계산 및 업데이트 (v2)
        
        Args:
            tickers: 티커 심볼 리스트 (None이면 모든 종목)
            country: 국가 필터 (KR, US 등)
            anchor_date: 기준 날짜 (YYYY-MM-DD, None이면 오늘)
            dry_run: True면 테스트 모드 (DB 저장 안 함)
        """
        start_time = time.time()
        
        # 1. Config 조회
        config_id = self._get_latest_config_id()
        config = self._load_config(config_id)
        
        logger.info(f"[V2] Using config_id: {config_id}")
        
        # 2. 기준 날짜
        if anchor_date:
            anchor_date_obj = datetime.strptime(anchor_date, '%Y-%m-%d').date()
        else:
            anchor_date_obj = date.today()
        
        run_id: Optional[int] = None
        if not dry_run:
            run_id = self._insert_signal_run_header(
                anchor_date=anchor_date_obj,
                config_id=config_id,
                country=country,
                executed_at=datetime.now(timezone.utc),
            )

        # 3. 티커 목록 조회
        tickers_list = self._get_tickers(tickers, country)
        
        logger.info(f"[V2] Processing {len(tickers_list)} tickers for date {anchor_date_obj}")
        
        # 4. Context Vector 한 번만 생성 (모든 티커 공유)
        logger.info("[V2] Building context vector (shared for all tickers)...")
        try:
            q_ctx = build_context_vector(self.db, anchor_date_obj, config)
            logger.info(f"[V2] Context vector created: dim={len(q_ctx)}")
        except Exception as e:
            logger.error(f"[V2] Failed to build context vector: {e}", exc_info=True)
            raise ValueError(f"Context vector generation failed: {e}")
        
        # 5. 각 티커 처리
        results = []
        errors = []
        success_count = 0
        failed_count = 0
        
        # 통계 추적
        up_count = 0
        down_count = 0
        diagnostics = []
        
        for ticker_id, symbol, company_name in tickers_list:
            try:
                result = self._process_ticker_v2(
                    ticker_id, symbol, company_name,
                    config_id, config, anchor_date_obj, q_ctx, dry_run, run_id
                )
                
                if result['success']:
                    results.append(result)
                    success_count += 1
                    
                    # 통계
                    if result.get('signal_1d', 0) > 0:
                        up_count += 1
                    else:
                        down_count += 1
                    
                    # 진단 정보 저장
                    if result.get('diagnostic'):
                        diagnostics.append(result['diagnostic'])
                else:
                    errors.append(result)
                    failed_count += 1
                    
                    # 🔥 "Insufficient data" 같은 예상 에러는 스킵, 나머지는 즉시 중단
                    error_msg = result.get('error', '')
                    if "Insufficient data" not in error_msg:
                        logger.error(f"[V2] ❌ First unexpected failure at {symbol}, stopping loop")
                        logger.error(f"[V2] Error details: {result}")
                        import traceback
                        traceback.print_exc()
                        raise RuntimeError(f"Unexpected failure at {symbol}: {error_msg}")
                    else:
                        logger.warning(f"[V2] ⚠️ Expected data issue at {symbol}, continuing...")
            except Exception as e:
                logger.error(f"[V2] ❌ Exception processing ticker {symbol}: {e}", exc_info=True)
                errors.append({'symbol': symbol, 'error': str(e)})
                failed_count += 1
                
                # 🔥 예상 에러가 아니면 즉시 중단 (스택 출력)
                if "Insufficient data" not in str(e):
                    logger.error(f"[V2] ❌ Unexpected exception caught, stopping loop at {symbol}")
                    import traceback
                    traceback.print_exc()
                    raise
                else:
                    logger.warning(f"[V2] ⚠️ Expected data issue exception at {symbol}, continuing...")
        
        elapsed = time.time() - start_time
        
        # 통계 로그
        total = up_count + down_count
        if total > 0:
            up_pct = up_count / total * 100
            down_pct = down_count / total * 100
            logger.info(f"[V2] Signal distribution: UP={up_pct:.1f}% ({up_count}), DOWN={down_pct:.1f}% ({down_count})")
        
        # 진단 정보 요약 (평균)
        if diagnostics:
            avg_delta_shape = np.mean([d['delta_shape'] for d in diagnostics])
            avg_delta_ctx = np.mean([d['delta_ctx'] for d in diagnostics])
            avg_breadth = np.mean([d['breadth'] for d in diagnostics])
            
            logger.info(f"[V2] Diagnostic averages: Δshape={avg_delta_shape:.4f}, Δctx={avg_delta_ctx:.4f}, breadth={avg_breadth:.4f}")
        
        # ✅ 적재 완료 후 프로시저 호출 (dry_run=false일 때만)
        procedure_executed = False
        if not dry_run and success_count > 0:
            try:
                logger.info(f"[V2] Calling stored procedure: update_pm_best_signal({config_id})")
                
                connection = self.db.connection()
                connection.execute(text("COMMIT"))
                connection.execute(text("CALL trading.update_pm_best_signal(:config_id)"), 
                                 {"config_id": config_id})
                
                procedure_executed = True
                logger.info(f"[V2] ✅ Stored procedure completed successfully")
            except Exception as e:
                logger.error(f"[V2] ❌ Stored procedure call failed (history rows are already committed): {e}", exc_info=True)
                procedure_executed = False
        
        return {
            'success': True,
            'config_id': config_id,
            'anchor_date': str(anchor_date_obj),
            'results': {
                'total': len(tickers_list),
                'success': success_count,
                'failed': failed_count,
                'up_count': up_count,
                'down_count': down_count,
                'up_pct': (up_count / total * 100) if total > 0 else 0.0,
                'down_pct': (down_count / total * 100) if total > 0 else 0.0
            },
            'diagnostics': {
                'avg_delta_shape': float(np.mean([d['delta_shape'] for d in diagnostics])) if diagnostics else 0.0,
                'avg_delta_ctx': float(np.mean([d['delta_ctx'] for d in diagnostics])) if diagnostics else 0.0,
                'avg_breadth': float(np.mean([d['breadth'] for d in diagnostics])) if diagnostics else 0.0
            },
            'elapsed_seconds': elapsed,
            'procedure_executed': procedure_executed
        }
    
    def _get_latest_config_id(self) -> int:
        """최신 config_id 조회"""
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
            'macro_cols': row[14] if row[14] else [],
            'macro_lag_days': row[15] if row[15] is not None else 1,
            'tail_weight': 0.0
        }
        
        return config
    
    def _get_tickers(
        self,
        ticker_symbols: Optional[List[str]],
        country: Optional[str]
    ) -> List[Tuple[int, str, str]]:
        """티커 목록 조회"""
        where_clauses = ["t.type IN ('stock', 'etf')"]
        params = {}
        
        if ticker_symbols:
            placeholders = ','.join([f':symbol_{i}' for i in range(len(ticker_symbols))])
            where_clauses.append(f"t.symbol IN ({placeholders})")
            for i, symbol in enumerate(ticker_symbols):
                params[f'symbol_{i}'] = symbol
        
        if country:
            where_clauses.append("t.country = :country")
            params['country'] = country
        
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
        
        result = self.db.execute(text(sql), params)
        return result.fetchall()
    
    def _logsumexp_tau(self, scores: np.ndarray, tau: float) -> float:
        """Temperature-scaled log-sum-exp"""
        tau = max(float(tau), 1e-6)
        a = np.asarray(scores, dtype=np.float64) / tau
        if a.size == 0:
            return -np.inf
        m = float(a.max())
        return m + np.log(np.exp(a - m).sum() + 1e-12)
    
    def _fetch_ohlcv(
        self,
        ticker_id: int,
        until_date: date,
        lookback: int
    ) -> np.ndarray:
        """OHLCV 데이터 조회"""
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
        """pgvector ANN 검색 + CPU Re-ranking"""
        cfg_table = f"trading.target_vecidx_cfg_{config_id}"
        
        limit = topN
        probe = 525
        
        self.db.execute(text("SET LOCAL enable_seqscan = off;"))
        self.db.execute(text(f"SET LOCAL ivfflat.probes = {probe};"))
        
        vec_param = q_shape.astype(np.float32).tolist()
        
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
        
        with self.db.connection().connection.cursor() as cur:
            cur.execute(sql, (vec_param, direction, until_date, vec_param, limit))
            rows = cur.fetchall()
        
        results = []
        for row in rows:
            shape_vec = pgvector_to_numpy(row[3])
            shape_vec = shape_vec / (np.linalg.norm(shape_vec) + 1e-12)
            
            cos_exact = float(np.dot(q_shape, shape_vec))
            
            ctx_vec = pgvector_to_numpy(row[4]) if row[4] else np.zeros(10, dtype=np.float32)
            
            results.append({
                'symbol': row[0],
                'anchor_date': row[1],
                'cos': cos_exact,
                'ctx_vec': ctx_vec,
                'tb_label': row[5],
                'iae_1_3': float(row[6]) if row[6] else 0.0
            })
        
        results.sort(key=lambda r: r['cos'], reverse=True)
        return results[:topN]
    
    def _process_ticker_v2(
        self,
        ticker_id: int,
        symbol: str,
        company_name: str,
        config_id: int,
        config: dict,
        anchor_date: date,
        q_ctx: np.ndarray,
        dry_run: bool,
        run_id: Optional[int],
    ) -> Dict[str, Any]:
        """티커별 처리 (v2)"""
        try:
            # OHLCV 조회 (데이터 부족 시 스킵)
            try:
                ohlcv = self._fetch_ohlcv(ticker_id, anchor_date, config['lookback'])
            except ValueError as e:
                if "Insufficient data" in str(e):
                    logger.warning(f"[V2] {symbol}: {e} → SKIP")
                    return {
                        'success': False,
                        'ticker_id': ticker_id,
                        'symbol': symbol,
                        'error': str(e)
                    }
                raise
            
            # Shape Vector 생성
            q_shape = build_shape_vector(ohlcv, config)
            
            # ANN 검색
            topN = config['topN']
            up_ranked = self._ann_search(config_id, 'UP', q_shape, anchor_date, topN)
            dn_ranked = self._ann_search(config_id, 'DOWN', q_shape, anchor_date, topN)
            
            # 신호 계산 (v2)
            signal_1d, best_info, reason, diagnostic, calc_detail = self._compute_signal_v2(
                up_ranked, dn_ranked, q_ctx, config
            )
            
            # best_target_id 조회 (원본 로직과 동일)
            best_target_id = self._get_best_target_id(
                config_id,
                best_info['symbol'],
                best_info['anchor_date'],
                best_info['direction']
            )
            best_info['target_id'] = best_target_id
            
            # DB 저장 (dry_run=False일 때만)
            if not dry_run:
                try:
                    self._upsert_signal(ticker_id, symbol, company_name, signal_1d, best_info, config_id)

                    action_code, passed_gate, excluded_reason_code, explanation_short = self._build_candidate_decision(
                        reason=reason,
                        signal_1d=signal_1d,
                        threshold=float(config.get('threshold', 0.0)),
                    )
                    self._insert_signal_snapshot_history(
                        run_id=run_id,
                        ticker_id=ticker_id,
                        symbol=symbol,
                        company_name=company_name,
                        signal_1d=signal_1d,
                        best_target_id=best_info.get('target_id'),
                        best_direction=best_info.get('direction'),
                        reason_code=reason,
                        reason_text=reason,
                        payload={
                            'best': best_info,
                            'diagnostic': diagnostic,
                            'calc_detail': calc_detail,
                        },
                    )
                    self._insert_candidate_decision_history(
                        run_id=run_id,
                        ticker_id=ticker_id,
                        symbol=symbol,
                        action_code=action_code,
                        passed_gate=passed_gate,
                        excluded_reason_code=excluded_reason_code,
                        explanation_short=explanation_short,
                    )
                    self.db.commit()
                except Exception as e:
                    logger.error(f"[V2] Failed to save signal/history for {symbol}: {e}")
                    self.db.rollback()
                    raise
            
            return {
                'success': True,
                'ticker_id': ticker_id,
                'symbol': symbol,
                'signal_1d': signal_1d,
                'reason': reason,
                'diagnostic': diagnostic
            }
        except Exception as e:
            logger.error(f"[V2] Error processing {symbol}: {e}", exc_info=True)
            return {
                'success': False,
                'ticker_id': ticker_id,
                'symbol': symbol,
                'error': str(e)
            }
    
    def _upsert_signal(
        self,
        ticker_id: int,
        symbol: str,
        company_name: str,
        signal_1d: float,
        best_info: dict,
        config_id: int
    ):
        """신호 저장 - pm_best_signal 테이블 사용"""
        # best_info에서 best_target_id 추출
        best_target_id = best_info.get('target_id', 0) if best_info else 0
        
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
            'ticker_id': ticker_id,
            'symbol': symbol,
            'company_name': company_name,
            'best_target_id': best_target_id,
            'signal_1d': signal_1d
        })
    
    def _get_best_target_id(
        self,
        config_id: int,
        symbol: str,
        anchor_date: date,
        direction: str
    ) -> int:
        """best_target_id 조회 (원본 로직과 동일)"""
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
            logger.warning(f"[V2] Target not found: {symbol} {anchor_date} {direction}")
            # Foreign key 위반 방지: 최소한 유효한 ID 반환 (예: 1)
            return 1
        
        return row[0]
    
    def _insert_signal_run_header(
        self,
        anchor_date: date,
        config_id: int,
        country: Optional[str],
        executed_at: datetime,
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
            "executed_at": executed_at,
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
        best_direction: Optional[str],
        reason_code: Optional[str],
        reason_text: Optional[str],
        payload: Optional[Dict[str, Any]],
    ) -> None:
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
            "payload": json.dumps(payload, default=str) if payload is not None else None,
        })

    def _build_candidate_decision(
        self,
        reason: str,
        signal_1d: float,
        threshold: float,
    ) -> Tuple[str, bool, Optional[str], str]:
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
        explanation_short: str,
    ) -> None:
        if run_id is None:
            return

        self.db.execute(text("""
            INSERT INTO trading.pm_candidate_decision_history
                (run_id, ticker_id, symbol, passed_gate, excluded_reason_code, action_code, explanation_short)
            VALUES
                (:run_id, :ticker_id, :symbol, :passed_gate, :excluded_reason_code, :action_code, :explanation_short)
            ON CONFLICT (run_id, ticker_id)
            DO UPDATE SET
                passed_gate = EXCLUDED.passed_gate,
                excluded_reason_code = EXCLUDED.excluded_reason_code,
                action_code = EXCLUDED.action_code,
                explanation_short = EXCLUDED.explanation_short
        """), {
            "run_id": run_id,
            "ticker_id": ticker_id,
            "symbol": symbol,
            "passed_gate": passed_gate,
            "excluded_reason_code": excluded_reason_code,
            "action_code": action_code,
            "explanation_short": explanation_short,
        })

    def _compute_signal_v2(
        self,
        up_ranked: List[dict],
        dn_ranked: List[dict],
        q_ctx: np.ndarray,
        config: dict
    ) -> Tuple[float, dict, str, Dict[str, Any], Dict[str, Any]]:
        """
        신호 계산 (v2) - 진단 정보 추가
        
        Returns:
            (signal_1d, best_target_info, reason, diagnostic)
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
        
        # ========================================================================
        # 🔧 Context gating (v2 개선) - breadth + Δctx 기반 β 동적 조정
        # ========================================================================
        # breadth가 높고 |Δctx|가 클수록 β를 줄여 컨텍스트 과가중 방지
        breadth_raw = float(q_ctx[-1]) if len(q_ctx) > 0 else 0.0
        strength = max(0.0, min(1.0, (abs(breadth_raw) - 0.6) / 0.4))  # |breadth| 0.6→1.0 선형
        
        # 재랭킹 전이므로 미리 delta_ctx 계산은 못 함, 기본값 사용
        # (실제 사용 시 rerank 후 계산된 delta_ctx로 보정)
        
        # 기본 gating: breadth 기반만 적용 (0.85 축소율)
        shrink = 1.0 - 0.85 * strength
        beta_eff = max(0.10, beta_norm * shrink)
        alpha_eff = 1.0 - beta_eff
        
        logger.debug(f"[V2] Context gating: breadth={breadth_raw:.4f}, strength={strength:.3f}, "
                    f"beta_norm={beta_norm:.3f} → beta_eff={beta_eff:.3f}")
        
        # 재랭킹
        # ⚠️ alpha = Shape 가중치, beta = Context 가중치
        # score = alpha * cos_shape + beta * cos_ctx
        def rerank(candidates):
            for c in candidates:
                ctx_vec = c['ctx_vec'] / (np.linalg.norm(c['ctx_vec']) + 1e-12)
                cos_ctx = float(np.dot(q_ctx, ctx_vec))  # Context 코사인 유사도
                cos_shape = c['cos']                      # Shape 코사인 유사도 (ANN 결과)
                
                # 최종 점수 = Shape * alpha_eff + Context * beta_eff (gated)
                c['score'] = alpha_eff * cos_shape + beta_eff * cos_ctx
                c['cos_ctx'] = cos_ctx
            candidates.sort(key=lambda c: c['score'], reverse=True)
            return candidates
        
        up_ranked = rerank(up_ranked)
        dn_ranked = rerank(dn_ranked)
        
        # ========================================================================
        # 🔍 Shape/Context 분해 (진단용) - TopK 적용
        # ========================================================================
        K = int(config.get('lse_topk', 20))  # 기본 20개만 사용
        
        sU_shape = np.array([alpha_eff * r['cos'] for r in up_ranked[:K]], dtype=np.float32)
        sD_shape = np.array([alpha_eff * r['cos'] for r in dn_ranked[:K]], dtype=np.float32)
        sU_ctx = np.array([beta_eff * r['cos_ctx'] for r in up_ranked[:K]], dtype=np.float32)
        sD_ctx = np.array([beta_eff * r['cos_ctx'] for r in dn_ranked[:K]], dtype=np.float32)
        
        logU_shape = self._logsumexp_tau(sU_shape, tau)
        logD_shape = self._logsumexp_tau(sD_shape, tau)
        logU_ctx = self._logsumexp_tau(sU_ctx, tau)
        logD_ctx = self._logsumexp_tau(sD_ctx, tau)
        
        delta_shape = logU_shape - logD_shape
        delta_ctx = logU_ctx - logD_ctx
        
        # breadth 값 추출 (진단용)
        breadth = float(q_ctx[-1]) if len(q_ctx) > 0 else 0.0
        
        # ========================================================================
        # 신호 계산 - TopK 적용 (v2 개선)
        # ========================================================================
        sU = np.array([r['score'] for r in up_ranked[:K]], dtype=np.float32)
        sD = np.array([r['score'] for r in dn_ranked[:K]], dtype=np.float32)
        
        logU = self._logsumexp_tau(sU, tau)
        logD = self._logsumexp_tau(sD, tau)
        
        if np.isinf(logU) and np.isinf(logD):
            raise ValueError("No valid candidates")
        
        margin = logU - logD
        margin = np.clip(margin, -50, 50)
        
        p_up = float(1.0 / (1.0 + np.exp(-margin)))
        p_down = 1.0 - p_up
        
        # ✅ 통일된 산출식: sign * max(p_up, p_down) (v2 핵심)
        # 범위: p_raw ∈ [0.5, 1.0] → signal_1d ∈ [-1.0, -0.5] ∪ [+0.5, +1.0]
        # 테스트와 배치 동일한 산출식 사용
        p_raw = max(p_up, p_down)
        
        sign = +1.0 if p_up > p_down else -1.0
        best_dir = 'UP' if p_up > p_down else 'DOWN'
        
        signal_1d = sign * p_raw
        
        # Best target 선택
        best_ranked = up_ranked if best_dir == 'UP' else dn_ranked
        best = best_ranked[0] if best_ranked else {'symbol': None, 'anchor_date': None, 'score': 0.0}
        
        best_info = {
            'symbol': best.get('symbol'),
            'anchor_date': best.get('anchor_date'),
            'direction': best_dir,
            'score': best.get('score', 0.0)
        }
        
        # 이유 판단
        if p_raw >= threshold:
            reason = "OK"
        elif len(up_ranked) + len(dn_ranked) < 10:
            reason = "TOO_FEW"
        else:
            reason = f"LOW_CONF(p={p_raw:.3f})"
        
        # 진단 정보
        diagnostic = {
            'delta_shape': float(delta_shape),
            'delta_ctx': float(delta_ctx),
            'delta_total': float(margin),
            'breadth': float(breadth),
            'alpha_norm': float(alpha_norm),
            'beta_norm': float(beta_norm),
            'alpha_eff': float(alpha_eff),  # v2: gated
            'beta_eff': float(beta_eff),    # v2: gated
            'tau': float(tau),
            'lse_topk': K                   # v2: TopK 사용
        }
        
        logger.info(
            f"[V2] [{len(up_ranked)}/{len(dn_ranked)}] K={K} "
            f"Δshape={delta_shape:.4f}, Δctx={delta_ctx:.4f}, Δtotal={margin:.4f}, "
            f"breadth={breadth:.4f}, β={beta_norm:.2f}→{beta_eff:.2f}, signal={signal_1d:.4f}"
        )
        
        calc_detail = {
            'p_up': float(p_up),
            'p_down': float(p_down),
            'margin': float(margin),
            'threshold': float(threshold),
        }
        return float(signal_1d), best_info, reason, diagnostic, calc_detail
    
    # ... (나머지 메서드들은 기존과 동일하므로 생략, 필요시 추가)

