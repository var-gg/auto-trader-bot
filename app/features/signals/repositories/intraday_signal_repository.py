# app/features/signals/repositories/intraday_signal_repository.py
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func, text
from decimal import Decimal
from datetime import datetime
import logging

from app.features.signals.models.intraday_signal_detection_config import IntradaySignalDetectionConfig
from app.features.signals.models.intraday_signal_detection_result import IntradaySignalDetectionResult

logger = logging.getLogger(__name__)


class IntradaySignalRepository:
    """
    분봉 시그널 탐지 결과 리포지토리
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_or_create_config(
        self,
        direction: str,
        lookback: int,
        future_window: int,
        min_change: float,
        max_reverse: float,
        flatness_k: float,
        atr_window: int = 7,
        version: str = "v1"
    ) -> IntradaySignalDetectionConfig:
        """
        설정 조회 또는 생성
        
        Args:
            direction: 시그널 방향
            lookback: 직전 구간 확인 기간
            future_window: 이후 구간 평가 기간
            min_change: 최소 변동률
            max_reverse: 반대 방향 최대 허용폭
            flatness_k: 평탄성 허용치
            atr_window: ATR 계산 윈도우
            version: 알고리즘 버전
        
        Returns:
            설정 객체
        """
        # 조회
        config = self.db.query(IntradaySignalDetectionConfig).filter(
            IntradaySignalDetectionConfig.direction == direction,
            IntradaySignalDetectionConfig.lookback == lookback,
            IntradaySignalDetectionConfig.future_window == future_window,
            IntradaySignalDetectionConfig.min_change == Decimal(str(min_change)),
            IntradaySignalDetectionConfig.max_reverse == Decimal(str(max_reverse)),
            IntradaySignalDetectionConfig.flatness_k == Decimal(str(flatness_k)),
            IntradaySignalDetectionConfig.atr_window == atr_window,
            IntradaySignalDetectionConfig.version == version
        ).first()
        
        if config:
            logger.info(f"Found existing intraday config: config_id={config.config_id}")
            return config
        
        # 생성
        config = IntradaySignalDetectionConfig(
            direction=direction,
            lookback=lookback,
            future_window=future_window,
            min_change=Decimal(str(min_change)),
            max_reverse=Decimal(str(max_reverse)),
            flatness_k=Decimal(str(flatness_k)),
            atr_window=atr_window,
            version=version
        )
        self.db.add(config)
        self.db.flush()  # config_id 생성
        logger.info(f"Created new intraday config: config_id={config.config_id}")
        
        return config
    
    def check_recent_signals_exist(
        self,
        ticker_id: int,
        config_ids: List[int],
        cutoff_datetime: datetime
    ) -> bool:
        """
        특정 티커의 특정 시간 이후 시그널이 존재하는지 확인
        
        Args:
            ticker_id: 티커 ID
            config_ids: 확인할 config_id 리스트
            cutoff_datetime: 기준 날짜/시간 (이 시간 >= 의 데이터가 있는지 확인)
            
        Returns:
            True: 기준 시간 이후의 데이터가 존재
            False: 기준 시간 이후의 데이터가 없음
        """
        count = self.db.query(IntradaySignalDetectionResult).filter(
            IntradaySignalDetectionResult.ticker_id == ticker_id,
            IntradaySignalDetectionResult.config_id.in_(config_ids),
            IntradaySignalDetectionResult.detected_at >= cutoff_datetime
        ).count()
        
        exists = count > 0
        logger.info(f"Ticker {ticker_id} has {count} signals >= {cutoff_datetime}: {exists}")
        return exists
    
    def delete_ticker_signals(
        self,
        ticker_id: int,
        config_ids: Optional[List[int]] = None
    ) -> int:
        """
        특정 티커의 인트라데이 시그널 삭제
        
        Args:
            ticker_id: 티커 ID
            config_ids: 삭제할 config_id 리스트 (None이면 전체 삭제)
            
        Returns:
            삭제된 레코드 수
        """
        query = self.db.query(IntradaySignalDetectionResult).filter(
            IntradaySignalDetectionResult.ticker_id == ticker_id
        )
        
        if config_ids:
            query = query.filter(IntradaySignalDetectionResult.config_id.in_(config_ids))
        
        count = query.count()
        query.delete(synchronize_session=False)
        
        logger.info(f"Deleted {count} intraday signals for ticker_id={ticker_id}, config_ids={config_ids}")
        return count
    
    def delete_ticker_signals_before(
        self,
        ticker_id: int,
        config_ids: List[int],
        cutoff_datetime: datetime
    ) -> int:
        """
        특정 티커의 특정 시간 이전 시그널만 삭제
        
        Args:
            ticker_id: 티커 ID
            config_ids: 삭제할 config_id 리스트
            cutoff_datetime: 기준 날짜/시간 (이 시간 < 의 데이터만 삭제)
            
        Returns:
            삭제된 레코드 수
        """
        query = self.db.query(IntradaySignalDetectionResult).filter(
            IntradaySignalDetectionResult.ticker_id == ticker_id,
            IntradaySignalDetectionResult.config_id.in_(config_ids),
            IntradaySignalDetectionResult.detected_at < cutoff_datetime
        )
        
        count = query.count()
        query.delete(synchronize_session=False)
        
        logger.info(f"Deleted {count} old intraday signals (before {cutoff_datetime}) for ticker_id={ticker_id}, config_ids={config_ids}")
        return count
    
    def upsert_results(
        self,
        results: List[dict]
    ) -> int:
        """
        결과를 upsert (PostgreSQL의 ON CONFLICT 사용)
        
        Args:
            results: 결과 딕셔너리 리스트
                [
                    {
                        "ticker_id": 1,
                        "config_id": 10,
                        "signal_datetime": datetime(...),
                        "close": 100.0,
                        "change_7_24d": 0.05,
                        "past_slope": 0.01,
                        "past_std": 0.02,
                        "atr": 0.03,
                        "shape_vector": [0.1, 0.2, ...],
                        "vector_dim": 27,
                        "vector_m": 10,
                        "prior_candles": 50,
                        "signal_score": None
                    },
                    ...
                ]
        
        Returns:
            upsert된 레코드 수
        """
        if not results:
            return 0
        
        # PostgreSQL ON CONFLICT DO UPDATE
        stmt = insert(IntradaySignalDetectionResult).values(results)
        
        stmt = stmt.on_conflict_do_update(
            constraint='uq_intraday_result_ticker_datetime_config',
            set_={
                "close": stmt.excluded.close,
                "change_7_24d": stmt.excluded.change_7_24d,
                "past_slope": stmt.excluded.past_slope,
                "past_std": stmt.excluded.past_std,
                "atr": stmt.excluded.atr,
                "shape_vector": stmt.excluded.shape_vector,
                "vector_dim": stmt.excluded.vector_dim,
                "vector_m": stmt.excluded.vector_m,
                "prior_candles": stmt.excluded.prior_candles,
                "signal_score": stmt.excluded.signal_score,
                "detected_at": func.now()  # ✅ 항상 현재 시각으로 갱신
            }
        )
        
        self.db.execute(stmt)
        
        logger.info(f"Upserted {len(results)} intraday signal results")
        return len(results)
    
    def search_similar_vectors(
        self,
        query_vector: List[float],
        direction_filter: Optional[str] = None,
        version_filter: Optional[str] = None,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        분봉 shape_vector 기반 코사인 유사도 검색
        
        Args:
            query_vector: 쿼리 기준 벡터
            direction_filter: "UP"/"DOWN" 필터
            version_filter: "v1"/"v2"/"v3" 버전 필터
            top_k: 상위 결과 개수
            
        Returns:
            유사 시그널 리스트
        """
        params = {
            "query_vec": query_vector,
            "top_k": top_k,
        }
        
        where_clauses = []
        if direction_filter:
            where_clauses.append("c.direction = :direction")
            params["direction"] = direction_filter
        
        if version_filter:
            where_clauses.append("c.version = :version")
            params["version"] = version_filter
        
        where_clause = ""
        if where_clauses:
            where_clause = "AND " + " AND ".join(where_clauses)
        
        sql = text(f"""
            WITH pairwise AS (
                SELECT
                    r.result_id,
                    r.ticker_id,
                    r.config_id,
                    c.direction,
                    r.signal_datetime,
                    r.close,
                    r.change_7_24d,
                    v.val AS a,
                    q.val AS b
                FROM trading.intraday_signal_detection_result r
                JOIN trading.intraday_signal_detection_config c
                    ON r.config_id = c.config_id
                CROSS JOIN LATERAL unnest(r.shape_vector) WITH ORDINALITY AS v(val, idx)
                JOIN LATERAL unnest(:query_vec) WITH ORDINALITY AS q(val, idx)
                    ON v.idx = q.idx
                WHERE array_length(r.shape_vector, 1) = array_length(:query_vec, 1)
                {where_clause}
            )
            SELECT
                result_id,
                ticker_id,
                config_id,
                direction,
                signal_datetime,
                close,
                change_7_24d,
                SUM(a * b) / (SQRT(SUM(a * a)) * SQRT(SUM(b * b))) AS similarity
            FROM pairwise
            GROUP BY result_id, ticker_id, config_id, direction, signal_datetime, close, change_7_24d
            ORDER BY similarity DESC
            LIMIT :top_k
        """)
        
        result = self.db.execute(sql, params)
        
        # Ticker 정보 조회를 위한 맵
        from app.shared.models.ticker import Ticker
        ticker_map = {}
        
        results = []
        for row in result:
            # Ticker 정보 캐싱
            if row.ticker_id not in ticker_map:
                t = self.db.query(Ticker).filter(Ticker.id == row.ticker_id).first()
                if t:
                    ticker_map[row.ticker_id] = {"symbol": t.symbol, "exchange": t.exchange}
                else:
                    ticker_map[row.ticker_id] = {"symbol": "N/A", "exchange": "N/A"}
            
            ticker_info = ticker_map[row.ticker_id]
            
            results.append({
                "result_id": row.result_id,
                "ticker_id": row.ticker_id,
                "symbol": ticker_info["symbol"],
                "exchange": ticker_info["exchange"],
                "signal_datetime": row.signal_datetime,
                "direction": row.direction,
                "close": row.close,
                "change_7_24d": row.change_7_24d,
                "similarity": row.similarity,
                "config_id": row.config_id
            })
        
        logger.info(f"Found {len(results)} similar intraday signals")
        return results

