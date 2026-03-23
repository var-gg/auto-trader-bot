# app/features/signals/repositories/trend_detection_repository.py
from __future__ import annotations
from typing import List, Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from decimal import Decimal

from app.features.signals.models.trend_detection_config import TrendDetectionConfig
from app.features.signals.models.trend_detection_result import TrendDetectionResult


class TrendDetectionRepository:
    """
    트렌드 탐지 설정/결과 Repository
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
    ) -> TrendDetectionConfig:
        """
        설정 조회 또는 생성 (upsert)
        동일한 파라미터 조합 + 버전이면 기존 config_id 반환
        
        Args:
            direction: 시그널 방향 (UP/DOWN)
            lookback: 직전 구간 확인 기간
            future_window: 이후 구간 평가 기간
            min_change: 최소 변동률
            max_reverse: 반대 방향 최대 허용폭
            flatness_k: 평탄성 허용치 (ATR 배수)
            atr_window: ATR 계산 윈도우
            version: 알고리즘 버전 (v1, v2, ...)
        
        Returns:
            TrendDetectionConfig 객체
        """
        # 기존 설정 조회
        existing = self.db.query(TrendDetectionConfig).filter(
            TrendDetectionConfig.direction == direction,
            TrendDetectionConfig.lookback == lookback,
            TrendDetectionConfig.future_window == future_window,
            TrendDetectionConfig.min_change == Decimal(str(min_change)),
            TrendDetectionConfig.max_reverse == Decimal(str(max_reverse)),
            TrendDetectionConfig.flatness_k == Decimal(str(flatness_k)),
            TrendDetectionConfig.atr_window == atr_window,
            TrendDetectionConfig.version == version
        ).first()
        
        if existing:
            return existing
        
        # 새로 생성
        new_config = TrendDetectionConfig(
            direction=direction,
            lookback=lookback,
            future_window=future_window,
            min_change=Decimal(str(min_change)),
            max_reverse=Decimal(str(max_reverse)),
            flatness_k=Decimal(str(flatness_k)),
            atr_window=atr_window,
            version=version
        )
        self.db.add(new_config)
        self.db.commit()
        self.db.refresh(new_config)
        
        return new_config
    
    def upsert_results(self, results: List[Dict[str, Any]]) -> int:
        """
        결과 일괄 upsert
        (ticker_id, signal_date, config_id) 조합이 동일하면 갱신
        
        Args:
            results: 결과 딕셔너리 리스트
            
        Returns:
            upsert 시도 건수
        """
        if not results:
            return 0
        
        insert_stmt = insert(TrendDetectionResult).values(results)
        
        update_cols = {
            "close": insert_stmt.excluded.close,
            "change_7_24d": insert_stmt.excluded.change_7_24d,
            "past_slope": insert_stmt.excluded.past_slope,
            "past_std": insert_stmt.excluded.past_std,
            "shape_vector": insert_stmt.excluded.shape_vector,
            "vector_dim": insert_stmt.excluded.vector_dim,
            "vector_m": insert_stmt.excluded.vector_m,
            "prior_candles": insert_stmt.excluded.prior_candles,
            "signal_score": insert_stmt.excluded.signal_score,
            "detected_at": insert_stmt.excluded.detected_at,
        }
        
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["ticker_id", "signal_date", "config_id"],
            set_=update_cols
        )
        
        self.db.execute(upsert_stmt)
        self.db.commit()
        
        return len(results)
    
    def get_results_by_ticker(
        self,
        ticker_id: int,
        config_id: Optional[int] = None,
        limit: int = 100
    ) -> List[TrendDetectionResult]:
        """
        티커별 결과 조회
        
        Args:
            ticker_id: 티커 ID
            config_id: 설정 ID (None이면 전체)
            limit: 조회 개수 제한
            
        Returns:
            결과 리스트
        """
        query = self.db.query(TrendDetectionResult).filter(
            TrendDetectionResult.ticker_id == ticker_id
        )
        
        if config_id is not None:
            query = query.filter(TrendDetectionResult.config_id == config_id)
        
        query = query.order_by(TrendDetectionResult.signal_date.desc())
        query = query.limit(limit)
        
        return query.all()
    
    def delete_results_by_ticker(self, ticker_id: int, config_id: Optional[int] = None) -> int:
        """
        티커별 결과 삭제
        
        Args:
            ticker_id: 티커 ID
            config_id: 설정 ID (None이면 전체)
            
        Returns:
            삭제된 행 수
        """
        query = self.db.query(TrendDetectionResult).filter(
            TrendDetectionResult.ticker_id == ticker_id
        )
        
        if config_id is not None:
            query = query.filter(TrendDetectionResult.config_id == config_id)
        
        count = query.delete()
        self.db.commit()
        
        return count
    
    def get_all_results_with_vectors(
        self,
        direction_filter: Optional[str] = None,
        version_filter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[TrendDetectionResult]:
        """
        벡터가 있는 모든 결과 조회 (유사도 검색용)
        
        Args:
            direction_filter: 방향 필터 (UP/DOWN, None이면 전체)
            version_filter: 버전 필터 (v1, v2, ..., None이면 전체)
            limit: 조회 개수 제한
            
        Returns:
            결과 리스트
        """
        query = self.db.query(TrendDetectionResult).filter(
            TrendDetectionResult.shape_vector.isnot(None)
        )
        
        # direction_filter 또는 version_filter가 있으면 join 필요
        if direction_filter or version_filter:
            query = query.join(
                TrendDetectionConfig,
                TrendDetectionResult.config_id == TrendDetectionConfig.config_id
            )
            
            if direction_filter:
                query = query.filter(TrendDetectionConfig.direction == direction_filter)
            
            if version_filter:
                query = query.filter(TrendDetectionConfig.version == version_filter)
        
        query = query.order_by(TrendDetectionResult.signal_date.desc())
        
        if limit is not None:
            query = query.limit(limit)
        
        return query.all()
    
    def search_similar_vectors(
        self,
        query_vector: List[float],
        direction_filter: Optional[str] = None,
        version_filter: Optional[str] = None,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        shape_vector(_float8[]) 기반 코사인 유사도 검색 + direction 조인

        Args:
            query_vector: 쿼리 기준 벡터 (Python list[float])
            direction_filter: "UP"/"DOWN" 중 선택하면 필터링
            version_filter: "v1"/"v2" 등 버전 필터링
            top_k: 유사도 상위 결과 개수

        Returns:
            [{"result": TrendDetectionResult, "similarity": float, "direction": str}, ...]
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
                    r.signal_date,
                    r.close,
                    r.change_7_24d,
                    v.val AS a,
                    q.val AS b
                FROM trading.trend_detection_result r
                JOIN trading.trend_detection_config c
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
                signal_date,
                close,
                change_7_24d,
                SUM(a * b) / (SQRT(SUM(a * a)) * SQRT(SUM(b * b))) AS similarity
            FROM pairwise
            GROUP BY result_id, ticker_id, config_id, direction, signal_date, close, change_7_24d
            ORDER BY similarity DESC
            LIMIT :top_k
        """)

        result = self.db.execute(sql, params)

        results = []
        for row in result:
            trend_result = TrendDetectionResult(
                result_id=row.result_id,
                ticker_id=row.ticker_id,
                config_id=row.config_id,
                signal_date=row.signal_date,
                close=row.close,
                change_7_24d=row.change_7_24d,
            )
            results.append({
                "result": trend_result,
                "similarity": float(row.similarity or 0.0),
                "direction": row.direction,
            })

        return results