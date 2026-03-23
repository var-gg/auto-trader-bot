# app/features/signals/repositories/vec40_repository.py
"""
Vec40 Repository
- trend_detection_result_vec40 테이블 관리
- HNSW 인덱스 기반 고속 벡터 유사도 검색
"""
from __future__ import annotations
from typing import List, Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.features.signals.models.trend_detection_result_vec40 import TrendDetectionResultVec40


class Vec40Repository:
    """
    Vec40 테이블 Repository
    - 40차원 벡터 유사도 검색
    - HNSW 인덱스 활용
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def search_similar_vectors(
        self,
        query_vector: List[float],
        direction_filter: Optional[str] = None,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        trend_detection_result_vec40 테이블의 shape_vector40 기반 코사인 유사도 검색
        HNSW 인덱스를 활용한 고속 벡터 유사도 검색

        Args:
            query_vector: 쿼리 기준 벡터 (Python list[float], 40차원)
            direction_filter: "UP"/"DOWN" 중 선택하면 필터링
            top_k: 유사도 상위 결과 개수

        Returns:
            [{"ticker_id": int, "signal_date": date, "direction": str, "similarity": float}, ...]
        """
        # 벡터를 PostgreSQL vector 리터럴 형식으로 변환
        vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"
        
        # 파라미터 준비 (vector는 리터럴로 직접 삽입)
        params = {"top_k": top_k}
        
        where_clauses = []
        if direction_filter:
            where_clauses.append("direction = :direction")
            params["direction"] = direction_filter
        
        where_clause = ""
        if where_clauses:
            where_clause = "WHERE " + " AND ".join(where_clauses)

        # HNSW 인덱스를 활용한 코사인 유사도 검색
        # <=> 연산자는 pgvector의 코사인 거리 연산자
        # 유사도 = 1 - 코사인 거리
        # ✅ vector 리터럴은 SQL 문자열에 직접 삽입 (바인딩 파라미터 불가)
        sql = text(f"""
            SELECT
                ticker_id,
                signal_date,
                direction,
                1 - (shape_vector40 <=> '{vector_str}'::vector) AS similarity
            FROM trading.trend_detection_result_vec40
            {where_clause}
            ORDER BY shape_vector40 <=> '{vector_str}'::vector
            LIMIT :top_k
        """)

        result = self.db.execute(sql, params)

        results = []
        for row in result:
            results.append({
                "ticker_id": row.ticker_id,
                "signal_date": row.signal_date,
                "direction": row.direction,
                "similarity": float(row.similarity or 0.0),
            })

        return results

