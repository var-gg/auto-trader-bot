# app/features/signals/repositories/similarity_analysis_repository.py
from __future__ import annotations
from typing import Optional
from sqlalchemy.orm import Session

from app.features.signals.models.similarity_analysis import SimilarityAnalysis


class SimilarityAnalysisRepository:
    """
    유사도 분석 결과 Repository
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def upsert(
        self,
        ticker_id: int,
        ticker_name_ko: Optional[str],
        exchange: Optional[str],
        p_up: float,
        p_down: float,
        exp_up: float,
        exp_down: float,
        top_similarity: float
    ) -> SimilarityAnalysis:
        """
        유사도 분석 결과를 Upsert (Insert or Update)
        
        Args:
            ticker_id: 티커 ID
            ticker_name_ko: 종목명 (한글)
            exchange: 거래소 코드
            p_up: 상승 확률
            p_down: 하락 확률
            exp_up: 상승 시 기대 변동률
            exp_down: 하락 시 기대 변동률
            top_similarity: 가장 높은 유사도 점수
            
        Returns:
            저장된 SimilarityAnalysis 객체
        """
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        
        stmt = pg_insert(SimilarityAnalysis).values(
            ticker_id=ticker_id,
            ticker_name_ko=ticker_name_ko,
            exchange=exchange,
            p_up=p_up,
            p_down=p_down,
            exp_up=exp_up,
            exp_down=exp_down,
            top_similarity=top_similarity
        )
        
        # ON CONFLICT DO UPDATE (PK 기반)
        from sqlalchemy.sql import func
        
        stmt = stmt.on_conflict_do_update(
            index_elements=['ticker_id'],  # PRIMARY KEY 컬럼명
            set_={
                "ticker_name_ko": stmt.excluded.ticker_name_ko,
                "exchange": stmt.excluded.exchange,
                "p_up": stmt.excluded.p_up,
                "p_down": stmt.excluded.p_down,
                "exp_up": stmt.excluded.exp_up,
                "exp_down": stmt.excluded.exp_down,
                "top_similarity": stmt.excluded.top_similarity,
                "updated_at": func.now(),  # 명시적으로 현재 시간 설정
            }
        )
        
        self.db.execute(stmt)
        self.db.commit()
        
        # 저장된 레코드 조회 후 반환
        result = self.db.query(SimilarityAnalysis).filter(
            SimilarityAnalysis.ticker_id == ticker_id
        ).first()
        
        return result
    
    def get_by_ticker(
        self,
        ticker_id: int
    ) -> Optional[SimilarityAnalysis]:
        """
        특정 티커의 분석 결과 조회
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            SimilarityAnalysis 객체 (없으면 None)
        """
        return self.db.query(SimilarityAnalysis).filter(
            SimilarityAnalysis.ticker_id == ticker_id
        ).first()

