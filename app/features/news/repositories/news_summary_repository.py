from sqlalchemy.orm import Session
from typing import List, Optional

from app.features.news.models.news import News
from app.features.news.models.news_summary import NewsSummary
from app.features.news.models.news_ticker import NewsTicker
from app.shared.models.ticker import Ticker


class NewsSummaryRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_ticker_by_id(self, ticker_id: int) -> Optional[Ticker]:
        """티커 ID로 티커 조회"""
        return self.db.query(Ticker).filter(Ticker.id == ticker_id).first()


    def get_related_news_by_ticker_id(self, ticker_id: int, limit: int = 10) -> List[dict]:
        """
        티커와 관련된 뉴스를 조회 (뉴스티커 직접 매핑, confidence >= 0.8)
        
        Args:
            ticker_id: 티커 ID
            limit: 반환할 뉴스 개수 (기본값: 10)
            
        Returns:
            List[dict]: 뉴스 요약 목록
        """
        # 직접 매핑된 뉴스 조회 (confidence >= 0.8, 최신순)
        news_list = self.db.query(
            News.id,
            News.published_date_kst,
            NewsSummary.summary_text
        ).join(
            NewsSummary, News.id == NewsSummary.news_id
        ).join(
            NewsTicker, News.id == NewsTicker.news_id
        ).filter(
            NewsTicker.ticker_id == ticker_id,
            NewsTicker.confidence >= 0.8,
            NewsSummary.lang == 'ko'
        ).group_by(
            News.id, News.published_date_kst, NewsSummary.summary_text
        ).order_by(
            News.published_at.desc()  # 최신 발행 기준 정렬
        ).limit(limit).all()
        
        result = []
        for news in news_list:
            result.append({
                'id': news.id,
                'summary_text': news.summary_text,
                'published_date_kst': news.published_date_kst.isoformat() if news.published_date_kst else None
            })
        
        return result

    def get_news_count_by_ticker_id(self, ticker_id: int) -> int:
        """
        티커와 관련된 뉴스 개수 조회 (뉴스티커 직접 매핑, confidence >= 0.8)
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            int: 뉴스 개수
        """
        count = self.db.query(NewsTicker.news_id).join(
            NewsSummary, NewsTicker.news_id == NewsSummary.news_id
        ).filter(
            NewsTicker.ticker_id == ticker_id,
            NewsTicker.confidence >= 0.8,
            NewsSummary.lang == 'ko'
        ).distinct().count()
        
        return count
