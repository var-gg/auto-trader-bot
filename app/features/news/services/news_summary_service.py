from sqlalchemy.orm import Session
from typing import Dict, Any, List, Optional
import logging

from app.features.news.repositories.news_summary_repository import NewsSummaryRepository
from app.shared.models.ticker import Ticker

logger = logging.getLogger(__name__)


class NewsSummaryService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = NewsSummaryRepository(db)

    def get_news_summary_for_ticker(self, ticker_id: int, limit: int = 10) -> Dict[str, Any]:
        """
        티커와 관련된 뉴스 요약을 조회하여 애널리스트 AI 프롬프트용 데이터 반환
        (뉴스티커 직접 매핑, confidence >= 0.8, 최신순)
        
        Args:
            ticker_id: 티커 ID
            limit: 반환할 뉴스 개수 (기본값: 10, 최대: 20)
        
        Returns:
            Dict[str, Any]: 티커 정보와 관련 뉴스 요약 목록
        """
        try:
            # 입력 검증
            if limit > 20:
                limit = 20
            if limit < 5:
                limit = 5

            # 티커 정보 조회
            ticker = self.repository.get_ticker_by_id(ticker_id)
            if not ticker:
                return {"error": f"Ticker not found: {ticker_id}"}

            # 관련 뉴스 조회 (confidence >= 0.8, 최신순)
            related_news = self.repository.get_related_news_by_ticker_id(ticker_id, limit)
            
            # 총 뉴스 개수 조회
            total_count = self.repository.get_news_count_by_ticker_id(ticker_id)

            # 결과 구성
            result = {
                "ticker": {
                    "id": ticker.id,
                    "symbol": ticker.symbol,
                    "exchange": ticker.exchange,
                    "type": ticker.type
                },
                "news_summaries": related_news,
                "returned_count": len(related_news),
                "total_available": total_count,
                "limit_requested": limit
            }

            logger.info(f"Retrieved {len(related_news)} news summaries for ticker {ticker.symbol} (ID: {ticker_id})")
            return result

        except Exception as e:
            logger.error(f"Error getting news summary for ticker {ticker_id}: {str(e)}")
            return {"error": str(e)}

