# app/features/recommendation/services/kr_recommendation_service.py

import logging
from typing import Dict, Any
from datetime import datetime
from app.features.recommendation.repositories.recommendation_repository import RecommendationRepository

logger = logging.getLogger("kr_recommendation_service")


class KrRecommendationService:
    """추천 관련 비즈니스 로직을 담당하는 서비스 (국내주식 전용)"""
    
    def __init__(self, repository: RecommendationRepository):
        self.repository = repository
    
    def get_candidate_tickers(self, days_back: int, country: str = "KR") -> Dict[str, Any]:
        """
        추천 후보 티커 목록을 조회합니다.
        
        Args:
            days_back: 현재일 기준 몇 일 전까지의 뉴스를 참조할지 (1~5)
            country: 국가 코드 ("US" 또는 "KR")
            
        Returns:
            Dict[str, Any]: 후보 티커 정보와 메타데이터
        """
        # 입력 검증
        if not (1 <= days_back <= 5):
            raise ValueError("days_back은 1~5 사이의 값이어야 합니다")
        
        if country not in ["US", "KR"]:
            raise ValueError("country는 'US' 또는 'KR'이어야 합니다")
        
        logger.info(f"추천 후보 티커 조회 시작: {days_back}일 전 뉴스 참조, 국가: {country}")
        
        try:
            # 후보 티커 목록 조회 (country 파라미터 포함)
            candidate_tickers = self.repository.get_candidate_tickers_for_recommendation(days_back, country)
            
            result = {
                "candidate_tickers": candidate_tickers,
                "total_count": len(candidate_tickers),
                "days_back": days_back,
                "country": country,
                "generated_at": datetime.now().isoformat(),
                "criteria": {
                    "news_period": f"최근 {days_back}일간 발행된 뉴스",
                    "ticker_source": f"뉴스티커 직접 매핑이 존재하는 티커 (confidence >= 0.5, 국가: {country})",
                    "exclusion": "해당 뉴스 생성일 이후 추천이 생성되지 않은 티커만"
                }
            }
            
            logger.info(f"추천 후보 티커 조회 완료: {len(candidate_tickers)}개 티커 (국가: {country})")
            return result
            
        except Exception as e:
            logger.error(f"추천 후보 티커 조회 중 오류 발생: {str(e)}")
            raise
    
    def validate_ticker_eligibility(self, ticker_id: int, days_back: int = 3, country: str = "KR") -> Dict[str, Any]:
        """
        특정 티커가 추천 후보가 될 수 있는지 검증합니다.
        
        Args:
            ticker_id: 검증할 티커 ID
            days_back: 참조할 뉴스 기간 (일수)
            country: 국가 코드 ("US" 또는 "KR")
            
        Returns:
            Dict[str, Any]: 검증 결과와 상세 정보
        """
        try:
            # 후보 티커 목록 조회 (country 파라미터 포함)
            candidates = self.repository.get_candidate_tickers_for_recommendation(days_back, country)
            
            # 해당 티커가 후보 목록에 있는지 확인
            is_eligible = any(ticker["id"] == ticker_id for ticker in candidates)
            
            result = {
                "ticker_id": ticker_id,
                "is_eligible": is_eligible,
                "days_back": days_back,
                "country": country,
                "checked_at": datetime.now().isoformat()
            }
            
            if is_eligible:
                ticker_info = next(ticker for ticker in candidates if ticker["id"] == ticker_id)
                result["ticker_info"] = ticker_info
                result["message"] = f"해당 티커는 {country} 추천 후보입니다"
            else:
                result["message"] = f"해당 티커는 {country} 추천 후보 조건을 만족하지 않습니다"
            
            return result
            
        except Exception as e:
            logger.error(f"티커 적격성 검증 중 오류 발생: {str(e)}")
            raise
