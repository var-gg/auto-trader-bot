# app/features/recommendation/repositories/recommendation_repository.py

from datetime import datetime, timedelta, date
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, distinct
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation
from app.features.news.models.news import News
from app.features.news.models.news_ticker import NewsTicker
from app.features.news.models.kis_news import KisNews
from app.shared.models.ticker import Ticker
from app.features.signals.services.signal_detection_service import SignalDetectionService
from app.features.signals.models.similarity_models import SimilaritySearchRequest
from app.features.signals.models.signal_models import AlgorithmVersion
import logging

logger = logging.getLogger(__name__)


class RecommendationRepository:
    """추천 관련 데이터 접근을 담당하는 리포지토리"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_candidate_tickers_for_recommendation(self, days_back: int, country: str = "US") -> List[dict]:
        """
        추천 후보 티커 목록을 조회합니다.
        
        Args:
            days_back: 현재일 기준 몇 일 전까지의 뉴스를 참조할지
            country: 국가 코드 ("US" 또는 "KR")
            
        Returns:
            List[dict]: 추천 후보 티커 정보 리스트
        """
        # 기준일 계산 (현재일 - days_back)
        reference_date = datetime.now().date() - timedelta(days=days_back)
        
        # 뉴스티커 매핑과 KIS 뉴스에서 티커 조회 (UNION)
        news_ticker_tickers = self._get_tickers_from_news_ticker_mapping(reference_date, country)
        kis_news_tickers = self._get_tickers_from_kis_news(reference_date, country)
        
        # 두 소스 합치기 (중복 제거)
        all_ticker_ids = list(set(news_ticker_tickers + kis_news_tickers))
        logger.info(f"📰 뉴스 소스별 티커 수 - NewsTicker: {len(news_ticker_tickers)}, KIS News: {len(kis_news_tickers)}, 합계(중복제거): {len(all_ticker_ids)}")
        
        # 추천이 생성되지 않은 티커들만 필터링
        eligible_tickers = self._filter_tickers_without_recent_recommendations(
            all_ticker_ids, reference_date, country
        )
        
        # 🔍 유사도 기반 필터링: 10기간 top3 direction이 모두 동일한 티커만 남김
        filtered_by_similarity = self._filter_by_signal_similarity(eligible_tickers)
        
        return filtered_by_similarity
    
    def _get_tickers_from_news_ticker_mapping(self, reference_date: datetime, country: str = "US") -> List[int]:
        """뉴스티커 매핑에서 티커 ID 목록을 조회합니다 (confidence >= 0.5, country 필터링, 티커 벡터 존재)."""
        from app.features.fundamentals.models.ticker_vector import TickerVector
        
        query = (
            self.db.query(distinct(NewsTicker.ticker_id))
            .join(News, NewsTicker.news_id == News.id)
            .join(Ticker, NewsTicker.ticker_id == Ticker.id)
            .join(TickerVector, TickerVector.ticker_id == Ticker.id)  # 티커 벡터가 존재하는 것만
            .filter(
                News.published_date_kst >= reference_date,
                NewsTicker.ticker_id.isnot(None),
                NewsTicker.confidence >= 0.8,  # US: 0.6 이상, 그 외: 0.7 이상
                NewsTicker.method == "gpt5_reevaluation",
                Ticker.country == country  # 국가 필터링
            )
        )
        return [ticker_id[0] for ticker_id in query.all()]
    
    def _get_tickers_from_kis_news(self, reference_date: datetime, country: str = "US") -> List[int]:
        """KIS 뉴스에서 티커 ID 목록을 조회합니다 (country 필터링, 티커 벡터 존재)."""
        from app.features.fundamentals.models.ticker_vector import TickerVector
        
        query = (
            self.db.query(distinct(KisNews.ticker_id))
            .join(Ticker, KisNews.ticker_id == Ticker.id)
            .join(TickerVector, TickerVector.ticker_id == Ticker.id)  # 티커 벡터가 존재하는 것만
            .filter(
                func.date(KisNews.published_at) >= reference_date,  # published_at을 date로 변환하여 비교
                KisNews.ticker_id.isnot(None),
                Ticker.country == country  # 국가 필터링
            )
        )
        return [ticker_id[0] for ticker_id in query.all()]
    
    def _filter_tickers_without_recent_recommendations(
        self, 
        ticker_ids: List[int], 
        reference_date: datetime,
        country: str = "US"
    ) -> List[dict]:
        """해당 뉴스 생성일 이후 추천이 생성되지 않은 티커들만 필터링합니다."""
        if not ticker_ids:
            return []
        
        # 각 티커별로 가장 최근 뉴스 생성일을 조회 (뉴스티커 + KIS 뉴스)
        latest_news_dates = {}
        
        # 1. 뉴스티커 매핑을 통한 뉴스 생성일 조회 (confidence >= 0.9, country 필터링)
        news_ticker_subquery = (
            self.db.query(
                NewsTicker.ticker_id,
                func.max(News.created_at).label('latest_news_created_at')
            )
            .join(News, NewsTicker.news_id == News.id)
            .join(Ticker, NewsTicker.ticker_id == Ticker.id)
            .filter(
                NewsTicker.ticker_id.in_(ticker_ids),
                News.published_date_kst >= reference_date,
                NewsTicker.confidence >= 0.9,
                Ticker.country == country
            )
            .group_by(NewsTicker.ticker_id)
        )
        
        # 뉴스티커에서 가져온 날짜 저장
        for ticker_id, latest_date in news_ticker_subquery.all():
            latest_news_dates[ticker_id] = latest_date
        
        # 2. KIS 뉴스를 통한 뉴스 생성일 조회 (country 필터링)
        kis_news_subquery = (
            self.db.query(
                KisNews.ticker_id,
                func.max(KisNews.created_at).label('latest_kis_news_created_at')
            )
            .join(Ticker, KisNews.ticker_id == Ticker.id)
            .filter(
                KisNews.ticker_id.in_(ticker_ids),
                func.date(KisNews.published_at) >= reference_date,
                Ticker.country == country
            )
            .group_by(KisNews.ticker_id)
        )
        
        # KIS 뉴스에서 가져온 날짜와 비교하여 더 최근 것으로 업데이트
        for ticker_id, latest_date in kis_news_subquery.all():
            if ticker_id not in latest_news_dates or latest_date > latest_news_dates[ticker_id]:
                latest_news_dates[ticker_id] = latest_date
        
        # 최근 뉴스 생성일 이후 추천이 없는 티커들 조회
        eligible_tickers = []
        for ticker_id in ticker_ids:
            # 해당 티커의 최근 뉴스 생성일 조회
            latest_news_date = latest_news_dates.get(ticker_id)
            if not latest_news_date:
                # 뉴스가 없는 경우는 제외
                continue
            
            # 해당 날짜 이후 추천이 있는지 확인
            recent_recommendation = (
                self.db.query(AnalystRecommendation)
                .filter(
                    AnalystRecommendation.ticker_id == ticker_id,
                    AnalystRecommendation.recommended_at > latest_news_date
                )
                .first()
            )
            
            # 추천이 없는 경우만 포함
            if not recent_recommendation:
                # 티커 정보 조회 (country 필터링 포함)
                ticker = (
                    self.db.query(Ticker.id, Ticker.symbol, Ticker.exchange, Ticker.country)
                    .filter(
                        Ticker.id == ticker_id,
                        Ticker.country == country
                    )
                    .first()
                )
                
                if ticker:
                    eligible_tickers.append({
                        "id": ticker.id,
                        "symbol": ticker.symbol,
                        "exchange": ticker.exchange,
                        "country": ticker.country
                    })
        
        return eligible_tickers
    
    def _filter_by_signal_similarity(self, tickers: List[dict]) -> List[dict]:
        """
        유사도 검색을 통해 티커를 필터링합니다.
        
        10기간에 상위 3개의 유사 시그널을 조회하여,
        3개의 direction이 모두 동일한 티커만 반환합니다.
        
        Args:
            tickers: 티커 정보 리스트 [{"id": ..., "symbol": ..., ...}]
            
        Returns:
            List[dict]: 유사도 필터링을 통과한 티커 정보 리스트
        """
        if not tickers:
            return []
        
        signal_service = SignalDetectionService(self.db)
        filtered_tickers = []
        
        logger.info(f"🔍 유사도 필터링 시작 - 대상 티커: {len(tickers)}개")
        
        for ticker in tickers:
            ticker_id = ticker["id"]
            symbol = ticker["symbol"]
            
            try:
                # 유사도 검색 요청 (lookback=10, top_k=3, version=v3)
                request = SimilaritySearchRequest(
                    ticker_id=ticker_id,
                    reference_date=None,  # 오늘 기준
                    lookback=10,
                    top_k=3,
                    direction_filter=None,  # 전체 방향
                    version=AlgorithmVersion.V3  # 알고리즘 버전 v3 (혼합형)
                )
                
                response = signal_service.search_similar_signals(request)
                
                # 3개 미만이면 제외
                if len(response.similar_signals) < 3:
                    logger.info(f"❌ {symbol} (ID:{ticker_id}): 유사 시그널 부족 ({len(response.similar_signals)}개)")
                    continue
                
                # 상위 3개의 direction 확인
                directions = [signal.direction for signal in response.similar_signals[:3]]
                
                # 모두 동일한지 확인
                if len(set(directions)) == 1:
                    # 모두 동일함 → 통과
                    direction = directions[0]
                    logger.info(f"✅ {symbol} (ID:{ticker_id}): 방향 일치 ({direction}) - 후보 유지")
                    filtered_tickers.append(ticker)
                else:
                    # 방향이 다름 → 제외
                    logger.info(f"❌ {symbol} (ID:{ticker_id}): 방향 불일치 ({directions}) - 후보 제외")
                    
            except Exception as e:
                # 에러 발생 시 해당 티커 제외 (데이터 부족 등)
                logger.warning(f"⚠️ {symbol} (ID:{ticker_id}): 유사도 검색 실패 - {str(e)}")
                continue
        
        logger.info(f"🔍 유사도 필터링 완료 - 통과: {len(filtered_tickers)}개 / 전체: {len(tickers)}개")
        
        return filtered_tickers
    
    def create_recommendation(self, recommendation_data: dict) -> AnalystRecommendation:
        """새로운 추천을 생성합니다."""
        recommendation = AnalystRecommendation(**recommendation_data)
        self.db.add(recommendation)
        self.db.commit()
        self.db.refresh(recommendation)
        return recommendation
    
    def get_recommendations_by_ticker(
        self, 
        ticker_id: int, 
        limit: int = 10
    ) -> List[AnalystRecommendation]:
        """특정 티커의 추천 목록을 조회합니다."""
        return (
            self.db.query(AnalystRecommendation)
            .filter(AnalystRecommendation.ticker_id == ticker_id)
            .order_by(AnalystRecommendation.recommended_at.desc())
            .limit(limit)
            .all()
        )
    
    def get_active_recommendations(self) -> List[AnalystRecommendation]:
        """현재 유효한 추천 목록을 조회합니다."""
        now = datetime.now()
        return (
            self.db.query(AnalystRecommendation)
            .filter(AnalystRecommendation.valid_until > now)
            .order_by(AnalystRecommendation.recommended_at.desc())
            .all()
        )
