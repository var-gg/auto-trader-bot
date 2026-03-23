# app/features/news/repositories/news_v2_repository.py
from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import desc, select, and_, func, text
from datetime import datetime, timezone, timedelta

from app.features.news.models.news import News, NewsStatus, ContentStatus
from app.features.news.models.news_vector import NewsVector
from app.features.news.models.news_ticker import NewsTicker
from app.features.news.models.news_summary import NewsSummary
from app.features.news.models.news_anchor_vector import NewsAnchorVector
from app.features.fundamentals.models.ticker_vector import TickerVector
from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n
from app.core.config import RSS_SOURCES

class NewsV2Repository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_link(self, link: str) -> News | None:
        return self.db.execute(select(News).where(News.link == link)).scalar_one_or_none()

    def create_if_not_exists(self, *, title: str, link: str, published_at, published_date_kst: str | None, source: str) -> News:
        """RSS 수집용 - 기존과 동일"""
        ex = self.get_by_link(link)
        if ex:
            return ex
        row = News(title=title, link=link, published_at=published_at, published_date_kst=published_date_kst, source=source)
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def list_for_content_fetch_v2(self, limit: int = 200, max_retry: int = 2):
        """V2 본문 크롤링 대상: RAW 상태이면서 1일 이내 뉴스 (최신순, 언론사별로 균등 배분)"""
        one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)
        
        # RSS_SOURCES에서 언론사 이름 추출
        source_names = [source["name"] for source in RSS_SOURCES]
        
        if not source_names:
            # RSS_SOURCES가 비어있는 경우 기존 방식대로
            return self.db.execute(
                select(News).where(
                    and_(
                        News.status == NewsStatus.RAW,
                        News.published_at >= one_day_ago,
                        News.content_status != ContentStatus.SUCCESS,
                        News.content_retry_count < max_retry
                    )
                ).order_by(desc(News.published_at)).limit(limit)
            ).scalars().all()
        
        # 각 언론사별로 할당할 limit 계산 (최소 1개씩)
        per_source_limit = max(1, limit // len(source_names))
        
        all_news = []
        for source_name in source_names:
            news_items = self.db.execute(
                select(News).where(
                    and_(
                        News.status == NewsStatus.RAW,
                        News.source == source_name,
                        News.published_at >= one_day_ago,
                        News.content_status != ContentStatus.SUCCESS,
                        News.content_retry_count < max_retry
                    )
                ).order_by(desc(News.published_at)).limit(per_source_limit)
            ).scalars().all()
            all_news.extend(news_items)
        
        # published_at 기준으로 정렬 (최신순)
        all_news.sort(key=lambda x: x.published_at, reverse=True)
        
        # 총 limit을 초과하지 않도록 자르기
        return all_news[:limit]

    def list_for_vector_generation(self, limit: int = 100):
        """V2 벡터 생성 대상: RAW 상태이면서 본문은 있지만 벡터가 없는 뉴스들 (최신순)"""
        vectorized_news_ids = (
            select(NewsVector.news_id)
            .distinct()
        )
        
        return self.db.execute(
            select(News).where(
                and_(
                    News.status == NewsStatus.RAW,
                    News.content_status == ContentStatus.SUCCESS,
                    News.id.notin_(vectorized_news_ids)
                )
            ).order_by(desc(News.published_at)).limit(limit)
        ).scalars().all()

    def list_for_economic_classification(self, limit: int = 100):
        """V2 경제관련 분류 대상: RAW 상태이면서 벡터는 있지만 분류가 안된 뉴스들 (최신순)"""
        return self.db.execute(
            select(News).join(NewsVector).where(
                and_(
                    News.status == NewsStatus.RAW,
                    News.filter_score.is_(None),  # 아직 분류 안됨 (confidence 점수 없음)
                    NewsVector.news_id == News.id
                )
            ).order_by(desc(News.published_at)).limit(limit)
        ).scalars().all()

    def list_for_ticker_mapping(self, limit: int = 100):
        """V2 티커 매핑 대상: RAW 상태이면서 경제관련으로 분류되었지만 티커 매핑이 안된 뉴스들 (최신순)"""
        return self.db.execute(
            select(News).join(NewsVector).where(
                and_(
                    News.status == NewsStatus.RAW,
                    NewsVector.news_id == News.id,
                    News.id.notin_(
                        select(NewsTicker.news_id).distinct()
                    )
                )
            ).order_by(desc(News.published_at)).limit(limit)
        ).scalars().all()

    def list_for_summary(self, limit: int = 100):
        """V2 요약 생성 대상: 재평가 완료 & confidence >= 0.8인 티커가 있고 요약이 없는 뉴스들 (최신순)"""
        return self.db.execute(
            select(News).where(
                and_(
                    News.status == NewsStatus.RAW,
                    News.id.in_(
                        select(NewsTicker.news_id).where(NewsTicker.confidence >= 0.8).distinct()
                    ),
                    News.id.notin_(
                        select(NewsSummary.news_id).where(NewsSummary.lang == "ko").distinct()
                    )
                )
            ).order_by(desc(News.published_at)).limit(limit)
        ).scalars().all()

    def list_for_reevaluation(self, limit: int = 100):
        """V2 재평가 대상: 티커 매핑 완료(confidence > 0)되었지만 요약이 없는 뉴스들 (최신순)
        confidence=0은 이미 재평가 완료되어 관련 없다고 판단된 것"""
        return self.db.execute(
            select(News).where(
                and_(
                    News.status == NewsStatus.RAW,
                    News.id.in_(
                        select(NewsTicker.news_id).where(NewsTicker.confidence >= 0).distinct()
                    ),
                    News.id.notin_(
                        select(NewsSummary.news_id).where(NewsSummary.lang == "ko").distinct()
                    )
                )
            ).order_by(desc(News.published_at)).limit(limit)
        ).scalars().all()

    # Updates
    def mark_content_success(self, news: News, content: str):
        news.content = content
        news.content_status = ContentStatus.SUCCESS
        news.updated_at = datetime.now(timezone.utc)
        self.db.commit()

    def mark_content_failed(self, news: News, max_retry: int = 2):
        news.content_retry_count += 1
        if news.content_retry_count >= max_retry:
            news.content_status = ContentStatus.FAILED_FINAL
        news.updated_at = datetime.now(timezone.utc)
        self.db.commit()

    def mark_economic_classification(self, news: News, is_finance_related: bool, 
                                   confidence: float, top_anchor: str = None,
                                   top_similarity: float = None, 
                                   second_similarity: float = None,
                                   similarity_gap: float = None):
        news.is_finance_related = is_finance_related
        news.filter_score = confidence
        news.filter_model = "vector_similarity"
        news.updated_at = datetime.now(timezone.utc)
        self.db.commit()

    def mark_v2_completed(self, news: News):
        """V2 프로세스 완료 시 상태를 ANALYZED로 변경"""
        news.status = NewsStatus.ANALYZED
        news.updated_at = datetime.now(timezone.utc)
        self.db.commit()

    def create_news_vector(self, news_id: int, model_name: str, embedding_vector: list, 
                          text_length: int = None, token_length: int = None,
                          processing_time_ms: int = None) -> NewsVector:
        """뉴스 벡터 생성"""
        news_vector = NewsVector(
            news_id=news_id,
            model_name=model_name,
            vector_dimension=len(embedding_vector),
            embedding_vector=embedding_vector,
            text_length=text_length,
            token_length=token_length,
            processing_time_ms=processing_time_ms
        )
        self.db.add(news_vector)
        self.db.commit()
        self.db.refresh(news_vector)
        return news_vector

    def create_news_ticker(self, news_id: int, ticker_id: int, confidence: float, 
                          method: str = "vector_similarity") -> NewsTicker:
        """뉴스-티커 매핑 생성"""
        news_ticker = NewsTicker(
            news_id=news_id,
            ticker_id=ticker_id,
            confidence=confidence,
            method=method
        )
        self.db.add(news_ticker)
        self.db.commit()
        self.db.refresh(news_ticker)
        return news_ticker

    def create_news_summary(self, news_id: int, summary_text: str, 
                           model: str, title_localized: str = None, 
                           lang: str = "ko") -> NewsSummary:
        """뉴스 요약 생성"""
        news_summary = NewsSummary(
            news_id=news_id,
            lang=lang,
            summary_text=summary_text,
            title_localized=title_localized,
            model=model
        )
        self.db.add(news_summary)
        self.db.commit()
        self.db.refresh(news_summary)
        return news_summary

    def get_news_vector(self, news_id: int, model_name: str) -> NewsVector | None:
        """뉴스 벡터 조회"""
        return self.db.execute(
            select(NewsVector).where(
                and_(
                    NewsVector.news_id == news_id,
                    NewsVector.model_name == model_name
                )
            )
        ).scalar_one_or_none()

    def get_anchor_vectors(self, model_name: str) -> list[NewsAnchorVector]:
        """앵커 벡터들 조회"""
        return self.db.execute(
            select(NewsAnchorVector).where(NewsAnchorVector.model_name == model_name)
        ).scalars().all()

    def find_ticker_id(self, *, symbol: str, exchange: str | None = None, country: str | None = None) -> int | None:
        """티커 ID 찾기 - 기존 로직 재사용"""
        sym = (symbol or "").strip().upper()
        if not sym:
            return None

        # 1) symbol + exchange
        if exchange:
            row = self.db.execute(
                select(Ticker.id).where(and_(Ticker.symbol == sym, Ticker.exchange.ilike(exchange.upper())))
            ).scalar_one_or_none()
            if row:
                return int(row)

        # 2) symbol + country
        if country:
            row = self.db.execute(
                select(Ticker.id).where(and_(Ticker.symbol == sym, Ticker.country.ilike(country.upper())))
            ).scalar_one_or_none()
            if row:
                return int(row)

        # 3) symbol only
        row = self.db.execute(select(Ticker.id).where(Ticker.symbol == sym)).scalar_one_or_none()
        return int(row) if row else None

    def get_held_tickers(self) -> list:
        """보유 티커 목록 조회 (ticker_id + 한글 기업명 + 심볼)"""
        # TODO: 실제 보유 티커 테이블에서 조회하는 로직 구현 필요
        # 현재는 예시로 모든 한국어 티커를 반환
        result = self.db.execute(
            select(Ticker.id, Ticker.symbol, TickerI18n.name)
            .join(TickerI18n, Ticker.id == TickerI18n.ticker_id)
            .where(TickerI18n.lang_code == 'ko')
            .order_by(Ticker.id)
        ).all()
        
        # Row 객체를 딕셔너리로 변환
        return [
            type('TickerInfo', (), {'id': row.id, 'symbol': row.symbol, 'name': row.name})()
            for row in result
        ]

    def get_news_summary(self, news_id: int, lang: str = "ko") -> NewsSummary | None:
        """뉴스 요약 조회"""
        return self.db.execute(
            select(NewsSummary).where(
                and_(
                    NewsSummary.news_id == news_id,
                    NewsSummary.lang == lang
                )
            )
        ).scalar_one_or_none()

