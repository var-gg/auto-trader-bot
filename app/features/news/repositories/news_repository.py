from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import desc, select, and_
from datetime import datetime, timezone

from app.features.news.models.news import News, NewsStatus, ContentStatus
from app.features.news.models.news_summary import NewsSummary
from app.features.news.models.news_theme import NewsTheme
from app.features.news.models.news_ticker import NewsTicker

from app.shared.models.ticker import Ticker
from app.shared.models.ticker_i18n import TickerI18n
from app.shared.utils.exchange_normalizer import normalize_exchange_to_yf

class NewsRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_link(self, link: str) -> News | None:
        return self.db.execute(select(News).where(News.link == link)).scalar_one_or_none()

    def create_if_not_exists(self, *, title: str, link: str, published_at, published_date_kst: str | None, source: str) -> News:
        ex = self.get_by_link(link)
        if ex:
            return ex
        row = News(title=title, link=link, published_at=published_at, published_date_kst=published_date_kst, source=source)
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    # Queues
    def list_for_filtering(self, limit: int = 150):
        return self.db.execute(
            select(News).where(News.status == NewsStatus.RAW).limit(limit)
        ).scalars().all()

    def list_for_content_fetch(self, limit: int = 200, max_retry: int = 2):
        return self.db.execute(
            select(News).where(
                News.status == NewsStatus.FILTERED_POSITIVE,
                News.content_status != ContentStatus.SUCCESS,
                News.content_retry_count < max_retry
            ).limit(limit)
        ).scalars().all()

    def list_for_analysis(self, limit: int = 60):
        return self.db.execute(
            select(News).where(
                News.status == NewsStatus.FILTERED_POSITIVE,
                News.content_status == ContentStatus.SUCCESS
            )
            .order_by(desc(News.id))   # ✅ 최신순
            .limit(limit)
        ).scalars().all()

    # 이미 특정 언어 요약된 뉴스 제외
    def list_for_analysis_missing_summary(self, limit: int = 60, lang: str = "ko"):
        subq_exists_summary = (
            select(NewsSummary.id)
            .where(and_(NewsSummary.news_id == News.id, NewsSummary.lang == lang))
            .exists()
        )
        return self.db.execute(
            select(News).where(
                News.status == NewsStatus.FILTERED_POSITIVE,
                News.content_status == ContentStatus.SUCCESS,
                ~subq_exists_summary
            )
            .order_by(desc(News.id))   # ✅ 최신순
            .limit(limit)
        ).scalars().all()

    # Updates
    def mark_filtered(self, news: News, is_related: bool, score: float | None, model: str | None):
        news.filter_attempts += 1
        news.filter_score = score
        news.filter_model = model
        news.is_finance_related = is_related
        news.status = NewsStatus.FILTERED_POSITIVE if is_related else NewsStatus.FILTERED_NEGATIVE
        self.db.commit()

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

    def mark_analyzed(self, news: News):
        news.status = NewsStatus.ANALYZED
        self.db.commit()

    # 심볼→티커 ID 매핑(유연)
    def find_ticker_id(self, *, symbol: str, exchange: str | None, country: str | None) -> int | None:
        """
        우선순위:
          1) symbol + exchange 완전일치
          2) symbol + country 힌트(해당 국가 대표 거래소 우선)
          3) symbol 단일(유니크한 경우)
        """
        symbol = symbol.upper().strip()
        q = self.db.query(Ticker).filter(Ticker.symbol == symbol)

        if exchange:
            qx = q.filter(Ticker.exchange == exchange.upper())
            row = qx.first()
            if row:
                return int(row.id)

        if country:
            qc = q.filter(Ticker.country == country.upper())
            row = qc.first()
            if row:
                return int(row.id)

        # 마지막 시도: 심볼 유니크 가정
        row = q.first()
        return int(row.id) if row else None

    def find_ticker_id(self, *, symbol: str, exchange: str | None = None, country: str | None = None) -> int | None:
        """
        - GPT가 준 exchange는 '야후코드'라고 가정(NMS/NYQ/…)
        - DB의 exchange가 야후코드든 표준명이든, 우선 야후코드 기준으로 1차 조회
        - country가 있으면 보조 조건으로 활용
        """
        sym = (symbol or "").strip().upper()
        if not sym:
            return None

        ex_yf = normalize_exchange_to_yf(exchange)  # None 허용
        ctry = (country or "").strip().upper() or None

        # 1) symbol + exchange(야후코드)
        if ex_yf:
            row = self.db.execute(
                select(Ticker.id).where(and_(Ticker.symbol == sym, Ticker.exchange.ilike(ex_yf)))
            ).scalar_one_or_none()
            if row:
                return int(row)

        # 2) symbol + country
        if ctry:
            row = self.db.execute(
                select(Ticker.id).where(and_(Ticker.symbol == sym, Ticker.country.ilike(ctry)))
            ).scalar_one_or_none()
            if row:
                return int(row)

        # 3) symbol only (최후)
        row = self.db.execute(select(Ticker.id).where(Ticker.symbol == sym)).scalar_one_or_none()
        return int(row) if row else None

    # 테스트 API용 추가 메서드들
    def get_by_id(self, news_id: int) -> News | None:
        """뉴스 ID로 뉴스 조회"""
        return self.db.execute(select(News).where(News.id == news_id)).scalar_one_or_none()

    def get_summary_by_news_id_and_lang(self, news_id: int, lang: str) -> NewsSummary | None:
        """뉴스 ID와 언어로 요약 조회"""
        return self.db.execute(
            select(NewsSummary).where(
                and_(NewsSummary.news_id == news_id, NewsSummary.lang == lang)
            )
        ).scalar_one_or_none()

    def get_themes_by_news_id(self, news_id: int) -> list[NewsTheme]:
        """뉴스 ID로 테마 목록 조회"""
        return self.db.execute(
            select(NewsTheme).where(NewsTheme.news_id == news_id)
        ).scalars().all()