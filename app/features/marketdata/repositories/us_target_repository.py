# app/features/marketdata/repositories/us_target_repository.py
from __future__ import annotations
from datetime import datetime
from typing import List, Dict, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from app.core import config as settings

# 모델들 (스키마/필드명은 프로젝트 기준)
from app.shared.models.ticker import Ticker                   # id, symbol, exchange, country, type
from app.features.news.models.news import News               # published_at (KST 저장 가정)
from app.features.news.models.news_ticker import NewsTicker  # news_id, ticker_id
from app.features.news.models.news_theme import NewsTheme    # news_id, theme_id, confidence
from app.shared.models.ticker_theme import TickerTheme       # ticker_id, theme_id


class USTargetRepository:
    """
    뉴스 기반 대상 티커(심볼+거래소) 조회 리포지토리.
    - 윈도우 내 news_ticker가 있으면 그 티커들 우선
    - 없다면 만점 테마(=confidence >= 임계치)의 theme에 속한 모든 티커
    결과는 (symbol, exchange) 쌍으로 반환(중복 제거).
    """

    def __init__(self, db: Session):
        self.db = db

    def _resolve_news_time_field(self):
        """News의 시간 필드가 published_at 또는 created_at인지 환경에 따라 다를 수 있어 coalesce 사용."""
        # published_at이 Null이면 created_at 사용
        return func.coalesce(News.published_at, News.created_at)

    def find_tickers_from_news_ticker(
        self,
        start_ts: datetime,
        end_ts: datetime,
        limit: int | None = None,
    ) -> List[Tuple[str, str]]:
        time_col = self._resolve_news_time_field()

        q = (
            self.db.query(Ticker.symbol, Ticker.exchange)
            .join(NewsTicker, NewsTicker.ticker_id == Ticker.id)
            .join(News, News.id == NewsTicker.news_id)
            .filter(time_col >= start_ts)
            .filter(time_col < end_ts)
            .distinct()
        )
        if limit and limit > 0:
            q = q.limit(limit)
        return [(s, e) for (s, e) in q.all()]

    def find_tickers_from_fullscore_themes(
        self,
        start_ts: datetime,
        end_ts: datetime,
        theme_full_score: float,
        limit: int | None = None,
    ) -> List[Tuple[str, str]]:
        time_col = self._resolve_news_time_field()

        # 윈도우 내 '만점 테마 뉴스' → 해당 theme에 얽힌 모든 티커
        q = (
            self.db.query(Ticker.symbol, Ticker.exchange)
            .join(TickerTheme, TickerTheme.ticker_id == Ticker.id)
            .join(NewsTheme, NewsTheme.theme_id == TickerTheme.theme_id)
            .join(News, News.id == NewsTheme.news_id)
            .filter(time_col >= start_ts)
            .filter(time_col < end_ts)
            .filter(NewsTheme.confidence >= theme_full_score)
            .distinct()
        )
        if limit and limit > 0:
            q = q.limit(limit)
        return [(s, e) for (s, e) in q.all()]

    def find_targets_from_news(
        self,
        start_ts: datetime,
        end_ts: datetime,
        theme_full_score: float | None = None,
        limit: int | None = None,
    ) -> List[Dict[str, str]]:
        """
        1단계: 뉴스-티커 매핑이 있는 티커들
        2단계: 없으면 '만점 테마'에 속한 모든 티커
        """
        score = theme_full_score if theme_full_score is not None else settings.THEME_FULL_SCORE_DEFAULT

        direct_pairs = self.find_tickers_from_news_ticker(start_ts, end_ts, limit)
        if direct_pairs:
            uniq = {(s, e) for (s, e) in direct_pairs}
            return [{"symbol": s, "exchange": e} for (s, e) in sorted(uniq)]

        theme_pairs = self.find_tickers_from_fullscore_themes(start_ts, end_ts, score, limit)
        uniq = {(s, e) for (s, e) in theme_pairs}
        return [{"symbol": s, "exchange": e} for (s, e) in sorted(uniq)]
