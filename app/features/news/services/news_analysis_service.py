from __future__ import annotations
import logging
from typing import List

from sqlalchemy.orm import Session

from app.features.news.repositories.news_repository import NewsRepository
from app.features.news.models.news_summary import NewsSummary
from app.features.news.models.news_theme import NewsTheme
from app.features.news.models.news_ticker import NewsTicker
from app.features.news.models.news_exchange import NewsExchange

from .news_ai_service import NewsAIService

logger = logging.getLogger("news_analysis_service")

MIN_FILTER_SCORE = 0.30
FULL_SCORE = 1.0
MAX_THEME_EPS = 0.99          # 만점 판정 임계
MAX_THEME_PENALTY = 0.10      # 티커 스코프일 때 '만점 테마'만 -0.1

class NewsAnalysisService:
    def __init__(self, db: Session, repo: NewsRepository):
        self.db, self.repo = db, repo

    # ---------- upserts ----------
    def _upsert_news_theme(self, news_id: int, theme_id: int, confidence: float | None):
        row = (
            self.db.query(NewsTheme)
            .filter(NewsTheme.news_id == news_id, NewsTheme.theme_id == theme_id)
            .first()
        )
        if row:
            if confidence is not None and (row.confidence or 0) < confidence:
                row.confidence = confidence
                self.db.commit()
            return row
        row = NewsTheme(news_id=news_id, theme_id=theme_id, confidence=confidence, method="ai")
        self.db.add(row)
        self.db.commit()
        return row

    def _force_set_theme_score(self, news_id: int, theme_id: int, score: float):
        row = (
            self.db.query(NewsTheme)
            .filter(NewsTheme.news_id == news_id, NewsTheme.theme_id == theme_id)
            .first()
        )
        if row:
            row.confidence = score
            self.db.commit()
            return row
        row = NewsTheme(news_id=news_id, theme_id=theme_id, confidence=score, method="ai")
        self.db.add(row)
        self.db.commit()
        return row

    def _bump_all_themes_to_full(self, news_id: int):
        for tid in range(1, 19):
            self._force_set_theme_score(news_id=news_id, theme_id=tid, score=FULL_SCORE)

    def _decrease_only_maxed_themes(self, news_id: int):
        """만점(>=0.99) 테마만 -0.1 감점"""
        rows = self.db.query(NewsTheme).filter(NewsTheme.news_id == news_id).all()
        affected = 0
        for r in rows:
            if r.confidence is not None and float(r.confidence) >= MAX_THEME_EPS:
                new_score = max(0.0, float(r.confidence) - MAX_THEME_PENALTY)
                if new_score != r.confidence:
                    r.confidence = new_score
                    affected += 1
        if affected:
            self.db.commit()

    def _upsert_news_ticker(self, news_id: int, ticker_id: int, confidence: float | None):
        row = (
            self.db.query(NewsTicker)
            .filter(NewsTicker.news_id == news_id, NewsTicker.ticker_id == ticker_id)
            .first()
        )
        if row:
            if confidence is not None and (row.confidence or 0) < confidence:
                row.confidence = confidence
                self.db.commit()
            return row
        row = NewsTicker(
            news_id=news_id, ticker_id=ticker_id, confidence=confidence, method="ai", candidate_pool_size=None
        )
        self.db.add(row)
        self.db.commit()
        return row

    def _upsert_news_exchange(self, news_id: int, exchange_code: str, confidence: float | None):
        row = (
            self.db.query(NewsExchange)
            .filter(NewsExchange.news_id == news_id, NewsExchange.exchange_code == exchange_code)
            .first()
        )
        if row:
            if confidence is not None and (row.confidence or 0) < confidence:
                row.confidence = confidence
                self.db.commit()
            return row
        row = NewsExchange(news_id=news_id, exchange_code=exchange_code, confidence=confidence, method="ai")
        self.db.add(row)
        self.db.commit()
        return row

    def _upsert_news_summary(self, *, news_id: int, lang: str, title_localized: str, summary_text: str, model: str):
        row = (
            self.db.query(NewsSummary)
            .filter(NewsSummary.news_id == news_id, NewsSummary.lang == lang)
            .first()
        )
        if row:
            row.title_localized = title_localized or row.title_localized
            row.summary_text = summary_text or row.summary_text
            row.model = model or row.model
            self.db.commit()
            return row
        row = NewsSummary(news_id=news_id, lang=lang, title_localized=title_localized, summary_text=summary_text, model=model)
        self.db.add(row)
        self.db.commit()
        return row

    # ---------- run ----------
    def run(self, limit: int = 60, lang: str = "ko", exclude_already_summarized: bool = True) -> dict:
        if exclude_already_summarized:
            items = self.repo.list_for_analysis_missing_summary(limit=limit, lang=lang)
        else:
            items = self.repo.list_for_analysis(limit=limit)

        done = 0
        for news in items:
            if news.filter_score is not None and float(news.filter_score) < MIN_FILTER_SCORE:
                logger.info(f"[Analyze] news_id={news.id} filter_score={news.filter_score} < {MIN_FILTER_SCORE} → skip")
                continue
            if not news.content:
                continue

            # A) 요약/테마/거래소
            title_ko, summary_ko, out_lang, model_sum, themes, exchanges, market_wide_hint = NewsAIService.summarize_and_tag_themes(
                title=news.title, content=news.content, news_id=news.id
            )
            self._upsert_news_summary(
                news_id=news.id, lang=out_lang, title_localized=title_ko, summary_text=summary_ko, model=model_sum
            )

            # 테마 업서트
            for th in themes:
                self._upsert_news_theme(news_id=news.id, theme_id=int(th["theme_id"]), confidence=float(th["confidence"]))

            # 거래소 업서트
            for ex in exchanges:
                self._upsert_news_exchange(news_id=news.id, exchange_code=str(ex["exchange_code"]), confidence=float(ex["confidence"]))

            # B) 스코프/티커
            suggested = NewsAIService.suggest_tickers_or_scope(
                title_ko=title_ko, summary_ko=summary_ko, themes=[{"theme_id": int(t["theme_id"])} for t in themes],
                news_id=news.id, max_tickers=10, db=self.db
            )
            scope = suggested.get("market_scope")
            sector_theme_ids = [int(x) for x in (suggested.get("sector_theme_ids") or []) if 1 <= int(x) <= 18]
            tickers = suggested.get("tickers") or []

            # C) 스코프 처리
            if scope == "ALL":
                self._bump_all_themes_to_full(news_id=news.id)
                logger.info(f"[Analyze] news_id={news.id} scope=ALL → all themes = 1.0; skip ticker mapping")
                self.repo.mark_analyzed(news)
                done += 1
                continue

            if scope == "SECTOR":
                for tid in set(sector_theme_ids):
                    self._force_set_theme_score(news_id=news.id, theme_id=tid, score=FULL_SCORE)
                logger.info(f"[Analyze] news_id={news.id} scope=SECTOR themes={sorted(set(sector_theme_ids))} → skip ticker mapping")
                self.repo.mark_analyzed(news)
                done += 1
                continue

            # scope == "TICKERS"
            mapped = 0
            for t in tickers:
                tid = self.repo.find_ticker_id(symbol=t["symbol"], exchange=t.get("exchange"), country=t.get("country"))
                if tid:
                    self._upsert_news_ticker(news_id=news.id, ticker_id=int(tid), confidence=float(t.get("confidence") or 0))
                    mapped += 1
            # 만점 테마만 감점(-0.1)
            if mapped > 0:
                self._decrease_only_maxed_themes(news_id=news.id)

            logger.info(f"[Analyze] news_id={news.id} scope=TICKERS mapped_tickers={mapped}")
            self.repo.mark_analyzed(news)
            done += 1

        return {"analyzed": done}
