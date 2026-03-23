# app/features/news/repositories/kis_news_repository.py

from sqlalchemy.orm import Session
from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert
from app.features.news.models.kis_news import KisNews
from app.shared.models.ticker import Ticker
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class KisNewsRepository:
    def __init__(self, db: Session):
        self.db = db

    def upsert_kis_news(self, data: Dict[str, Any]) -> KisNews:
        """
        KIS 뉴스를 UPSERT (source_type, source_key 기준 중복 방지)
        """
        stmt = insert(KisNews).values(**data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source_type", "source_key"],
            set_={
                "ticker_id": stmt.excluded.ticker_id,
                "title": stmt.excluded.title,
                "published_at": stmt.excluded.published_at,
                "publisher": stmt.excluded.publisher,
                "class_cd": stmt.excluded.class_cd,
                "class_name": stmt.excluded.class_name,
                "nation_cd": stmt.excluded.nation_cd,
                "exchange_cd": stmt.excluded.exchange_cd,
                "symbol": stmt.excluded.symbol,
                "symbol_name": stmt.excluded.symbol_name,
                "kr_iscd": stmt.excluded.kr_iscd,
                "lang": stmt.excluded.lang,
                "raw_json": stmt.excluded.raw_json,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        result = self.db.execute(stmt)
        self.db.commit()
        
        # 삽입/업데이트된 레코드 조회
        row = self.db.execute(
            select(KisNews).where(
                and_(
                    KisNews.source_type == data["source_type"],
                    KisNews.source_key == data["source_key"],
                )
            )
        ).scalar_one()
        return row

    def find_ticker_by_symbol(self, symbol: str, exchange: Optional[str] = None) -> Optional[int]:
        """
        심볼로 ticker_id 조회 (해외 종목)
        거래소는 무시하고 심볼만으로 조회
        """
        sym = (symbol or "").strip().upper()
        if not sym:
            return None

        row = self.db.execute(
            select(Ticker.id).where(Ticker.symbol == sym)
        ).scalar_one_or_none()
        
        return int(row) if row else None

    def find_ticker_by_kr_code(self, kr_code: str) -> Optional[int]:
        """
        한국 종목코드(6자리)로 ticker_id 조회
        kr_code는 보통 symbol 또는 별도 필드에 저장됨
        예: "005930" → ticker.symbol = "005930.KQ" 또는 "005930.KS"
        """
        code = (kr_code or "").strip()
        if not code:
            return None

        # 1) symbol이 "코드.거래소" 형태로 시작하는 경우 찾기 (KR 국가코드)
        row = self.db.execute(
            select(Ticker.id).where(
                and_(
                    Ticker.symbol.like(f"{code}.%"),
                    Ticker.country == "KR"
                )
            )
        ).first()
        
        if row:
            return int(row[0])
        
        # 2) 정확히 일치하는 symbol 찾기 (거래소 없이 저장된 경우)
        row = self.db.execute(
            select(Ticker.id).where(
                and_(
                    Ticker.symbol == code,
                    Ticker.country == "KR"
                )
            )
        ).first()
        
        return int(row[0]) if row else None

    def get_by_id(self, news_id: int) -> Optional[KisNews]:
        """ID로 뉴스 조회"""
        return self.db.execute(
            select(KisNews).where(KisNews.id == news_id)
        ).scalar_one_or_none()

    def list_recent(self, limit: int = 100) -> list[KisNews]:
        """최근 뉴스 목록 조회"""
        return list(
            self.db.execute(
                select(KisNews)
                .order_by(KisNews.published_at.desc())
                .limit(limit)
            ).scalars().all()
        )

    def list_by_ticker(self, ticker_id: int, limit: int = 50) -> list[KisNews]:
        """특정 티커의 뉴스 목록 조회"""
        return list(
            self.db.execute(
                select(KisNews)
                .where(KisNews.ticker_id == ticker_id)
                .order_by(KisNews.published_at.desc())
                .limit(limit)
            ).scalars().all()
        )

