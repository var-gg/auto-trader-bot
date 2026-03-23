from __future__ import annotations

from typing import List, Optional, Dict, Tuple
from datetime import datetime, date
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from sqlalchemy.dialects.postgresql import insert

from app.features.fundamentals.models.fundamental_snapshot import FundamentalSnapshot
from app.features.fundamentals.models.dividend_history import DividendHistory
from app.shared.models.ticker import Ticker


class FundamentalRepository:
    def __init__(self, db: Session):
        self.db = db

    # -------- Ticker 조회 --------
    def get_ticker_by_symbol_exchange(self, symbol: str, exchange: str) -> Optional[Ticker]:
        return self.db.query(Ticker).filter(
            and_(Ticker.symbol == symbol, Ticker.exchange == exchange)
        ).first()

    def get_ticker_by_id(self, ticker_id: int) -> Optional[Ticker]:
        return self.db.query(Ticker).filter(Ticker.id == ticker_id).first()

    def get_all_tickers_by_exchanges(self, exchanges: List[str]) -> List[Ticker]:
        return self.db.query(Ticker).filter(Ticker.exchange.in_(exchanges)).all()

    # -------- Snapshot 조회/업서트 --------
    def get_fundamental_snapshot_by_ticker_id(self, ticker_id: int) -> Optional[FundamentalSnapshot]:
        return self.db.query(FundamentalSnapshot).filter(
            FundamentalSnapshot.ticker_id == ticker_id
        ).first()

    def get_snapshot_map_by_ticker_ids(self, ticker_ids: List[int]) -> Dict[int, FundamentalSnapshot]:
        if not ticker_ids:
            return {}
        rows = self.db.query(FundamentalSnapshot).filter(
            FundamentalSnapshot.ticker_id.in_(ticker_ids)
        ).all()
        return {r.ticker_id: r for r in rows}

    def upsert_fundamental_snapshot(
        self,
        ticker_id: int,
        per: Optional[float] = None,
        pbr: Optional[float] = None,
        dividend_yield: Optional[float] = None,
        market_cap: Optional[float] = None,
        debt_ratio: Optional[float] = None
    ) -> FundamentalSnapshot:
        """단건 업서트 (기존 동작 유지: 내부에서 커밋)"""
        snapshot = self.db.query(FundamentalSnapshot).filter(
            FundamentalSnapshot.ticker_id == ticker_id
        ).first()

        if snapshot:
            if per is not None:
                snapshot.per = per
            if pbr is not None:
                snapshot.pbr = pbr
            if dividend_yield is not None:
                snapshot.dividend_yield = dividend_yield
            if market_cap is not None:
                snapshot.market_cap = market_cap
            if debt_ratio is not None:
                snapshot.debt_ratio = debt_ratio
            snapshot.updated_at = datetime.utcnow()
        else:
            snapshot = FundamentalSnapshot(
                ticker_id=ticker_id,
                per=per,
                pbr=pbr,
                dividend_yield=dividend_yield,
                market_cap=market_cap,
                debt_ratio=debt_ratio,
                updated_at=datetime.utcnow()
            )
            self.db.add(snapshot)

        self.db.commit()
        self.db.refresh(snapshot)
        return snapshot

    def bulk_upsert_fundamental_snapshots(self, rows: List[Dict[str, object]]) -> int:
        """
        벌크 업서트: FundamentalSnapshot(ticker_id UNIQUE 가정)
        rows: [{ticker_id, per, pbr, dividend_yield, market_cap, debt_ratio, updated_at}, ...]
        반환값: 실제 삽입/갱신된 행 수(대략)
        """
        if not rows:
            return 0
        table = FundamentalSnapshot.__table__
        stmt = insert(table).values(rows)
        # ticker_id 가 UNIQUE/PK로 걸려 있어야 함
        update_cols = {
            "per": stmt.excluded.per,
            "pbr": stmt.excluded.pbr,
            "dividend_yield": stmt.excluded.dividend_yield,
            "market_cap": stmt.excluded.market_cap,
            "debt_ratio": stmt.excluded.debt_ratio,
            "updated_at": stmt.excluded.updated_at,
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker_id"],
            set_=update_cols
        ).returning(table.c.ticker_id)

        res = self.db.execute(stmt)
        inserted_ids = [r[0] for r in res.fetchall()]
        self.db.commit()
        return len(inserted_ids)

    # -------- Dividend 조회/삽입 --------
    def get_dividend_histories_by_ticker_id(self, ticker_id: int) -> List[DividendHistory]:
        return self.db.query(DividendHistory).filter(
            DividendHistory.ticker_id == ticker_id
        ).order_by(DividendHistory.payment_date.desc()).all()

    def bulk_insert_dividends_map(self, rows_map: Dict[int, List[Dict[str, object]]]) -> Dict[int, int]:
        """
        여러 티커의 배당을 한 번에 삽입 (중복 건 무시)
        rows_map: {ticker_id: [{payment_date, dividend_per_share, dividend_yield?, currency?}, ...]}
        반환: {ticker_id: inserted_count}
        """

        created_at = datetime.now(ZoneInfo("Asia/Seoul"))
        # 평평화
        flat_rows = []
        for tid, items in rows_map.items():
            for r in items:
                flat_rows.append({
                    "ticker_id": tid,
                    "payment_date": r["payment_date"],
                    "dividend_per_share": r["dividend_per_share"],
                    "dividend_yield": r.get("dividend_yield"),
                    "currency": r.get("currency", "USD"),
                    "created_at": created_at
                })
        if not flat_rows:
            return {}

        table = DividendHistory.__table__
        stmt = insert(table).values(flat_rows)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["ticker_id", "payment_date"]
        ).returning(table.c.ticker_id)

        res = self.db.execute(stmt)
        inserted_tids = [r[0] for r in res.fetchall()]
        self.db.commit()

        # per ticker 집계
        count_map: Dict[int, int] = {}
        for tid in inserted_tids:
            count_map[tid] = count_map.get(tid, 0) + 1
        return count_map
