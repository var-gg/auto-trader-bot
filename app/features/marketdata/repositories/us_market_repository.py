# app/features/marketdata/repositories/us_market_repository.py
from __future__ import annotations
from typing import Iterable, List, Dict, Any
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from app.features.marketdata.models.ohlcv_daily import OhlcvDaily

class USMarketRepository:
    def __init__(self, db: Session):
        self.db = db

    def upsert_daily_rows(self, rows: List[Dict[str, Any]]) -> int:
        """
        (ticker_id, trade_date) 기준으로 Postgres UPSERT
        - 같은 날짜가 오면 값을 갱신(수정주가/미마감→마감 반영)
        - rows: [{ticker_id, trade_date, open, high, low, close, volume, is_final, source, ...}, ...]
        반환: upsert 시도 건수(len(rows))  (실제 영향 row 수와 1:1은 아님)
        """
        if not rows:
            return 0

        insert_stmt = insert(OhlcvDaily).values(rows)

        update_cols = {
            # 시세 값 갱신
            "open": insert_stmt.excluded.open,
            "high": insert_stmt.excluded.high,
            "low": insert_stmt.excluded.low,
            "close": insert_stmt.excluded.close,
            "volume": insert_stmt.excluded.volume,
            "is_final": insert_stmt.excluded.is_final,
            # 소스 메타도 최신으로
            "source": insert_stmt.excluded.source,
            "source_symbol": insert_stmt.excluded.source_symbol,
            "source_exchange": insert_stmt.excluded.source_exchange,
            "source_payload": insert_stmt.excluded.source_payload,
        }

        # 고유 제약조건: (ticker_id, trade_date)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["ticker_id", "trade_date"],
            set_=update_cols,
        )
        self.db.execute(upsert_stmt)
        self.db.commit()
        return len(rows)

    def delete_current_price_data(self, ticker_id: int, trade_date: date) -> int:
        """
        특정 티커의 특정 날짜 현재가 데이터 삭제
        - 현재가 수집 시 기존 데이터를 삭제한 후 새로 삽입하기 위함
        - 반환: 삭제된 행 수
        """
        deleted_count = (
            self.db.query(OhlcvDaily)
            .filter(OhlcvDaily.ticker_id == ticker_id)
            .filter(OhlcvDaily.trade_date == trade_date)
            .delete()
        )
        self.db.commit()
        return deleted_count
