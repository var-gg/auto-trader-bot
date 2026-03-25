from __future__ import annotations

import os
from dataclasses import dataclass

from sqlalchemy import create_engine, text

from backtest_app.db.local import LocalBacktestDbConfig, validate_local_db_url

WHITELIST = {
    "trading.ticker": {
        "target": "trading.bt_mirror_ticker",
        "truncate": "TRUNCATE TABLE trading.bt_mirror_ticker",
        "insert": """
            INSERT INTO trading.bt_mirror_ticker(ticker_id, symbol, exchange, country)
            SELECT id, symbol, exchange, country
            FROM source_rows
        """,
        "source": "SELECT id, symbol, exchange, country FROM trading.ticker",
    },
    "trading.ohlcv_daily": {
        "target": "trading.bt_mirror_ohlcv_daily",
        "truncate": "TRUNCATE TABLE trading.bt_mirror_ohlcv_daily",
        "insert": """
            INSERT INTO trading.bt_mirror_ohlcv_daily(ticker_id, symbol, trade_date, open, high, low, close, volume)
            SELECT o.ticker_id, t.symbol, o.trade_date, o.open, o.high, o.low, o.close, o.volume
            FROM source_rows o
            JOIN trading.bt_mirror_ticker t ON t.ticker_id = o.ticker_id
        """,
        "source": "SELECT ticker_id, trade_date, open, high, low, close, volume FROM trading.ohlcv_daily",
    },
}


def main() -> int:
    source_url = os.getenv("SOURCE_DB_URL", "").strip()
    if not source_url:
        raise SystemExit("SOURCE_DB_URL is required (live/proxy source)")
    local_cfg = LocalBacktestDbConfig.from_env()
    local_url = validate_local_db_url(local_cfg.url)

    source_engine = create_engine(source_url, future=True)
    local_engine = create_engine(local_url, future=True)

    with source_engine.connect() as source_conn, local_engine.begin() as local_conn:
        local_conn.execute(text("SET search_path TO trading, public"))
        # ticker first for FK-ish dependency order
        for source_name in ("trading.ticker", "trading.ohlcv_daily"):
            spec = WHITELIST[source_name]
            rows = [dict(r._mapping) for r in source_conn.execute(text(spec["source"]))]
            local_conn.execute(text(spec["truncate"]))
            if not rows:
                print(f"skipped {source_name}: 0 rows")
                continue
            if source_name == "trading.ticker":
                local_conn.execute(
                    text("""
                    INSERT INTO trading.bt_mirror_ticker(ticker_id, symbol, exchange, country)
                    VALUES (:id, :symbol, :exchange, :country)
                    """),
                    rows,
                )
            elif source_name == "trading.ohlcv_daily":
                local_conn.execute(
                    text("""
                    INSERT INTO trading.bt_mirror_ohlcv_daily(ticker_id, symbol, trade_date, open, high, low, close, volume)
                    SELECT :ticker_id, t.symbol, :trade_date, :open, :high, :low, :close, :volume
                    FROM trading.bt_mirror_ticker t
                    WHERE t.ticker_id = :ticker_id
                    """),
                    rows,
                )
            print(f"mirrored {source_name}: {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
