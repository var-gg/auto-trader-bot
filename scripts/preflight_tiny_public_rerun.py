from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import asdict
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy import text

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only

load_dotenv()


def _mask_url(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    if not parsed.scheme:
        return value
    username = parsed.username or ""
    password = "***" if parsed.password else ""
    auth = ""
    if username:
        auth = username
        if password:
            auth += f":{password}"
        auth += "@"
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    path = parsed.path or ""
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{parsed.scheme}://{auth}{host}{port}{path}{query}"


def _url_parts(value: str) -> dict[str, Any]:
    if not value:
        return {"host": None, "port": None, "db": None}
    parsed = urlparse(value)
    db = (parsed.path or "").lstrip("/") or None
    return {"host": parsed.hostname, "port": parsed.port, "db": db}


def _resolve_source() -> dict[str, Any]:
    value = os.getenv("SOURCE_DB_URL", "").strip()
    parts = _url_parts(value)
    return {"raw": value, "masked": _mask_url(value), **parts}


def _resolve_backtest() -> dict[str, Any]:
    cfg = LocalBacktestDbConfig.from_env()
    safe_url = guard_backtest_local_only(cfg.url)
    parts = _url_parts(safe_url)
    return {
        "url": safe_url,
        "masked": _mask_url(safe_url),
        "schema": cfg.schema,
        "search_path": cfg.search_path,
        **parts,
    }


def _git_commit() -> str:
    proc = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def _sector_coverage(session, symbols: list[str]) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT t.symbol,
                   sec.name AS sector_name,
                   ind.name AS industry_name
            FROM trading.bt_mirror_ticker t
            LEFT JOIN trading.bt_mirror_ticker_industry ti ON ti.ticker_id = t.ticker_id
            LEFT JOIN trading.bt_mirror_industry ind ON ind.industry_id = ti.industry_id
            LEFT JOIN trading.bt_mirror_sector sec ON sec.sector_id = ind.sector_id
            WHERE t.symbol = ANY(:symbols)
            ORDER BY t.symbol
            """
        ),
        {"symbols": symbols},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-mode", default="research_similarity_v2")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    spec = ResearchExperimentSpec()

    print(f"git_commit={_git_commit()}")
    print(f"strategy_mode={args.strategy_mode}")

    source = _resolve_source()
    backtest = _resolve_backtest()

    print(f"SOURCE_DB_URL={source['masked']}")
    print(f"BACKTEST_DB_URL={backtest['masked']}")
    if not os.getenv("BACKTEST_DB_URL", "").strip():
        for key in ["BACKTEST_DB_HOST", "BACKTEST_DB_PORT", "BACKTEST_DB_NAME", "BACKTEST_DB_USER", "BACKTEST_DB_PASSWORD"]:
            value = os.getenv(key, "")
            if key.endswith("PASSWORD") and value:
                value = "***MASKED***"
            print(f"{key}={value}")

    print(f"resolved_BACKTEST host={backtest['host']} port={backtest['port']} db={backtest['db']} search_path={backtest['search_path']}")
    print(f"resolved_SOURCE host={source['host']} port={source['port']} db={source['db']}")
    same_target = (source["host"], source["port"], source["db"]) == (backtest["host"], backtest["port"], backtest["db"])
    print(f"resolved_target_matches_source={str(same_target).lower()}")
    if backtest["port"] == 5432:
        print("WARNING: BACKTEST resolved to port 5432")
    if source["port"] == 5433:
        print("WARNING: SOURCE resolved to port 5433")

    session_factory = create_backtest_session_factory()
    with session_factory() as session:
        ohlcv_count = session.execute(text("SELECT COUNT(*) FROM trading.bt_mirror_ohlcv_daily")).scalar_one()
        ticker_count = session.execute(text("SELECT COUNT(*) FROM trading.bt_mirror_ticker")).scalar_one()
        macro_count = session.execute(text("SELECT COUNT(*) FROM trading.macro_data_series")).scalar_one()
        sector_rows = _sector_coverage(session, symbols)

    print(f"bt_mirror_ohlcv_daily_row_count={ohlcv_count}")
    print(f"bt_mirror_ticker_row_count={ticker_count}")
    print(f"macro_series_count={macro_count}")
    print("sector_coverage=" + json.dumps(sector_rows, ensure_ascii=False))

    missing_sector = sorted(set(symbols) - {row.get("symbol") for row in sector_rows if row.get("sector_name")})
    if missing_sector:
        print("PRECHECK_FAIL missing_sector_coverage=" + ",".join(missing_sector))
        return 2
    if not source["raw"]:
        print("PRECHECK_FAIL SOURCE_DB_URL missing")
        return 2
    print(
        "preflight_ok="
        + json.dumps(
            {
                "symbols": symbols,
                "window": {"start_date": args.start_date, "end_date": args.end_date},
                "strategy_mode": args.strategy_mode,
                "spec": asdict(spec),
                "resolved_backtest": {k: backtest[k] for k in ("host", "port", "db", "schema", "search_path")},
                "resolved_source": {k: source[k] for k in ("host", "port", "db")},
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
