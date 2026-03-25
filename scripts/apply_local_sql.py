from __future__ import annotations

import argparse
from pathlib import Path

from sqlalchemy import create_engine

from backtest_app.db.local_session import LocalBacktestDbConfig, guard_backtest_local_only


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply db/sql/*.sql to local backtest Postgres")
    parser.add_argument("files", nargs="+", help="sql files to apply")
    args = parser.parse_args()

    cfg = LocalBacktestDbConfig.from_env()
    db_url = validate_local_db_url(cfg.url)
    engine = create_engine(db_url, future=True)
    for file_name in args.files:
        sql = Path(file_name).read_text(encoding="utf-8")
        with engine.begin() as conn:
            conn.connection.cursor().execute(sql)
        print(f"applied {file_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
