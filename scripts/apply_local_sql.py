from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "DEPRECATED: use scripts/db_apply_sql.py against BACKTEST_DB_URL instead of apply_local_sql.py"
        )
    )
    parser.add_argument("files", nargs="*", help="Legacy positional SQL file list (ignored)")
    args = parser.parse_args()

    if args.files:
        ignored = ", ".join(args.files)
        print(
            "[DEPRECATED] scripts/apply_local_sql.py no longer applies ad-hoc positional SQL files. "
            f"Ignored: {ignored}",
            file=sys.stderr,
        )

    print(
        "[DEPRECATED] Use the single bootstrap path instead:\n"
        "  1) set BACKTEST_DB_URL (or BACKTEST_DB_* envs)\n"
        "  2) python scripts/db_apply_sql.py --db-url \"$BACKTEST_DB_URL\"\n"
        "  3) python scripts/refresh_local_trading.py init-full\n"
        "  4) run backtest_app against local-db\n",
        file=sys.stderr,
    )

    return subprocess.call(
        [sys.executable, "scripts/db_apply_sql.py", "--db-url", "env:BACKTEST_DB_URL"],
        cwd=REPO_ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
