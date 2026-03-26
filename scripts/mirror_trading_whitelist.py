from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "DEPRECATED: use scripts/refresh_local_trading.py init-full instead of mirror_trading_whitelist.py"
        )
    )
    parser.parse_args()

    print(
        "[DEPRECATED] scripts/mirror_trading_whitelist.py has been replaced by the SQL-first local mirror flow:\n"
        "  1) python scripts/db_apply_sql.py --db-url \"$BACKTEST_DB_URL\"\n"
        "  2) python scripts/refresh_local_trading.py init-full\n"
        "  3) python scripts/refresh_local_trading.py refresh-reference|refresh-market\n"
        "Dump-first local trading mirror is the supported strategy.\n",
        file=sys.stderr,
    )

    return subprocess.call(
        [sys.executable, "scripts/refresh_local_trading.py", "init-full"],
        cwd=REPO_ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
