from __future__ import annotations

import argparse
import logging

from app.core.db import SessionLocal
from app.features.premarket.services.pm_history_batch_service import PMHistoryBatchService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute PM T+N outcomes")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        svc = PMHistoryBatchService(db)
        summary = svc.compute_tplus_outcomes(lookback_days=args.lookback_days, limit=args.limit)
        print({
            "scanned": summary.scanned,
            "upserted": summary.upserted,
            "skipped_missing_price": summary.skipped_missing_price,
        })
    finally:
        db.close()


if __name__ == "__main__":
    main()
