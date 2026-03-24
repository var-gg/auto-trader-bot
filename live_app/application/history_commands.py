from __future__ import annotations

from app.features.premarket.services.pm_history_batch_service import PMHistoryBatchService
from .adapters import SqlAlchemySessionAdapter
from .context import RunContext


class BackfillUnfilledReasonsCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, *, lookback_days: int, limit: int, ctx: RunContext):
        return PMHistoryBatchService(self.db.session).backfill_unfilled_reasons(lookback_days=lookback_days, limit=limit)


class ComputeOutcomesCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, *, lookback_days: int, limit: int, ctx: RunContext):
        return PMHistoryBatchService(self.db.session).compute_tplus_outcomes(lookback_days=lookback_days, limit=limit)


class RunHistoryPostprocessCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, *, backfill_lookback_days: int, backfill_limit: int, outcome_lookback_days: int, outcome_limit: int, ctx: RunContext):
        return PMHistoryBatchService(self.db.session).run_postprocess(
            backfill_lookback_days=backfill_lookback_days,
            backfill_limit=backfill_limit,
            outcome_lookback_days=outcome_lookback_days,
            outcome_limit=outcome_limit,
        )
