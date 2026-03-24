from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from app.features.kis_test.models.kis_test_models import BootstrapRequest
from app.features.premarket.models.pm_signal_models import UpdatePMSignalsRequest
from .bootstrap_commands import RunBootstrapCommand
from .context import RunContext
from .history_commands import BackfillUnfilledReasonsCommand, ComputeOutcomesCommand, RunHistoryPostprocessCommand
from .pm_signal_commands import UpdatePMSignalsCommand
from .risk_commands import RefreshRiskSnapshotCommand
from .trading_commands import RunTradingHybridCommand


@dataclass(frozen=True)
class SlotDispatchRequest:
    slot: str
    test_mode: bool = False
    params: Dict[str, Any] | None = None


class ScheduleSlotDispatcher:
    def __init__(self, db):
        self.db = db

    async def execute(self, request: SlotDispatchRequest, ctx: RunContext):
        params = dict(request.params or {})
        slot = request.slot.upper()

        if slot == "KR_PREOPEN":
            bootstrap = await RunBootstrapCommand(self.db).execute(
                BootstrapRequest(
                    skip_token_refresh=bool(params.get("skip_token_refresh", False)),
                    skip_fred_ingest=bool(params.get("skip_fred_ingest", False)),
                    skip_yahoo_ingest=bool(params.get("skip_yahoo_ingest", False)),
                    skip_risk_refresh=False,
                    skip_signal_update=False,
                    token_threshold_hours=int(params.get("token_threshold_hours", 24)),
                    fred_lookback_days=int(params.get("fred_lookback_days", 30)),
                    yahoo_period=str(params.get("yahoo_period", "1mo")),
                ),
                ctx,
            )
            return {"slot": slot, "bootstrap": bootstrap.model_dump() if hasattr(bootstrap, 'model_dump') else bootstrap.dict()}

        if slot == "US_PREOPEN":
            bootstrap = await RunBootstrapCommand(self.db).execute(
                BootstrapRequest(
                    skip_token_refresh=bool(params.get("skip_token_refresh", False)),
                    skip_fred_ingest=bool(params.get("skip_fred_ingest", False)),
                    skip_yahoo_ingest=bool(params.get("skip_yahoo_ingest", False)),
                    skip_risk_refresh=False,
                    skip_signal_update=False,
                    token_threshold_hours=int(params.get("token_threshold_hours", 24)),
                    fred_lookback_days=int(params.get("fred_lookback_days", 30)),
                    yahoo_period=str(params.get("yahoo_period", "1mo")),
                ),
                ctx,
            )
            return {"slot": slot, "bootstrap": bootstrap.model_dump() if hasattr(bootstrap, 'model_dump') else bootstrap.dict()}

        if slot == "KR_OPEN":
            result = await RunTradingHybridCommand(self.db).run_open(market="KR", test_mode=request.test_mode, ctx=ctx)
            return {"slot": slot, "result": result}

        if slot == "US_OPEN":
            result = await RunTradingHybridCommand(self.db).run_open(market="US", test_mode=request.test_mode, ctx=ctx)
            return {"slot": slot, "result": result}

        if slot == "KR_INTRADAY":
            result = await RunTradingHybridCommand(self.db).run_intraday(market="KR", test_mode=request.test_mode, ctx=ctx)
            return {"slot": slot, "result": result}

        if slot == "US_INTRADAY":
            result = await RunTradingHybridCommand(self.db).run_intraday(market="US", test_mode=request.test_mode, ctx=ctx)
            return {"slot": slot, "result": result}

        if slot == "HOUSEKEEPING":
            backfill = BackfillUnfilledReasonsCommand(self.db).execute(
                lookback_days=int(params.get("backfill_lookback_days", 7)),
                limit=int(params.get("backfill_limit", 2000)),
                ctx=ctx,
            )
            outcomes = ComputeOutcomesCommand(self.db).execute(
                lookback_days=int(params.get("outcome_lookback_days", 14)),
                limit=int(params.get("outcome_limit", 5000)),
                ctx=ctx,
            )
            return {
                "slot": slot,
                "backfill": backfill.__dict__,
                "outcomes": outcomes.__dict__,
            }

        raise ValueError(f"Unsupported slot: {request.slot}")
