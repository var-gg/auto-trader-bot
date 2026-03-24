from __future__ import annotations

from app.features.trading_hybrid.engines import runbooks
from live_app.observability.structured_logging import build_live_run_log
from .adapters import SqlAlchemySessionAdapter
from .context import RunContext


class RunTradingHybridCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    async def run_open(self, *, market: str, test_mode: bool, ctx: RunContext):
        command = f"trading.run_open:{market}"
        result = await (runbooks.run_kr_open(self.db.session, test_mode=test_mode) if market == "KR" else runbooks.run_us_open(self.db.session, test_mode=test_mode))
        summary = result.get("summary", {}) if isinstance(result, dict) else {}
        build_live_run_log(
            run_id=f"{market.lower()}-open-{ctx.invoked_at.strftime('%Y%m%d%H%M%S')}",
            slot=f"{market}_OPEN",
            command=command,
            strategy_version=str(ctx.metadata.get("strategy_version", "unknown")),
            decision_summary={
                "buy_count": summary.get("buy_count", 0),
                "sell_count": summary.get("sell_count", 0),
                "skip_count": summary.get("skip_count", 0),
                "test_mode": test_mode,
            },
            risk_reject_reason=result.get("message") if isinstance(result, dict) and result.get("message") == "시장 휴장" else None,
            extra={"actor": ctx.actor, "channel": ctx.channel},
        )
        return result

    async def run_intraday(self, *, market: str, test_mode: bool, ctx: RunContext):
        command = f"trading.run_intraday:{market}"
        result = await (runbooks.run_kr_intraday(self.db.session, test_mode=test_mode) if market == "KR" else runbooks.run_us_intraday(self.db.session, test_mode=test_mode))
        summary = result.get("summary", {}) if isinstance(result, dict) else {}
        build_live_run_log(
            run_id=f"{market.lower()}-intraday-{ctx.invoked_at.strftime('%Y%m%d%H%M%S')}",
            slot=f"{market}_INTRADAY",
            command=command,
            strategy_version=str(ctx.metadata.get("strategy_version", "unknown")),
            decision_summary={
                "buy_count": summary.get("buy_count", 0),
                "sell_count": summary.get("sell_count", 0),
                "skip_count": summary.get("skip_count", 0),
                "test_mode": test_mode,
            },
            risk_reject_reason=result.get("message") if isinstance(result, dict) and result.get("message") == "시장 휴장" else None,
            extra={"actor": ctx.actor, "channel": ctx.channel},
        )
        return result
