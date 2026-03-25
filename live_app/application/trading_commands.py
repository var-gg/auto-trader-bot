from __future__ import annotations

from app.features.trading_hybrid.engines import runbooks
from live_app.observability.structured_logging import build_live_run_log
from .adapters import SqlAlchemySessionAdapter
from .context import RunContext


def _log_correlation(result):
    correlation = result.get("correlation", {}) if isinstance(result, dict) else {}
    order_batch_ids = correlation.get("order_batch_ids", [])
    order_plan_ids = correlation.get("order_plan_ids", [])
    broker_request_ids = correlation.get("broker_request_ids", [])
    broker_response_ids = correlation.get("broker_response_ids", [])
    return {
        "order_batch_id": ",".join(order_batch_ids) if order_batch_ids else None,
        "order_plan_id": ",".join(order_plan_ids) if order_plan_ids else None,
        "broker_request_id": ",".join(broker_request_ids) if broker_request_ids else None,
        "broker_response_id": ",".join(broker_response_ids) if broker_response_ids else None,
        "correlation": correlation,
    }


class RunTradingHybridCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    async def run_open(self, *, market: str, test_mode: bool, ctx: RunContext):
        command = f"trading.run_open:{market}"
        result = await (runbooks.run_kr_open(self.db.session, test_mode=test_mode) if market == "KR" else runbooks.run_us_open(self.db.session, test_mode=test_mode))
        summary = result.get("summary", {}) if isinstance(result, dict) else {}
        correlation = _log_correlation(result)
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
            order_batch_id=correlation["order_batch_id"],
            order_plan_id=correlation["order_plan_id"],
            broker_request_id=correlation["broker_request_id"],
            broker_response_id=correlation["broker_response_id"],
            extra={"actor": ctx.actor, "channel": ctx.channel, "correlation": correlation["correlation"]},
        )
        return result

    async def run_intraday(self, *, market: str, test_mode: bool, ctx: RunContext):
        command = f"trading.run_intraday:{market}"
        result = await (runbooks.run_kr_intraday(self.db.session, test_mode=test_mode) if market == "KR" else runbooks.run_us_intraday(self.db.session, test_mode=test_mode))
        summary = result.get("summary", {}) if isinstance(result, dict) else {}
        correlation = _log_correlation(result)
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
            order_batch_id=correlation["order_batch_id"],
            order_plan_id=correlation["order_plan_id"],
            broker_request_id=correlation["broker_request_id"],
            broker_response_id=correlation["broker_response_id"],
            extra={"actor": ctx.actor, "channel": ctx.channel, "correlation": correlation["correlation"]},
        )
        return result
