from __future__ import annotations

from live_app.application.context import RunContext
from live_app.application.trading_commands import RunTradingHybridCommand


async def run_open_job(db, *, market: str, test_mode: bool = False):
    ctx = RunContext(actor="scheduler", channel="live_app.jobs", metadata={"job": f"{market.lower()}_open"})
    return await RunTradingHybridCommand(db).run_open(market=market, test_mode=test_mode, ctx=ctx)


async def run_intraday_job(db, *, market: str, test_mode: bool = False):
    ctx = RunContext(actor="scheduler", channel="live_app.jobs", metadata={"job": f"{market.lower()}_intraday"})
    return await RunTradingHybridCommand(db).run_intraday(market=market, test_mode=test_mode, ctx=ctx)
