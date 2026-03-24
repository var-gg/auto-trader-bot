from __future__ import annotations

from app.features.kis_test.models.kis_test_models import BootstrapRequest
from live_app.application.bootstrap_commands import RunBootstrapCommand
from live_app.application.context import RunContext


async def run_bootstrap_job(db, request: BootstrapRequest):
    ctx = RunContext(actor="scheduler", channel="live_app.jobs", metadata={"job": "bootstrap"})
    return await RunBootstrapCommand(db).execute(request, ctx)
