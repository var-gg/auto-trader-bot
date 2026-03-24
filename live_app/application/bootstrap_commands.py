from __future__ import annotations

from app.features.kis_test.models.kis_test_models import BootstrapRequest, BootstrapResponse
from app.features.kis_test.services.bootstrap_service import BootstrapService
from live_app.observability.structured_logging import build_live_run_log
from .adapters import SqlAlchemySessionAdapter
from .context import RunContext


class RunBootstrapCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    async def execute(self, request: BootstrapRequest, ctx: RunContext) -> BootstrapResponse:
        service = BootstrapService(self.db.session)
        response = await service.run_bootstrap(request)
        build_live_run_log(
            run_id=f"bootstrap-{ctx.invoked_at.strftime('%Y%m%d%H%M%S')}",
            slot=str(ctx.metadata.get("slot", "PREOPEN")),
            command="bootstrap.run",
            strategy_version=str(ctx.metadata.get("strategy_version", "pm-core-v2")),
            decision_summary={
                "overall_success": response.overall_success,
                "successful_steps": response.successful_steps,
                "failed_steps": response.failed_steps,
                "skipped_steps": response.skipped_steps,
            },
            risk_reject_reason=None,
            extra={"actor": ctx.actor, "channel": ctx.channel},
        )
        return response
