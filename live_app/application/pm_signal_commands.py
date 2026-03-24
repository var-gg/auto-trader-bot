from __future__ import annotations

from typing import Optional

from app.features.premarket.models.pm_signal_models import (
    GetPMSignalsResponse,
    TestPMSignalRequest,
    TestPMSignalResponse,
    UpdatePMSignalsRequest,
    UpdatePMSignalsResponse,
)
from app.features.premarket.services.pm_signal_service import PMSignalService
from .adapters import SqlAlchemySessionAdapter
from .context import RunContext


class UpdatePMSignalsCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, request: UpdatePMSignalsRequest, ctx: RunContext) -> UpdatePMSignalsResponse:
        service = PMSignalService(self.db.session)
        return service.update_signals(request)


class GetPMSignalsQuery:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, *, limit: int, min_signal: Optional[float], max_signal: Optional[float], order: str, ctx: RunContext) -> GetPMSignalsResponse:
        service = PMSignalService(self.db.session)
        return service.get_signals(limit=limit, min_signal=min_signal, max_signal=max_signal, order=order)


class TestPMSignalQuery:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, request: TestPMSignalRequest, ctx: RunContext) -> TestPMSignalResponse:
        service = PMSignalService(self.db.session)
        return service.test_pm_signal(request)
