from __future__ import annotations

from app.features.premarket.services.headline_risk_service import HeadlineRiskService
from .adapters import SqlAlchemySessionAdapter
from .context import RunContext


class RefreshRiskSnapshotCommand:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, *, scope: str, window_minutes: int, ctx: RunContext):
        svc = HeadlineRiskService(self.db.session)
        return svc.refresh_snapshot(scope=scope, window_minutes=window_minutes)


class GetLatestRiskSnapshotQuery:
    def __init__(self, db):
        self.db = SqlAlchemySessionAdapter(db)

    def execute(self, *, scope: str, ctx: RunContext):
        svc = HeadlineRiskService(self.db.session)
        return svc.get_latest_active_snapshot(scope=scope)
