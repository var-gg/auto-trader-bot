import os

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.db import get_db
from app.features.premarket.controllers import pm_signal_controller, pm_risk_controller, pm_history_batch_controller
from app.features.premarket.models.pm_signal_models import (
    GetPMSignalsResponse,
    PMSignalItem,
    SignalSample,
    TestPMSignalResponse,
    UpdatePMSignalsResponse,
)


class _DummyUpdatePMSignalsCommand:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, request, ctx):
        type(self).calls.append({"request": request.model_dump(), "ctx": dict(ctx.metadata)})
        return UpdatePMSignalsResponse(
            success=True,
            config_id=4,
            anchor_date=request.anchor_date or "2026-03-25",
            results={"total": 1, "success": 1, "failed": 0, "no_signal": 0},
            elapsed_seconds=0.1,
            samples=[SignalSample(ticker_id=1, symbol="SYM", company_name="Demo", best_target_id=11, signal_1d=0.8, reason="CONFIDENT")],
            errors=None,
        )


class _DummyGetPMSignalsQuery:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, *, limit, min_signal, max_signal, order, ctx):
        type(self).calls.append({"limit": limit, "min_signal": min_signal, "max_signal": max_signal, "order": order, "ctx": dict(ctx.metadata)})
        return GetPMSignalsResponse(success=True, count=1, signals=[PMSignalItem(ticker_id=1, symbol="SYM", company_name="Demo", signal_1d=0.9, best_target_id=11, updated_at="2026-03-25T00:00:00Z")])


class _DummyTestPMSignalQuery:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, request, ctx):
        type(self).calls.append({"request": request.model_dump(), "ctx": dict(ctx.metadata)})
        return TestPMSignalResponse(
            success=True,
            ticker_id=request.ticker_id,
            symbol="SYM",
            company_name="Demo",
            country="US",
            config_id=4,
            anchor_date=request.anchor_date or "2026-03-25",
            signal_1d=0.7,
            p_up=0.8,
            p_down=0.2,
            best_direction="UP",
            best_target={"id": 1},
            up_matches=[],
            down_matches=[],
            up_reranked_top10=[],
            down_reranked_top10=[],
            stats={"count": 0},
            error=None,
        )


class _DummyRefreshRiskSnapshotCommand:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, *, scope, window_minutes, ctx):
        type(self).calls.append({"scope": scope, "window_minutes": window_minutes, "ctx": dict(ctx.metadata)})
        return {"scope": scope, "window_minutes": window_minutes, "rows": 1}


class _DummyGetLatestRiskSnapshotQuery:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, *, scope, ctx):
        type(self).calls.append({"scope": scope, "ctx": dict(ctx.metadata)})
        return {"scope": scope, "captured_at": "2026-03-25T00:00:00Z"}


class _Summary:
    def __init__(self, scanned=0, updated=0, unresolved=0, upserted=0, skipped_missing_price=0):
        self.scanned = scanned
        self.updated = updated
        self.unresolved = unresolved
        self.upserted = upserted
        self.skipped_missing_price = skipped_missing_price


class _HistoryBundle:
    def __init__(self):
        self.unfilled = _Summary(scanned=5, updated=3, unresolved=2)
        self.outcomes = _Summary(scanned=7, upserted=4, skipped_missing_price=3)


class _DummyBackfillUnfilledReasonsCommand:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, *, lookback_days, limit, ctx):
        type(self).calls.append({"lookback_days": lookback_days, "limit": limit, "ctx": dict(ctx.metadata)})
        return _Summary(scanned=5, updated=3, unresolved=2)


class _DummyComputeOutcomesCommand:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, *, lookback_days, limit, ctx):
        type(self).calls.append({"lookback_days": lookback_days, "limit": limit, "ctx": dict(ctx.metadata)})
        return _Summary(scanned=7, upserted=4, skipped_missing_price=3)


class _DummyRunHistoryPostprocessCommand:
    calls = []

    def __init__(self, db):
        self.db = db

    def execute(self, *, backfill_lookback_days, backfill_limit, outcome_lookback_days, outcome_limit, ctx):
        type(self).calls.append({
            "backfill_lookback_days": backfill_lookback_days,
            "backfill_limit": backfill_limit,
            "outcome_lookback_days": outcome_lookback_days,
            "outcome_limit": outcome_limit,
            "ctx": dict(ctx.metadata),
        })
        return _HistoryBundle()


def _build_app(monkeypatch):
    app = FastAPI()
    monkeypatch.setattr(pm_signal_controller, "UpdatePMSignalsCommand", _DummyUpdatePMSignalsCommand)
    monkeypatch.setattr(pm_signal_controller, "GetPMSignalsQuery", _DummyGetPMSignalsQuery)
    monkeypatch.setattr(pm_signal_controller, "TestPMSignalQuery", _DummyTestPMSignalQuery)
    monkeypatch.setattr(pm_risk_controller, "RefreshRiskSnapshotCommand", _DummyRefreshRiskSnapshotCommand)
    monkeypatch.setattr(pm_risk_controller, "GetLatestRiskSnapshotQuery", _DummyGetLatestRiskSnapshotQuery)
    monkeypatch.setattr(pm_history_batch_controller, "BackfillUnfilledReasonsCommand", _DummyBackfillUnfilledReasonsCommand)
    monkeypatch.setattr(pm_history_batch_controller, "ComputeOutcomesCommand", _DummyComputeOutcomesCommand)
    monkeypatch.setattr(pm_history_batch_controller, "RunHistoryPostprocessCommand", _DummyRunHistoryPostprocessCommand)
    app.dependency_overrides[get_db] = lambda: object()
    app.include_router(pm_signal_controller.router)
    app.include_router(pm_risk_controller.router)
    app.include_router(pm_history_batch_controller.router)
    return app


def test_pm_signal_routes_accept_asis_contract(monkeypatch):
    client = TestClient(_build_app(monkeypatch))

    post_resp = client.post("/api/premarket/signals/update", json={"country": "US", "dry_run": True, "anchor_date": "2026-03-25"})
    assert post_resp.status_code == 200
    assert _DummyUpdatePMSignalsCommand.calls[-1]["request"]["country"] == "US"

    get_resp = client.get("/api/premarket/signals/update", params={"tickers": "NVDA,AAPL", "dry_run": "true"})
    assert get_resp.status_code == 200
    assert _DummyUpdatePMSignalsCommand.calls[-1]["request"]["tickers"] == ["NVDA", "AAPL"]

    list_resp = client.get("/api/premarket/signals", params={"limit": 25, "order": "updated_desc"})
    assert list_resp.status_code == 200
    assert _DummyGetPMSignalsQuery.calls[-1]["limit"] == 25

    test_resp = client.get("/api/premarket/signals/test", params={"ticker_id": 348, "use_ann": "false"})
    assert test_resp.status_code == 200
    assert _DummyTestPMSignalQuery.calls[-1]["request"]["ticker_id"] == 348


def test_pm_risk_and_history_routes_require_auth_and_dispatch(monkeypatch):
    os.environ["INTERNAL_API_TOKEN"] = "secret-token"
    client = TestClient(_build_app(monkeypatch))

    unauthorized = client.post("/api/premarket/risk/refresh")
    assert unauthorized.status_code == 401

    headers = {"X-Scheduler-Token": "secret-token"}
    refresh = client.post("/api/premarket/risk/refresh", params={"scope": "US", "window_minutes": 180}, headers=headers)
    assert refresh.status_code == 200
    assert _DummyRefreshRiskSnapshotCommand.calls[-1]["scope"] == "US"
    assert _DummyRefreshRiskSnapshotCommand.calls[-1]["ctx"]["route"] == "/api/premarket/risk/refresh"

    latest = client.get("/api/premarket/risk/latest", params={"scope": "GLOBAL"}, headers=headers)
    assert latest.status_code == 200

    backfill = client.post("/api/premarket/history/backfill-unfilled-reasons", json={"lookback_days": 9, "limit": 333}, headers=headers)
    assert backfill.status_code == 200
    assert _DummyBackfillUnfilledReasonsCommand.calls[-1]["lookback_days"] == 9
    assert _DummyBackfillUnfilledReasonsCommand.calls[-1]["ctx"]["command"] == "history.backfill_unfilled"

    outcomes = client.post("/api/premarket/history/compute-outcomes", json={"lookback_days": 21, "limit": 444}, headers=headers)
    assert outcomes.status_code == 200
    assert _DummyComputeOutcomesCommand.calls[-1]["lookback_days"] == 21

    postprocess = client.post("/api/premarket/history/postprocess", json={"backfill_lookback_days": 8, "outcome_lookback_days": 16}, headers=headers)
    assert postprocess.status_code == 200
    assert _DummyRunHistoryPostprocessCommand.calls[-1]["ctx"]["command"] == "history.postprocess"
