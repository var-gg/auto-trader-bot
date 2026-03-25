from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.db import get_db
from app.features.kis_test.controllers import bootstrap_controller
from app.features.trading_hybrid.controllers import trading_hybrid_controller
from app.features.kis_test.models.kis_test_models import BootstrapResponse


class _DummyRunBootstrapCommand:
    calls = []

    def __init__(self, db):
        self.db = db

    async def execute(self, request, ctx):
        type(self).calls.append({
            "request": request.model_dump() if hasattr(request, "model_dump") else request.dict(),
            "ctx": {
                "actor": ctx.actor,
                "channel": ctx.channel,
                "metadata": dict(ctx.metadata),
            },
        })
        return BootstrapResponse(
            overall_success=True,
            total_steps=5,
            successful_steps=5,
            failed_steps=0,
            skipped_steps=0,
            total_duration_seconds=1.23,
            started_at="2026-03-25T00:00:00Z",
            completed_at="2026-03-25T00:00:01Z",
            steps=[],
        )


class _DummyRunTradingHybridCommand:
    calls = []

    def __init__(self, db):
        self.db = db

    async def run_open(self, *, market, test_mode, ctx):
        type(self).calls.append({
            "kind": "open",
            "market": market,
            "test_mode": test_mode,
            "ctx": {"actor": ctx.actor, "channel": ctx.channel, "metadata": dict(ctx.metadata)},
        })
        return {
            "buy_plans": [],
            "sell_plans": [],
            "skipped": [],
            "summary": {"buy_count": 0, "sell_count": 0, "skip_count": 0},
            "correlation": {"order_batch_ids": [], "order_plan_ids": [], "broker_request_ids": [], "broker_response_ids": []},
        }

    async def run_intraday(self, *, market, test_mode, ctx):
        type(self).calls.append({
            "kind": "intraday",
            "market": market,
            "test_mode": test_mode,
            "ctx": {"actor": ctx.actor, "channel": ctx.channel, "metadata": dict(ctx.metadata)},
        })
        return {
            "buy_plans": [],
            "sell_plans": [],
            "skipped": [],
            "summary": {"buy_count": 0, "sell_count": 0, "skip_count": 0},
            "correlation": {"order_batch_ids": [], "order_plan_ids": [], "broker_request_ids": [], "broker_response_ids": []},
        }


def _build_app(monkeypatch):
    app = FastAPI()
    monkeypatch.setattr(bootstrap_controller, "RunBootstrapCommand", _DummyRunBootstrapCommand)
    monkeypatch.setattr(trading_hybrid_controller, "RunTradingHybridCommand", _DummyRunTradingHybridCommand)
    app.dependency_overrides[get_db] = lambda: object()
    app.include_router(bootstrap_controller.router)
    app.include_router(trading_hybrid_controller.router)
    return app


def test_bootstrap_get_contract_and_dispatch(monkeypatch):
    _DummyRunBootstrapCommand.calls.clear()
    client = TestClient(_build_app(monkeypatch))

    response = client.get("/kis-test/bootstrap", params={"skip_token_refresh": "true", "fred_lookback_days": 14})

    assert response.status_code == 200
    payload = response.json()
    assert payload["overall_success"] is True
    assert len(_DummyRunBootstrapCommand.calls) == 1
    call = _DummyRunBootstrapCommand.calls[0]
    assert call["request"]["skip_token_refresh"] is True
    assert call["request"]["fred_lookback_days"] == 14
    assert call["ctx"]["metadata"]["route"] == "/kis-test/bootstrap"
    assert call["ctx"]["metadata"]["slot"] == "US_PREOPEN"


def test_trading_hybrid_routes_contract_and_dispatch(monkeypatch):
    _DummyRunTradingHybridCommand.calls.clear()
    client = TestClient(_build_app(monkeypatch))

    for path in [
        "/api/trading-hybrid/kr/open",
        "/api/trading-hybrid/us/open",
        "/api/trading-hybrid/kr/intraday",
        "/api/trading-hybrid/us/intraday",
    ]:
        response = client.post(path, params={"test_mode": "true"})
        assert response.status_code == 200, path

    assert [c["kind"] for c in _DummyRunTradingHybridCommand.calls] == ["open", "open", "intraday", "intraday"]
    assert [c["market"] for c in _DummyRunTradingHybridCommand.calls] == ["KR", "US", "KR", "US"]
    assert all(c["test_mode"] is True for c in _DummyRunTradingHybridCommand.calls)
    assert _DummyRunTradingHybridCommand.calls[0]["ctx"]["metadata"]["route"] == "/api/trading-hybrid/kr/open"
    assert _DummyRunTradingHybridCommand.calls[0]["ctx"]["metadata"]["slot"] == "KR_OPEN"
    assert _DummyRunTradingHybridCommand.calls[-1]["ctx"]["metadata"]["route"] == "/api/trading-hybrid/us/intraday"
    assert _DummyRunTradingHybridCommand.calls[-1]["ctx"]["metadata"]["slot"] == "US_INTRADAY"
