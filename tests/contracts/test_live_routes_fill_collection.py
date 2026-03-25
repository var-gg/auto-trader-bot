from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.db import get_db
from app.features.portfolio.controllers import domestic_fill_collection_controller, overseas_fill_collection_controller


class _DummyDomesticFillCollectionService:
    calls = []

    def __init__(self, db):
        self.db = db

    async def collect_domestic_fills(self, days_back):
        type(self).calls.append({"days_back": days_back})
        return {"success": True, "message": "ok", "processed_count": 3, "upserted_count": 2}

    async def get_collection_stats(self):
        return {"period": "7d", "total_fills": 2, "status_counts": {"FULL": 1, "PARTIAL": 1}}


class _DummyOverseasFillCollectionService:
    calls = []

    def __init__(self, db):
        self.db = db

    async def collect_overseas_fills(self, days_back):
        type(self).calls.append({"days_back": days_back})
        return {"success": True, "message": "ok", "processed_count": 4, "upserted_count": 3}

    async def get_collection_stats(self):
        return {"period": "7d", "total_fills": 3, "status_counts": {"FULL": 2, "UNFILLED": 1}}


def _build_app(monkeypatch):
    app = FastAPI()
    monkeypatch.setattr(domestic_fill_collection_controller, "DomesticFillCollectionService", _DummyDomesticFillCollectionService)
    monkeypatch.setattr(overseas_fill_collection_controller, "OverseasFillCollectionService", _DummyOverseasFillCollectionService)
    app.dependency_overrides[get_db] = lambda: object()
    app.include_router(domestic_fill_collection_controller.router)
    app.include_router(overseas_fill_collection_controller.router)
    return app


def test_fill_collection_routes_contract(monkeypatch):
    client = TestClient(_build_app(monkeypatch))

    kr_collect = client.post("/domestic-fill-collection/collect", params={"days_back": 5})
    assert kr_collect.status_code == 200
    assert _DummyDomesticFillCollectionService.calls[-1]["days_back"] == 5

    us_collect = client.post("/overseas-fill-collection/collect", params={"days_back": 6})
    assert us_collect.status_code == 200
    assert _DummyOverseasFillCollectionService.calls[-1]["days_back"] == 6

    kr_stats = client.get("/domestic-fill-collection/stats")
    us_stats = client.get("/overseas-fill-collection/stats")
    assert kr_stats.status_code == 200
    assert us_stats.status_code == 200
