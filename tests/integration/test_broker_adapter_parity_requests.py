from __future__ import annotations

import json

from app.features.trading_hybrid.repositories import order_repository


class _FakeDB:
    def __init__(self):
        self.inserts = []
        self.committed = False

    def execute(self, stmt, params=None):
        self.inserts.append({"sql": str(stmt), "params": params})
        class _R:
            def scalar(self):
                return None
        return _R()

    def commit(self):
        self.committed = True


class _CaptureKIS:
    def __init__(self):
        self.calls = []

    def order_cash_buy(self, **kwargs):
        self.calls.append(("order_cash_buy", kwargs))
        return {"rt_cd": "0", "output": {"ODNO": "KR-BUY-1"}}

    def order_cash_sell(self, **kwargs):
        self.calls.append(("order_cash_sell", kwargs))
        return {"rt_cd": "0", "output": {"ODNO": "KR-SELL-1"}}

    def order_stock(self, **kwargs):
        self.calls.append(("order_stock", kwargs))
        return {"rt_cd": "0", "output": {"ODNO": "US-ORD-1"}}


class _RejectKIS:
    def order_stock(self, **kwargs):
        return {"rt_cd": "1", "msg_cd": "OPSQ2001", "msg1": "insufficient quantity", "output": {}}


class _ErrorKIS:
    def order_stock(self, **kwargs):
        raise RuntimeError("temporary gateway failure")


def _stub_ctx(**overrides):
    base = {
        "leg_id": 999,
        "leg_type": "LIMIT",
        "side": "BUY",
        "quantity": 2,
        "limit_price": 107.64,
        "plan_id": 1,
        "ticker_id": 11,
        "plan_action": "BUY",
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "country": "US",
        "broker_order_id": None,
        "broker_order_no": None,
        "broker_status": None,
    }
    base.update(overrides)
    return base


def test_us_limit_order_outbound_shape(monkeypatch):
    db = _FakeDB()
    kis = _CaptureKIS()
    monkeypatch.setattr(order_repository, "_load_leg_context", lambda db_, leg_id: _stub_ctx())
    monkeypatch.setattr(order_repository, "_kis_client_or_none", lambda db_: kis)
    monkeypatch.setattr(order_repository, "_resolve_pm_run_id", lambda db_, ticker_id: None)
    monkeypatch.setattr(order_repository, "_insert_pm_order_execution_history", lambda **kwargs: None)

    order_repository._submit_leg_to_broker(db, 999, test_mode=False)

    name, kwargs = kis.calls[0]
    assert name == "order_stock"
    assert kwargs["order_type"] == "buy"
    assert kwargs["symbol"] == "AAPL"
    assert kwargs["quantity"] == "2"
    assert kwargs["price"] == "107.64"
    assert kwargs["order_method"] == "LIMIT"
    assert kwargs["exchange"] == "NAS"
    assert db.committed is True
    broker_insert = db.inserts[-1]["params"]
    assert broker_insert["status"] == "SUBMITTED"
    assert broker_insert["ord_no"] == "US-ORD-1"


def test_kr_after_hours_sell_outbound_shape(monkeypatch):
    db = _FakeDB()
    kis = _CaptureKIS()
    monkeypatch.setattr(order_repository, "_load_leg_context", lambda db_, leg_id: _stub_ctx(country="KR", exchange="KRX", symbol="005930", side="SELL", quantity=3, limit_price=71200, leg_type="AFTER_HOURS_06"))
    monkeypatch.setattr(order_repository, "_kis_client_or_none", lambda db_: kis)
    monkeypatch.setattr(order_repository, "_resolve_pm_run_id", lambda db_, ticker_id: None)
    monkeypatch.setattr(order_repository, "_insert_pm_order_execution_history", lambda **kwargs: None)

    order_repository._submit_leg_to_broker(db, 999, test_mode=False)

    name, kwargs = kis.calls[0]
    assert name == "order_cash_sell"
    assert kwargs["CANO"]
    assert kwargs["ACNT_PRDT_CD"]
    assert kwargs["PDNO"] == "005930"
    assert kwargs["ORD_DVSN"] == "00"
    assert kwargs["ORD_QTY"] == "3"
    assert kwargs["ORD_UNPR"] == "71200"
    assert kwargs["EXCG_ID_DVSN_CD"] == "NXT"


def test_broker_reject_maps_to_common_reject_fields(monkeypatch):
    db = _FakeDB()
    monkeypatch.setattr(order_repository, "_load_leg_context", lambda db_, leg_id: _stub_ctx())
    monkeypatch.setattr(order_repository, "_kis_client_or_none", lambda db_: _RejectKIS())
    monkeypatch.setattr(order_repository, "_resolve_pm_run_id", lambda db_, ticker_id: None)
    monkeypatch.setattr(order_repository, "_insert_pm_order_execution_history", lambda **kwargs: None)

    order_repository._submit_leg_to_broker(db, 999, test_mode=False)

    broker_insert = db.inserts[-1]["params"]
    assert broker_insert["status"] == "REJECTED"
    assert broker_insert["reject_code"] == "OPSQ2001"
    assert "insufficient quantity" in broker_insert["reject_message"]


def test_broker_exception_maps_to_common_reject_fields(monkeypatch):
    db = _FakeDB()
    monkeypatch.setattr(order_repository, "_load_leg_context", lambda db_, leg_id: _stub_ctx())
    monkeypatch.setattr(order_repository, "_kis_client_or_none", lambda db_: _ErrorKIS())
    monkeypatch.setattr(order_repository, "_resolve_pm_run_id", lambda db_, ticker_id: None)
    monkeypatch.setattr(order_repository, "_insert_pm_order_execution_history", lambda **kwargs: None)

    order_repository._submit_leg_to_broker(db, 999, test_mode=False)

    broker_insert = db.inserts[-1]["params"]
    payload = json.loads(broker_insert["payload"])
    assert broker_insert["status"] == "REJECTED"
    assert broker_insert["reject_code"] == "EXCEPTION"
    assert "temporary gateway failure" in broker_insert["reject_message"]
    assert payload["msg_cd"] == "EXCEPTION"
