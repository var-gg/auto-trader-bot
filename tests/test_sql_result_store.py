from datetime import datetime

import pytest

from backtest_app.results import store
from shared.domain.models import ExecutionVenue, FillOutcome, FillStatus, OrderPlan, Side


class DummySession:
    def __init__(self, should_fail=False, shared_state=None):
        self.should_fail = should_fail
        self.added = []
        self.committed = False
        self.rolled_back = False
        self.shared_state = {} if shared_state is None else shared_state

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def add(self, obj):
        self.added.append(obj)
        if getattr(obj, "run_key", None):
            obj.id = 123
            self.shared_state["run"] = obj

    def add_all(self, objs):
        self.added.extend(objs)

    def flush(self):
        if self.shared_state.get("run") is not None:
            self.shared_state["run"].id = 123

    def commit(self):
        if self.should_fail:
            raise RuntimeError("insert failed")
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def get(self, model, key):
        return self.shared_state.get("run")


class DummyFactory:
    def __init__(self, sessions):
        self.sessions = sessions
        self.idx = 0

    def __call__(self):
        session = self.sessions[self.idx]
        self.idx += 1
        return session


def _plan():
    return OrderPlan(
        plan_id="plan-1",
        symbol="AAPL",
        ticker_id=1,
        side=Side.BUY,
        generated_at=datetime(2026, 1, 1, 0, 0, 0),
        status="READY",
        rationale="test",
        venue=ExecutionVenue.BACKTEST,
        requested_budget=1000,
        requested_quantity=10,
        legs=[],
        metadata={},
    )


def _fill():
    return FillOutcome(
        plan_id="plan-1",
        leg_id="leg-1",
        symbol="AAPL",
        side=Side.BUY,
        fill_status=FillStatus.FULL,
        venue=ExecutionVenue.BACKTEST,
        event_time=datetime(2026, 1, 1, 0, 1, 0),
        requested_quantity=10,
        filled_quantity=10,
        requested_price=100,
        average_fill_price=100,
        metadata={},
    )


def test_sql_result_store_marks_failed_on_save_error(monkeypatch):
    shared_state = {}
    failing = DummySession(should_fail=True, shared_state=shared_state)
    repair = DummySession(should_fail=False, shared_state=shared_state)
    result_store = store.SqlResultStore.__new__(store.SqlResultStore)
    result_store.db_url = "postgresql://test"
    result_store.engine = object()
    result_store.session_factory = DummyFactory([failing, repair])

    with pytest.raises(RuntimeError):
        result_store.save_run(
            run_key="run-1",
            scenario_id="scn-1",
            strategy_id="pm_open",
            strategy_mode="research_similarity_v1",
            market="US",
            data_source="local-db",
            config_version="v1",
            label_version="lv1",
            vector_version="vv1",
            initial_capital=10000.0,
            params={},
            summary={"total_plans": 1},
            diagnostics={"top_matches": {}},
            plans=[_plan()],
            fills=[_fill()],
            snapshot_info={"dump_id": "d1"},
        )

    assert shared_state["run"].status == "FAILED"
    assert "insert failed" in shared_state["run"].notes


def test_build_metrics_includes_top_k_diagnostics():
    metrics = store._build_metrics(
        run_id=1,
        config_version="v1",
        summary={"total_plans": 1},
        diagnostics={"AAPL": {"top_matches": {"long": [{"prototype_id": "p1"}]}}},
        strategy_mode="research_similarity_v1",
        label_version="lv1",
        vector_version="vv1",
        snapshot_info={"dump_id": "d1"},
    )
    diag = next(m for m in metrics if m.metric_group == "diagnostics")
    assert diag.metric_name == "top_k_prototype_matches"
    assert diag.metric_payload["AAPL"]["top_matches"]["long"][0]["prototype_id"] == "p1"
