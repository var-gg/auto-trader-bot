import json
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, ResearchExperimentSpec, build_research_manifest
from backtest_app.research_runtime.runner import build_data_snapshot_id
from backtest_app.results.store import JsonResultStore
from shared.domain.models import ExecutionVenue, OrderPlan, Side
from datetime import datetime


def test_research_manifest_is_reproducible_for_same_spec_snapshot_and_commit():
    scenario = BacktestScenario(scenario_id="exp-1", market="US", start_date="2026-01-01", end_date="2026-01-31", symbols=["AAPL"])
    config = BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(feature_window_bars=20, horizon_days=3))
    snapshot_id = build_data_snapshot_id(scenario=scenario, config=config, data_source="local-db", historical_metadata={"source": "local-db"})
    m1 = build_research_manifest(scenario=scenario, config=config, data_snapshot_id=snapshot_id, code_commit="abc123")
    m2 = build_research_manifest(scenario=scenario, config=config, data_snapshot_id=snapshot_id, code_commit="abc123")
    assert m1.manifest_id() == m2.manifest_id()
    assert m1.spec_hash == config.research_spec.spec_hash()


def test_json_result_store_separates_research_namespace(tmp_path):
    plan = OrderPlan(plan_id="p1", symbol="AAPL", ticker_id=1, side=Side.BUY, generated_at=datetime(2026, 1, 1), status="READY", rationale="x", venue=ExecutionVenue.BACKTEST)
    store = JsonResultStore(str(tmp_path), namespace="research")
    path = store.save_run(run_id="manifest-1", plans=[plan], fills=[], summary={"ok": 1}, diagnostics={"d": True}, manifest={"experiment_id": "exp-1"})
    assert "research" in path
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    assert payload["namespace"] == "research"
    assert payload["manifest"]["experiment_id"] == "exp-1"
