import json
from pathlib import Path
from datetime import datetime
import tempfile

from backtest_app.configs.models import BacktestConfig, BacktestScenario, ResearchExperimentSpec, build_research_manifest
from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research.pipeline import fit_train_artifacts, run_test_with_frozen_artifacts
from backtest_app.research_runtime.runner import build_data_snapshot_id
from backtest_app.results.store import JsonResultStore
from shared.domain.models import ExecutionVenue, OrderPlan, Side
from backtest_app.historical_data.models import HistoricalBar


def _bars(symbol: str):
    return [HistoricalBar(symbol=symbol, timestamp=f"2026-01-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100 + i, volume=1000) for i in range(1, 10)]


def test_research_manifest_is_reproducible_for_same_spec_snapshot_and_commit():
    scenario = BacktestScenario(scenario_id="exp-1", market="US", start_date="2026-01-01", end_date="2026-01-31", symbols=["AAPL"])
    config = BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(feature_window_bars=20, horizon_days=3))
    snapshot_id = build_data_snapshot_id(scenario=scenario, config=config, data_source="local-db", historical_metadata={"source": "local-db"})
    m1 = build_research_manifest(scenario=scenario, config=config, data_snapshot_id=snapshot_id, code_commit="abc123")
    m2 = build_research_manifest(scenario=scenario, config=config, data_snapshot_id=snapshot_id, code_commit="abc123")
    assert m1.manifest_id() == m2.manifest_id()


def test_json_result_store_separates_research_namespace(tmp_path):
    plan = OrderPlan(plan_id="p1", symbol="AAPL", ticker_id=1, side=Side.BUY, generated_at=datetime(2026, 1, 1), status="READY", rationale="x", venue=ExecutionVenue.BACKTEST)
    store = JsonResultStore(str(tmp_path), namespace="research")
    path = store.save_run(run_id="manifest-1", plans=[plan], fills=[], summary={"ok": 1}, diagnostics={"d": True}, manifest={"experiment_id": "exp-1"})
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    assert payload["namespace"] == "research"


def test_fold_native_train_artifact_exports_snapshot_metadata():
    store = JsonResearchArtifactStore(tempfile.mkdtemp(prefix="arch-"))
    spec = ResearchExperimentSpec(feature_window_bars=2, horizon_days=2)
    bars = {"AAPL": _bars("AAPL")}
    artifact = fit_train_artifacts(run_id="foldx", artifact_store=store, train_end="2026-01-05", test_start="2026-01-06", purge=1, embargo=1, spec=spec, bars_by_symbol=bars, macro_history_by_date={}, sector_map={}, market="US")
    frozen = run_test_with_frozen_artifacts(train_artifact=artifact, artifact_store=store, decision_dates=["2026-01-06"], spec=spec, bars_by_symbol=bars, macro_history_by_date={}, sector_map={}, market="US")
    assert artifact["spec_hash"] == spec.spec_hash()
    assert artifact["snapshot_ids"]["prototype_snapshot_id"] == frozen["frozen_snapshot_id"]
