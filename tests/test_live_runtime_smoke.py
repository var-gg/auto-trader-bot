import json
from pathlib import Path

from backtest_app.historical_data.models import HistoricalBar
from live_runtime.adapters import LiveRuntimeAdapters
from live_runtime.runner import run_live_runtime


class FakeOrderAdapter:
    def __init__(self):
        self.calls = []
    def place_orders(self, plans):
        self.calls.append(list(plans))
        return [{"plan_id": p.plan_id, "status": "staged"} for p in plans]


class FakeStateAdapter:
    def get_positions(self):
        return []
    def get_cash(self):
        return 10000.0
    def get_bars(self, *, symbols, end_date, lookback_bars):
        return {s: [HistoricalBar(symbol=s, timestamp=f"2026-03-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100 + i, volume=1000) for i in range(1, 70)] for s in symbols}
    def get_macro(self, *, day):
        return {"growth": 0.2}
    def get_sector_map(self, *, symbols):
        return {s: "TECH" for s in symbols}


class FakeBrokerAdapter:
    def __init__(self):
        self.submitted = []
        self.cancelled = []
    def submit(self, plan):
        self.submitted.append(plan.plan_id)
        return {"order_id": f"ord-{plan.plan_id}", "status": "submitted"}
    def cancel(self, order_id):
        self.cancelled.append(order_id)
        return {"order_id": order_id, "status": "cancelled"}
    def collect_fills(self, plans):
        return []


class FakeCalendarAdapter:
    def is_open(self, market, day):
        return True
    def next_session(self, market, day):
        return day


def _write_bundle(tmp_path: Path):
    manifest = {"experiment_id": "exp1", "research_spec": {"feature_window_bars": 5, "horizon_days": 3, "target_return_pct": 0.04, "stop_return_pct": 0.03, "flat_return_band_pct": 0.005, "feature_version": "multiscale_v2", "label_version": "event_outcome_v1", "memory_version": "memory_asof_v1"}, "top_n": 2, "risk_budget_fraction": 0.5, "quote_policy_calibration": {"ev_threshold": 0.001, "uncertainty_cap": 0.2, "min_effective_sample_size": 1.0, "min_fill_probability": 0.0}}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    art_dir = tmp_path / "artifacts" / "run1"
    art_dir.mkdir(parents=True, exist_ok=True)
    snapshot = {"as_of_date": "2026-03-26", "memory_version": "memory_asof_v1", "snapshot_id": "snap1", "prototypes": [{"prototype_id": "p1", "anchor_code": "a1", "embedding": [0.1] * 12, "member_count": 3, "anchor_quality": 0.8, "regime_code": "RISK_ON", "sector_code": "TECH", "side_stats": {"BUY": {"support_count": 3, "decayed_support": 3.0, "mean_return_pct": 0.05, "median_return_pct": 0.04, "win_rate": 0.7, "uncertainty": 0.02, "freshness_days": 1.0, "p_target_first": 0.6, "p_stop_first": 0.2, "p_flat": 0.2, "p_ambiguous": 0.0, "p_no_trade": 0.0, "return_q10_pct": -0.02, "return_q50_pct": 0.03, "return_q90_pct": 0.07, "horizon_up_count": 2, "horizon_down_count": 1}, "SELL": {"support_count": 1, "decayed_support": 1.0, "mean_return_pct": -0.01, "median_return_pct": -0.01, "win_rate": 0.4, "uncertainty": 0.04, "freshness_days": 1.0, "p_target_first": 0.2, "p_stop_first": 0.5, "p_flat": 0.3, "p_ambiguous": 0.0, "p_no_trade": 0.0, "return_q10_pct": -0.03, "return_q50_pct": -0.01, "return_q90_pct": 0.01, "horizon_up_count": 1, "horizon_down_count": 2}}}], "calibration": {"method": "logistic", "slope": 1.0, "intercept": 0.0}, "quote_policy_calibration": manifest["quote_policy_calibration"]}
    (art_dir / "prototype_snapshot.json").write_text(json.dumps(snapshot), encoding="utf-8")
    return str(manifest_path), str(tmp_path / "artifacts")


def test_live_runtime_shadow_run_uses_live_root_and_adapters(tmp_path):
    manifest_path, artifact_dir = _write_bundle(tmp_path)
    adapters = LiveRuntimeAdapters(order_adapter=FakeOrderAdapter(), state_adapter=FakeStateAdapter(), broker_adapter=FakeBrokerAdapter(), calendar_adapter=FakeCalendarAdapter())
    result = run_live_runtime(adapters, market="US", day="2026-03-27", symbols=["AAPL"], manifest_path=manifest_path, artifact_dir=artifact_dir, run_id="run1", mode="shadow", output_dir=str(tmp_path))
    assert result["mode"] == "shadow"
    assert result["diagnostics"]["order_requests"]
    assert result["result_path"].replace("\\", "/").endswith("/live/live_snap1_2026-03-27_shadow.json")


def test_live_runtime_submit_mode_uses_broker_submit(tmp_path):
    manifest_path, artifact_dir = _write_bundle(tmp_path)
    order_adapter = FakeOrderAdapter()
    broker_adapter = FakeBrokerAdapter()
    adapters = LiveRuntimeAdapters(order_adapter=order_adapter, state_adapter=FakeStateAdapter(), broker_adapter=broker_adapter, calendar_adapter=FakeCalendarAdapter())
    result = run_live_runtime(adapters, market="US", day="2026-03-27", symbols=["AAPL"], manifest_path=manifest_path, artifact_dir=artifact_dir, run_id="run1", mode="submit", output_dir=str(tmp_path))
    assert result["summary"]["submitted_count"] >= 1
    assert broker_adapter.submitted
