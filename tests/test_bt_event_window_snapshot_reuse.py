from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import materialize_bt_event_window as materialize
from scripts import research_first_batch as first_batch
from scripts import research_matrix_batch as matrix_batch


class DummySession:
    def __init__(self, manifest_rows=None):
        self.manifest_rows = manifest_rows or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        if "information_schema.tables" in sql:
            return DummyResult({"manifest_exists": True})
        if "FROM meta.bt_scenario_snapshot_manifest" in sql:
            row = self.manifest_rows.get(params.get("scenario_id"))
            return DummyResult(row)
        raise AssertionError(f"unexpected SQL: {sql}")


class DummyResult:
    def __init__(self, row):
        self.row = row

    def one(self):
        return type("Row", (), {"_mapping": self.row})()

    def fetchone(self):
        if self.row is None:
            return None
        return type("Row", (), {"_mapping": self.row})()


class DummyFactory:
    def __init__(self, session):
        self.session = session

    def __call__(self):
        return self.session


def _fake_backtest_result(request):
    return {
        "manifest": {"data_snapshot_id": f"snap::{request.scenario.scenario_id}"},
        "validation": {"fold_engine": {"folds": [], "aggregate": {}}},
        "portfolio": {"decisions": [], "date_artifacts": [{"open_position_count": 0}]},
        "plans": [],
        "fills": [],
        "diagnostics": {},
        "bars_by_symbol": {},
        "historical_context": {"bars_by_symbol": {}},
        "summary": {},
        "skipped": [],
    }


def test_materialize_compute_manifest_stable_across_run_id_changes():
    rows = [
        {
            "scenario_id": "legacy_discovery",
            "market": "US",
            "symbol": "AAPL",
            "ticker_id": 1,
            "event_time": "2026-01-03T00:00:00+00:00",
            "anchor_date": "2026-01-02",
            "reference_date": "2026-01-03",
            "side_bias": "BUY",
            "signal_strength": 1.2,
            "confidence": 0.8,
            "current_price": 100.0,
            "atr_pct": 0.02,
            "target_return_pct": 0.04,
            "max_reverse_pct": 0.03,
            "expected_horizon_days": 5,
            "reverse_breach_day": None,
            "outcome_label": "UNKNOWN",
            "provenance": {},
            "diagnostics": {},
            "notes": [],
        }
    ]
    m1 = materialize.compute_manifest(scenario_id="legacy_discovery", phase="discovery", source_kind="import-json", market="US", rows=rows, notes="run-a")
    m2 = materialize.compute_manifest(scenario_id="legacy_discovery", phase="discovery", source_kind="import-json", market="US", rows=rows, notes="run-b")
    assert m1["snapshot_id"] == m2["snapshot_id"]


def test_first_batch_reuses_same_legacy_snapshot_across_runs(monkeypatch, tmp_path):
    manifest_rows = {
        "legacy_discovery": {"snapshot_id": "btw-disc-1", "scenario_id": "legacy_discovery", "phase": "discovery", "row_count": 10, "window_start": "2026-01-01", "window_end": "2026-03-31"},
        "legacy_holdout": {"snapshot_id": "btw-hold-1", "scenario_id": "legacy_holdout", "phase": "holdout", "row_count": 5, "window_start": "2026-04-01", "window_end": "2026-04-30"},
    }
    monkeypatch.setattr(first_batch, "create_backtest_session_factory", lambda cfg: DummyFactory(DummySession(manifest_rows)))
    monkeypatch.setattr(first_batch, "guard_backtest_local_only", lambda url: None)
    monkeypatch.setattr(first_batch.LocalBacktestDbConfig, "from_env", classmethod(lambda cls: type("Cfg", (), {"url": "postgresql://local", "schema": "trading"})()))
    monkeypatch.setattr(first_batch, "preflight_local_db", lambda *args, **kwargs: {"discovery_start": "2026-01-01", "discovery_end": "2026-03-31", "holdout_start": "2026-04-01", "holdout_end": "2026-04-30", "window_mode": "9m_3m", "ohlcv_common_coverage": 1.0, "macro_coverage": 1.0, "sector_coverage": 1.0, "legacy_snapshot_ready": False})
    calls = []

    def fake_run_backtest(**kwargs):
        calls.append(kwargs["scenario_id"])
        return _fake_backtest_result(kwargs["request"])

    monkeypatch.setattr(first_batch, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(first_batch, "run_configs", lambda: [{"label": "legacy_event_window", "strategy_mode": "legacy_event_window", "metadata": {}}])

    first_batch.main.__globals__["date"] = type("FixedDate", (), {"today": staticmethod(lambda: type("D", (), {"strftime": lambda self, fmt: "20260327"})())})
    monkeypatch.setattr("sys.argv", ["research_first_batch.py", "--output-root", str(tmp_path)])
    first_batch.main()
    monkeypatch.setattr("sys.argv", ["research_first_batch.py", "--output-root", str(tmp_path), "--experiment-group", "rerun"])
    first_batch.main()

    assert calls == ["legacy_discovery", "legacy_holdout", "legacy_discovery", "legacy_holdout"]
    manifests = sorted(tmp_path.rglob("manifest.json"))
    payloads = [json.loads(p.read_text(encoding="utf-8")) for p in manifests]
    assert {p["bt_event_window_snapshot_id"] for p in payloads} == {"btw-disc-1"}


def test_first_batch_missing_snapshot_fails_friendly(monkeypatch):
    monkeypatch.setattr(first_batch, "fetch_snapshot_manifest", lambda scenario_id: None)
    with pytest.raises(RuntimeError, match="materialize_bt_event_window 먼저 실행 또는 --skip-legacy-reference 사용"):
        first_batch.resolve_legacy_scenarios(preflight={}, discovery_scenario_id="legacy_discovery", holdout_scenario_id="legacy_holdout", skip_legacy_reference=False)


def test_matrix_batch_skip_legacy_reference_allows_nonlegacy_runs(monkeypatch, tmp_path):
    monkeypatch.setattr(matrix_batch, "preflight_local_db", lambda *args, **kwargs: {"discovery_start": "2026-01-01", "discovery_end": "2026-03-31", "holdout_start": "2026-04-01", "holdout_end": "2026-04-30", "window_mode": "9m_3m", "ohlcv_common_coverage": 1.0, "macro_coverage": 1.0, "sector_coverage": 1.0, "legacy_snapshot_ready": False})
    monkeypatch.setattr(matrix_batch, "resolve_legacy_scenarios", lambda **kwargs: {"skip_legacy_reference": True, "discovery": None, "holdout": None})
    monkeypatch.setattr(matrix_batch, "run_backtest", lambda **kwargs: _fake_backtest_result(kwargs["request"]))
    monkeypatch.setattr(matrix_batch, "build_spec", lambda: type("Spec", (), {"feature_window_bars": 60, "lookback_horizons": [1, 5], "horizon_days": 5, "target_return_pct": 0.04, "stop_return_pct": 0.03, "flat_return_band_pct": 0.005, "to_dict": lambda self: {}, "spec_hash": lambda self: "spec-1"})())
    matrix_batch.main.__globals__["date"] = type("FixedDate", (), {"today": staticmethod(lambda: type("D", (), {"strftime": lambda self, fmt: "20260327"})())})
    monkeypatch.setattr("sys.argv", ["research_matrix_batch.py", "--output-root", str(tmp_path), "--skip-legacy-reference"])
    assert matrix_batch.main() == 0
