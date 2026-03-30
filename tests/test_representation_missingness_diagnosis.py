from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    root = Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "representation_missingness_diagnosis.py"
    spec = importlib.util.spec_from_file_location("representation_missingness_diagnosis", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_feature_coverage_score_formula_matches_contract():
    module = _load_module()
    score = module.compute_feature_coverage_score(
        raw_zero_default_keys_count=4,
        raw_feature_count=20,
        transform_missing_keys_filled_zero_count=2,
        transform_feature_count=10,
        macro_series_present_count=4,
        sector_proxy_fallback_to_self=True,
    )
    expected = max(0.0, 1 - (0.35 * (4 / 20) + 0.35 * (2 / 10) + 0.20 * (1 - 4 / 5) + 0.25))
    assert score == expected


def test_summary_and_output_files_are_generated(tmp_path):
    module = _load_module()
    rows = [
        {
            "decision_date": "2026-01-02",
            "symbol": "AAPL",
            "query_available": True,
            "exclude_reasons": [],
            "insufficient_query_history": False,
            "insufficient_bars": False,
            "missing_decision_bar": False,
            "sector_proxy_fallback_to_self": False,
            "market_proxy_peer_count_min": 5,
            "market_proxy_peer_count_max": 6,
            "macro_history_length": 20,
            "macro_series_present_count": 5,
            "raw_feature_count": 10,
            "transform_feature_count": 12,
            "raw_zero_default_keys_count": 1,
            "raw_zero_default_keys_top_n": ["beta_20"],
            "transform_missing_keys_filled_zero_count": 2,
            "transform_missing_keys_filled_zero_top_n": ["vix_zscore_20", "rate_pct_change_20"],
            "transformed_zero_dominant_keys_top_n": ["beta_20"],
            "feature_coverage_score": 0.82,
            "too_sparse": False,
        },
        {
            "decision_date": "2026-01-03",
            "symbol": "MSFT",
            "query_available": False,
            "exclude_reasons": ["insufficient_query_history"],
            "insufficient_query_history": True,
            "insufficient_bars": False,
            "missing_decision_bar": False,
            "sector_proxy_fallback_to_self": True,
            "market_proxy_peer_count_min": 0,
            "market_proxy_peer_count_max": 0,
            "macro_history_length": 5,
            "macro_series_present_count": 3,
            "raw_feature_count": 0,
            "transform_feature_count": 0,
            "raw_zero_default_keys_count": 0,
            "raw_zero_default_keys_top_n": [],
            "transform_missing_keys_filled_zero_count": 0,
            "transform_missing_keys_filled_zero_top_n": [],
            "transformed_zero_dominant_keys_top_n": [],
            "feature_coverage_score": 0.45,
            "too_sparse": True,
        },
    ]
    summary = module.summarize_rows(rows)
    outputs = module.write_outputs(rows=rows, summary=summary, output_root=tmp_path)
    rows_path = Path(outputs["rows_path"])
    summary_path = Path(outputs["summary_path"])
    report_path = Path(outputs["report_path"])
    assert rows_path.exists()
    assert summary_path.exists()
    assert report_path.exists()
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["row_count"] == 2
    assert "insufficient_query_history" in summary_payload["exclude_reason_histogram"]
    report_text = report_path.read_text(encoding="utf-8")
    assert "medium_verdict_hold_recommended" in report_text


def test_build_row_record_preserves_excluded_rows_and_fallback_flags():
    module = _load_module()
    row = module.build_row_record(
        symbol="AAPL",
        decision_date="2026-01-04",
        query_item={
            "meta": {
                "proxy_diagnostics": {
                    "market": {"peer_count_by_date": {"2026-01-03": 4}},
                    "sector": {"peer_count_by_date": {"2026-01-03": 1}, "fallback_to_self": True},
                },
                "raw_zero_default_keys": ["beta_20"],
                "transform_missing_keys_filled_zero": ["vix_zscore_20"],
                "transformed_zero_feature_keys": ["beta_20", "vix_zscore_20"],
                "raw_features": {"beta_20": 0.0, "ret_1": 0.01},
                "feature_keys": ["beta_20", "ret_1", "vix_zscore_20"],
                "macro_series_present_count": 4,
            }
        },
        query_reasons=["missing_decision_bar"],
        library_reasons=["insufficient_bars"],
        macro_window={"2026-01-04": {"vix": 20.0, "rate": 3.0, "dollar": 100.0, "oil": 70.0}},
    )
    assert row["missing_decision_bar"] is True
    assert row["insufficient_bars"] is True
    assert row["sector_proxy_fallback_to_self"] is True
    assert row["market_proxy_peer_count_max"] == 4
    assert row["transform_missing_keys_filled_zero_count"] == 1
