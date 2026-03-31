import csv
import json
from pathlib import Path

from scripts.forecast_deploy_contract_audit import audit_contract


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_audit_contract_detects_zero_collapse_between_forecast_and_quote_policy(tmp_path):
    forecast_path = tmp_path / "forecast_panel.csv"
    decisions_path = tmp_path / "decisions.csv"
    result_json_path = tmp_path / "result.json"
    _write_csv(
        forecast_path,
        [
            "decision_date",
            "symbol",
            "chosen_side_before_deploy",
            "forecast_selected",
            "q10",
            "q50",
            "q90",
            "effective_sample_size",
            "lower_bound",
            "interval_width",
        ],
        [
            {
                "decision_date": "2025-04-03",
                "symbol": "JPM",
                "chosen_side_before_deploy": "SELL",
                "forecast_selected": "true",
                "q10": "0.012",
                "q50": "0.031",
                "q90": "0.064",
                "effective_sample_size": "4.75",
                "lower_bound": "0.028",
                "interval_width": "0.05",
            }
        ],
    )
    diagnostics = {
        "quote_policy": {
            "q10_return": 0.0,
            "q50_return": 0.0,
            "q90_return": 0.064,
            "effective_sample_size": 0.0,
            "chosen_policy_reason": "confidence_bound_non_positive,low_effective_sample_size",
            "optimizer_best": {"retained_edge": -0.0037},
            "decision_surface_summary": {
                "decision_rule": {
                    "winner": "SELL",
                    "chosen_effective_sample_size": 4.75,
                    "chosen_lower_bound": 0.028,
                }
            },
        }
    }
    _write_csv(
        decisions_path,
        ["decision_date", "symbol", "side", "kill_reason", "diagnostics"],
        [
            {
                "decision_date": "2025-04-03",
                "symbol": "JPM",
                "side": "SELL",
                "kill_reason": "quote_policy_no_trade",
                "diagnostics": repr({"quote_policy": diagnostics["quote_policy"]}),
            }
        ],
    )

    summary = audit_contract(forecast_panel_csv=forecast_path, decisions_csv=decisions_path)
    result_json_path.write_text(
        json.dumps(
            {
                "portfolio": {
                    "decisions": [
                        {
                            "decision_date": "2025-04-03",
                            "symbol": "JPM",
                            "side": "SELL",
                            "kill_reason": "quote_policy_no_trade",
                            "diagnostics": {"quote_policy": diagnostics["quote_policy"]},
                        }
                    ]
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    summary_from_result = audit_contract(forecast_panel_csv=forecast_path, result_json=result_json_path)

    assert summary["forecast_selected_count"] == 1
    assert summary["join_count"] == 1
    assert summary["chosen_ess_positive_quote_ess_zero_count"] == 1
    assert summary["chosen_q50_nonzero_quote_q50_zero_count"] == 1
    assert summary["positive_lower_bound_quote_q50_zero_count"] == 1
    assert summary["quote_q10_all_zero"] is True
    assert summary["quote_q50_all_zero"] is True
    assert summary["quote_effective_sample_size_all_zero"] is True
    assert summary["retained_edge_values"] == [-0.0037]
    assert summary_from_result["join_count"] == 1
    assert summary_from_result["chosen_q50_nonzero_quote_q50_zero_count"] == 1
