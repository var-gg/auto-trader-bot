from __future__ import annotations

import argparse
import ast
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_struct(raw: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return ast.literal_eval(text)


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_decision_rows(*, decisions_csv: Path | None = None, result_json: Path | None = None) -> list[dict[str, Any]]:
    if decisions_csv is not None:
        return _load_csv_rows(decisions_csv)
    if result_json is not None:
        payload = json.loads(result_json.read_text(encoding="utf-8"))
        return [
            {
                "decision_date": row.get("decision_date"),
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "kill_reason": row.get("kill_reason"),
                "diagnostics": row.get("diagnostics") or {},
            }
            for row in ((payload.get("portfolio") or {}).get("decisions") or [])
        ]
    raise ValueError("Either decisions_csv or result_json is required")


def _forecast_chosen_metrics(row: dict[str, Any]) -> dict[str, float | None]:
    chosen_side = str(row.get("chosen_side_before_deploy") or "ABSTAIN").upper()
    if "q10" in row or "effective_sample_size" in row:
        return {
            "q10": _to_float(row.get("q10")),
            "q50": _to_float(row.get("q50")),
            "q90": _to_float(row.get("q90")),
            "effective_sample_size": _to_float(row.get("effective_sample_size")),
            "lower_bound": _to_float(row.get("lower_bound")),
            "interval_width": _to_float(row.get("interval_width")),
        }
    prefix = "buy" if chosen_side == "BUY" else "sell" if chosen_side == "SELL" else None
    return {
        "q10": _to_float(row.get(f"{prefix}_q10")) if prefix else None,
        "q50": _to_float(row.get(f"{prefix}_q50")) if prefix else None,
        "q90": _to_float(row.get(f"{prefix}_q90")) if prefix else None,
        "effective_sample_size": _to_float(row.get(f"{prefix}_effective_sample_size")) if prefix else None,
        "lower_bound": _to_float(row.get("lower_bound")),
        "interval_width": _to_float(row.get("interval_width")),
    }


def audit_contract(*, forecast_panel_csv: Path, decisions_csv: Path | None = None, result_json: Path | None = None) -> dict[str, Any]:
    forecast_rows = [row for row in _load_csv_rows(forecast_panel_csv) if _truthy(row.get("forecast_selected"))]
    forecast_by_key = {(str(row.get("decision_date")), str(row.get("symbol"))): row for row in forecast_rows}
    decisions = _load_decision_rows(decisions_csv=decisions_csv, result_json=result_json)
    kill_reason_histogram = Counter(str(row.get("kill_reason") or "") for row in decisions)
    policy_reason_histogram: Counter[str] = Counter()
    contract_missing_reason_histogram: Counter[str] = Counter()
    joined_rows: list[dict[str, Any]] = []
    unmatched_decisions: list[dict[str, Any]] = []

    for row in decisions:
        diagnostics = _parse_struct(row.get("diagnostics"))
        quote = dict((diagnostics or {}).get("quote_policy") or {})
        if not quote:
            continue
        key = (str(row.get("decision_date")), str(row.get("symbol")))
        forecast_row = forecast_by_key.get(key)
        if forecast_row is None:
            unmatched_decisions.append({"decision_date": key[0], "symbol": key[1], "kill_reason": row.get("kill_reason")})
            continue
        forecast_metrics = _forecast_chosen_metrics(forecast_row)
        decision_surface_summary = dict(quote.get("decision_surface_summary") or {})
        decision_rule = dict(decision_surface_summary.get("decision_rule") or {})
        contract_missing_reasons = [str(reason) for reason in (quote.get("contract_missing_reasons") or [])]
        for reason in contract_missing_reasons:
            contract_missing_reason_histogram[reason] += 1
        policy_reason_histogram[str(quote.get("chosen_policy_reason") or "")] += 1
        joined_rows.append(
            {
                "decision_date": key[0],
                "symbol": key[1],
                "side": str(row.get("side") or ""),
                "kill_reason": str(row.get("kill_reason") or ""),
                "policy_reason": str(quote.get("chosen_policy_reason") or ""),
                "forecast_q10": forecast_metrics["q10"],
                "forecast_q50": forecast_metrics["q50"],
                "forecast_q90": forecast_metrics["q90"],
                "forecast_effective_sample_size": forecast_metrics["effective_sample_size"],
                "forecast_lower_bound": forecast_metrics["lower_bound"],
                "quote_q10": _to_float(quote.get("q10_return")),
                "quote_q50": _to_float(quote.get("q50_return")),
                "quote_q90": _to_float(quote.get("q90_return")),
                "quote_effective_sample_size": _to_float(quote.get("effective_sample_size")),
                "retained_edge": _to_float(((quote.get("optimizer_best") or {}).get("retained_edge"))),
                "contract_missing_reasons": contract_missing_reasons,
                "decision_surface_winner": decision_rule.get("winner"),
                "decision_surface_chosen_effective_sample_size": _to_float(decision_rule.get("chosen_effective_sample_size")),
                "decision_surface_chosen_lower_bound": _to_float(decision_rule.get("chosen_lower_bound")),
            }
        )

    joined_keys = {(row["decision_date"], row["symbol"]) for row in joined_rows}
    unmatched_forecast = [
        {"decision_date": key[0], "symbol": key[1]}
        for key in forecast_by_key
        if key not in joined_keys
    ]
    chosen_ess_positive_quote_ess_zero = [
        row
        for row in joined_rows
        if (row["forecast_effective_sample_size"] or 0.0) > 0.0 and (row["quote_effective_sample_size"] or 0.0) == 0.0
    ]
    chosen_q50_nonzero_quote_q50_zero = [
        row
        for row in joined_rows
        if (row["forecast_q50"] or 0.0) != 0.0 and (row["quote_q50"] or 0.0) == 0.0
    ]
    positive_lower_bound_quote_q50_zero = [
        row
        for row in joined_rows
        if (row["forecast_lower_bound"] or 0.0) > 0.0 and (row["quote_q50"] or 0.0) == 0.0
    ]
    retained_edges = sorted({round(float(row["retained_edge"]), 6) for row in joined_rows if row["retained_edge"] is not None})
    summary = {
        "forecast_panel_csv": str(forecast_panel_csv),
        "decisions_csv": str(decisions_csv) if decisions_csv is not None else None,
        "result_json": str(result_json) if result_json is not None else None,
        "forecast_selected_count": len(forecast_rows),
        "decision_count": len(decisions),
        "decision_with_quote_policy_count": len(joined_rows) + len(unmatched_decisions),
        "join_count": len(joined_rows),
        "unmatched_forecast_selected_count": len(unmatched_forecast),
        "unmatched_decision_count": len(unmatched_decisions),
        "kill_reason_histogram": dict(kill_reason_histogram),
        "policy_reason_histogram": dict(policy_reason_histogram),
        "contract_missing_reason_histogram": dict(contract_missing_reason_histogram),
        "chosen_ess_positive_quote_ess_zero_count": len(chosen_ess_positive_quote_ess_zero),
        "chosen_q50_nonzero_quote_q50_zero_count": len(chosen_q50_nonzero_quote_q50_zero),
        "positive_lower_bound_quote_q50_zero_count": len(positive_lower_bound_quote_q50_zero),
        "quote_q10_all_zero": bool(joined_rows) and all((row["quote_q10"] or 0.0) == 0.0 for row in joined_rows),
        "quote_q50_all_zero": bool(joined_rows) and all((row["quote_q50"] or 0.0) == 0.0 for row in joined_rows),
        "quote_effective_sample_size_all_zero": bool(joined_rows) and all((row["quote_effective_sample_size"] or 0.0) == 0.0 for row in joined_rows),
        "retained_edge_unique_count": len(retained_edges),
        "retained_edge_values": retained_edges[:10],
        "examples": chosen_q50_nonzero_quote_q50_zero[:5],
        "unmatched_forecast_examples": unmatched_forecast[:5],
        "unmatched_decision_examples": unmatched_decisions[:5],
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit the forecast-to-deploy contract on persisted artifacts.")
    parser.add_argument("--forecast-panel-csv", required=True, help="Path to forecast_panel.csv")
    parser.add_argument("--decisions-csv", help="Path to decisions.csv")
    parser.add_argument("--result-json", help="Path to a result JSON containing portfolio.decisions")
    parser.add_argument("--output-json", help="Optional path for the audit summary JSON")
    args = parser.parse_args()
    if not args.decisions_csv and not args.result_json:
        parser.error("one of --decisions-csv or --result-json is required")

    summary = audit_contract(
        forecast_panel_csv=Path(args.forecast_panel_csv),
        decisions_csv=Path(args.decisions_csv) if args.decisions_csv else None,
        result_json=Path(args.result_json) if args.result_json else None,
    )
    payload = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
