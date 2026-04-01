from __future__ import annotations

import ast
import json
from typing import Any


def _to_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_matches(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        try:
            payload = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            return []
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def _clamp(value: float, lower: float, upper: float) -> float:
    upper_bound = max(lower, float(upper))
    return float(max(lower, min(float(value), upper_bound)))


def _dominant_side(row: dict[str, Any]) -> str:
    buy_q50 = _to_float(row.get("buy_q50"))
    sell_q50 = _to_float(row.get("sell_q50"))
    if buy_q50 > sell_q50:
        return "BUY"
    if sell_q50 > buy_q50:
        return "SELL"
    chosen_side = str(row.get("chosen_side_before_deploy") or "ABSTAIN").upper()
    if chosen_side in {"BUY", "SELL"}:
        return chosen_side
    return "SELL" if sell_q50 >= 0.0 else "BUY"


def _shape_bucket(q50: float, interval_width: float) -> str:
    abs_q50 = abs(float(q50))
    width = max(float(interval_width), 0.0)
    if width <= abs_q50:
        return "tight"
    if width <= 2.0 * abs_q50:
        return "mid"
    return "wide"


def _normalized_match_shares(matches: list[dict[str, Any]]) -> list[float]:
    shares = [_to_float(match.get("weight_share"), -1.0) for match in matches]
    if shares and all(share >= 0.0 for share in shares):
        total = sum(shares)
        if total > 1e-12:
            return [float(share) / float(total) for share in shares]
    weights = [_to_float(match.get("weight")) for match in matches]
    total = sum(weights)
    if total <= 1e-12:
        return [0.0 for _ in matches]
    return [float(weight) / float(total) for weight in weights]


def _consensus_signature_from_matches(matches: list[dict[str, Any]]) -> str:
    tokens: list[str] = []
    for match in matches[:3]:
        token = str(match.get("representative_hash") or match.get("prototype_id") or "").strip()
        if token:
            tokens.append(token)
    return "|".join(tokens)


def _side_metrics(row: dict[str, Any], side: str) -> dict[str, Any]:
    prefix = "buy" if side == "BUY" else "sell"
    member_matches = _parse_matches(row.get(f"{prefix}_member_top_matches_summary"))
    matches = member_matches or _parse_matches(row.get(f"{prefix}_top_matches_summary") or row.get("top_matches_summary"))
    weight_shares = _normalized_match_shares(matches)
    support_values = [_to_float(match.get("support")) for match in matches]
    consensus_signature = (
        str(row.get(f"{prefix}_member_consensus_signature") or "").strip()
        or str(row.get(f"{prefix}_consensus_signature") or "").strip()
        or _consensus_signature_from_matches(matches)
    )
    q10 = _to_float(row.get(f"{prefix}_q10"), _to_float(row.get("q10")))
    q50 = _to_float(row.get(f"{prefix}_q50"), _to_float(row.get("q50")))
    q90 = _to_float(row.get(f"{prefix}_q90"), _to_float(row.get("q90")))
    interval_width = _to_float(row.get(f"{prefix}_interval_width"), max(q90 - q10, 0.0))
    uncertainty = _to_float(row.get(f"{prefix}_uncertainty"), _to_float(row.get("uncertainty")))
    mixture_ess = _to_float(
        row.get(f"{prefix}_member_mixture_ess"),
        _to_float(row.get(f"{prefix}_mixture_ess"), _to_float(row.get(f"{prefix}_effective_sample_size"), _to_float(row.get("effective_sample_size")))),
    )
    return {
        "q10": q10,
        "q50": q50,
        "q90": q90,
        "q50_d2_return": _to_float(row.get(f"{prefix}_q50_d2_return")),
        "q50_d3_return": _to_float(row.get(f"{prefix}_q50_d3_return")),
        "p_resolved_by_d2": _to_float(row.get(f"{prefix}_p_resolved_by_d2")),
        "p_resolved_by_d3": _to_float(row.get(f"{prefix}_p_resolved_by_d3")),
        "interval_width": max(interval_width, max(q90 - q10, 0.0)),
        "uncertainty": uncertainty,
        "mixture_ess": mixture_ess,
        "prototype_pool_size": _to_int(row.get(f"{prefix}_prototype_pool_size"), 0),
        "ranked_candidate_count": _to_int(row.get(f"{prefix}_ranked_candidate_count"), 0),
        "positive_weight_candidate_count": _to_int(
            row.get(f"{prefix}_positive_weight_member_count"),
            _to_int(row.get(f"{prefix}_positive_weight_candidate_count"), len([share for share in weight_shares if share > 0.0])),
        ),
        "pre_truncation_candidate_count": _to_int(
            row.get(f"{prefix}_member_pre_truncation_count"),
            _to_int(row.get(f"{prefix}_pre_truncation_candidate_count"), len(matches)),
        ),
        "top1_weight_share": _to_float(
            row.get(f"{prefix}_member_top1_weight_share"),
            _to_float(row.get(f"{prefix}_top1_weight_share"), weight_shares[0] if weight_shares else 0.0),
        ),
        "cumulative_weight_top3": _to_float(
            row.get(f"{prefix}_member_cumulative_weight_top3"),
            _to_float(row.get(f"{prefix}_cumulative_weight_top3"), sum(weight_shares[:3])),
        ),
        "member_support_sum": _to_float(row.get(f"{prefix}_member_support_sum"), _to_float(row.get(f"{prefix}_top_match_support_sum"), sum(support_values))),
        "consensus_signature": consensus_signature or "no_consensus",
        "matches": matches,
        "member_candidate_count": _to_int(row.get(f"{prefix}_member_candidate_count"), len(matches)),
    }


def _policy_family_for_row(row: dict[str, Any]) -> str:
    if not bool(row.get("recurring_family")):
        return "echo_or_collapse"
    if float(row.get("dominant_q10", 0.0) or 0.0) > 0.0 and float(row.get("mixture_ess", 0.0) or 0.0) >= 2.0 and float(row.get("top1_weight_share", 0.0) or 0.0) <= 0.75:
        return "tight_consensus"
    if float(row.get("dominant_q50", 0.0) or 0.0) > 0.0 and float(row.get("mixture_ess", 0.0) or 0.0) >= 1.5 and float(row.get("top1_weight_share", 0.0) or 0.0) <= 0.85:
        return "directional_wide"
    return "echo_or_collapse"


def _policy_params(row: dict[str, Any]) -> dict[str, Any]:
    family = str(row.get("policy_family") or "echo_or_collapse")
    q10 = float(row.get("dominant_q10", 0.0) or 0.0)
    q50 = float(row.get("dominant_q50", 0.0) or 0.0)
    q90 = float(row.get("dominant_q90", 0.0) or 0.0)
    interval_width = float(row.get("dominant_interval_width", 0.0) or 0.0)
    uncertainty = float(row.get("dominant_uncertainty", 0.0) or 0.0)
    if family == "tight_consensus":
        return {
            "policy_entry_offset_pct": _clamp(0.10 * interval_width + 0.25 * uncertainty, 0.001, 0.01),
            "policy_target_pct": _clamp(0.70 * q50, 0.003, q90),
            "policy_stop_pct": _clamp(abs(min(q10, -0.50 * uncertainty)), 0.003, 0.02),
            "policy_ttl_days": 1,
        }
    if family == "directional_wide":
        return {
            "policy_entry_offset_pct": _clamp(0.35 * interval_width + 0.20 * uncertainty, 0.002, 0.02),
            "policy_target_pct": _clamp(0.50 * q50, 0.003, q90),
            "policy_stop_pct": _clamp(abs(min(q10, -0.75 * uncertainty)), 0.004, 0.03),
            "policy_ttl_days": 1,
        }
    return {
        "policy_entry_offset_pct": None,
        "policy_target_pct": None,
        "policy_stop_pct": None,
        "policy_ttl_days": None,
    }


def build_pre_optuna_evidence(
    forecast_rows: list[dict[str, Any]] | None,
    *,
    recurring_min_dates: int = 3,
    recurring_min_rows: int = 5,
) -> dict[str, Any]:
    rows = [dict(row) for row in list(forecast_rows or []) if isinstance(row, dict)]
    has_core_columns = bool(rows) and any(("buy_q50" in row or "sell_q50" in row) for row in rows)
    annotated_rows: list[dict[str, Any]] = []
    if rows and has_core_columns:
        for raw_row in rows:
            dominant_side = _dominant_side(raw_row)
            metrics = _side_metrics(raw_row, dominant_side)
            regime_code = str(raw_row.get("query_regime_code") or raw_row.get("regime_code") or "UNKNOWN")
            sector_code = str(raw_row.get("sector_code") or raw_row.get("query_sector_code") or "UNKNOWN")
            shape_bucket = _shape_bucket(metrics["q50"], metrics["interval_width"])
            pattern_key = "|".join(
                [
                    dominant_side,
                    str(metrics["consensus_signature"] or "no_consensus"),
                    regime_code,
                    sector_code,
                    shape_bucket,
                ]
            )
            single_prototype_collapse = bool(
                metrics["member_candidate_count"] <= 1
                or metrics["positive_weight_candidate_count"] <= 1
                or metrics["top1_weight_share"] >= 0.95
                or metrics["mixture_ess"] <= 1.05
            )
            annotated = dict(raw_row)
            annotated.update(
                {
                    "dominant_side": dominant_side,
                    "dominant_q10": metrics["q10"],
                    "dominant_q50": metrics["q50"],
                    "dominant_q90": metrics["q90"],
                    "dominant_q50_d2_return": metrics["q50_d2_return"],
                    "dominant_q50_d3_return": metrics["q50_d3_return"],
                    "dominant_p_resolved_by_d2": metrics["p_resolved_by_d2"],
                    "dominant_p_resolved_by_d3": metrics["p_resolved_by_d3"],
                    "dominant_interval_width": metrics["interval_width"],
                    "dominant_uncertainty": metrics["uncertainty"],
                    "prototype_pool_size": metrics["prototype_pool_size"],
                    "member_candidate_count": metrics["member_candidate_count"],
                    "ranked_candidate_count": metrics["ranked_candidate_count"],
                    "positive_weight_candidate_count": metrics["positive_weight_candidate_count"],
                    "pre_truncation_candidate_count": metrics["pre_truncation_candidate_count"],
                    "top1_weight_share": metrics["top1_weight_share"],
                    "cumulative_weight_top3": metrics["cumulative_weight_top3"],
                    "mixture_ess": metrics["mixture_ess"],
                    "member_support_sum": metrics["member_support_sum"],
                    "consensus_signature": metrics["consensus_signature"],
                    "shape_bucket": shape_bucket,
                    "pattern_key": pattern_key,
                    "query_regime_code": regime_code,
                    "query_sector_code": sector_code,
                    "single_prototype_collapse": single_prototype_collapse,
                    "positive_edge": metrics["q50"] > 0.0,
                }
            )
            annotated_rows.append(annotated)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in annotated_rows:
        grouped.setdefault(str(row.get("pattern_key") or "missing"), []).append(row)

    pattern_family_table: list[dict[str, Any]] = []
    recurring_keys: set[str] = set()
    for pattern_key, group in grouped.items():
        decision_dates = sorted({str(row.get("decision_date") or "") for row in group if row.get("decision_date")})
        anchor_row_count = len(group)
        positive_rows = [row for row in group if bool(row.get("positive_edge"))]
        collapse_rows = [row for row in positive_rows if bool(row.get("single_prototype_collapse"))]
        recurring_family = len(decision_dates) >= recurring_min_dates and anchor_row_count >= recurring_min_rows
        if recurring_family:
            recurring_keys.add(pattern_key)
        pattern_family_table.append(
            {
                "pattern_key": pattern_key,
                "dominant_side": group[0].get("dominant_side"),
                "consensus_signature": group[0].get("consensus_signature"),
                "regime_code": group[0].get("query_regime_code"),
                "sector_code": group[0].get("query_sector_code"),
                "shape_bucket": group[0].get("shape_bucket"),
                "decision_date_count": len(decision_dates),
                "anchor_row_count": anchor_row_count,
                "positive_row_count": len(positive_rows),
                "single_prototype_collapse_share": (float(len(collapse_rows)) / float(len(positive_rows))) if positive_rows else 0.0,
                "median_dominant_q50": sorted(float(row.get("dominant_q50") or 0.0) for row in group)[len(group) // 2],
                "median_top1_weight_share": sorted(float(row.get("top1_weight_share") or 0.0) for row in group)[len(group) // 2],
                "median_mixture_ess": sorted(float(row.get("mixture_ess") or 0.0) for row in group)[len(group) // 2],
                "median_member_support_sum": sorted(float(row.get("member_support_sum") or 0.0) for row in group)[len(group) // 2],
                "recurring_family": recurring_family,
            }
        )
    pattern_family_table.sort(key=lambda row: (-int(bool(row.get("recurring_family"))), -int(row.get("anchor_row_count") or 0), -int(row.get("decision_date_count") or 0), str(row.get("pattern_key") or "")))

    eligible_pattern_keys: set[str] = set()
    policy_family_candidates: list[dict[str, Any]] = []
    for row in annotated_rows:
        recurring_family = str(row.get("pattern_key") or "") in recurring_keys
        row["recurring_family"] = recurring_family
        row["policy_family"] = _policy_family_for_row(row)
        row["optuna_eligible"] = row["policy_family"] != "echo_or_collapse"
        row.update(_policy_params(row))
        if row["optuna_eligible"]:
            eligible_pattern_keys.add(str(row.get("pattern_key") or ""))
            policy_family_candidates.append(
                {
                    "decision_date": row.get("decision_date"),
                    "symbol": row.get("symbol"),
                    "dominant_side": row.get("dominant_side"),
                    "pattern_key": row.get("pattern_key"),
                    "policy_family": row.get("policy_family"),
                    "shape_bucket": row.get("shape_bucket"),
                    "consensus_signature": row.get("consensus_signature"),
                    "dominant_q10": row.get("dominant_q10"),
                    "dominant_q50": row.get("dominant_q50"),
                    "dominant_q90": row.get("dominant_q90"),
                    "dominant_q50_d2_return": row.get("dominant_q50_d2_return"),
                    "dominant_q50_d3_return": row.get("dominant_q50_d3_return"),
                    "dominant_p_resolved_by_d2": row.get("dominant_p_resolved_by_d2"),
                    "dominant_p_resolved_by_d3": row.get("dominant_p_resolved_by_d3"),
                    "dominant_interval_width": row.get("dominant_interval_width"),
                    "dominant_uncertainty": row.get("dominant_uncertainty"),
                    "mixture_ess": row.get("mixture_ess"),
                    "top1_weight_share": row.get("top1_weight_share"),
                    "member_support_sum": row.get("member_support_sum"),
                    "entry_offset_pct": row.get("policy_entry_offset_pct"),
                    "target_pct": row.get("policy_target_pct"),
                    "stop_pct": row.get("policy_stop_pct"),
                    "ttl_days": row.get("policy_ttl_days"),
                }
            )

    positive_rows = [row for row in annotated_rows if bool(row.get("positive_edge"))]
    collapse_count = sum(1 for row in positive_rows if bool(row.get("single_prototype_collapse")))
    single_prototype_collapse_share = (float(collapse_count) / float(len(positive_rows))) if positive_rows else 0.0
    eligible_families = {str(row.get("policy_family") or "") for row in annotated_rows if bool(row.get("optuna_eligible"))}
    if not rows or not has_core_columns:
        verdict = "not_ready_contract_or_environment"
        verdict_reason = "forecast_panel rows are missing or do not carry the side-level distribution contract required for pre-Optuna analysis."
    elif policy_family_candidates:
        verdict = "optuna_ready"
        verdict_reason = "At least one repeated pattern family produced an Optuna-eligible execution policy candidate."
    elif single_prototype_collapse_share >= 0.5:
        verdict = "not_ready_single_prototype_collapse"
        verdict_reason = "Positive-edge rows are still dominated by single-prototype collapse, so policy search would optimize echo artifacts instead of repeatable mixtures."
    else:
        verdict = "not_ready_no_repeated_patterns"
        verdict_reason = "The forecast surface contains signals, but they do not recur often enough under a stable pattern key to justify Optuna yet."

    next_optuna_target_scope = "blocked"
    if verdict == "optuna_ready":
        if eligible_families == {"tight_consensus"}:
            next_optuna_target_scope = "tight_consensus_only"
        elif eligible_families == {"directional_wide"}:
            next_optuna_target_scope = "directional_wide_only"
        else:
            next_optuna_target_scope = "mixed_families"

    top_recurring_families = [
        {
            "pattern_key": row.get("pattern_key"),
            "dominant_side": row.get("dominant_side"),
            "shape_bucket": row.get("shape_bucket"),
            "decision_date_count": row.get("decision_date_count"),
            "anchor_row_count": row.get("anchor_row_count"),
            "single_prototype_collapse_share": row.get("single_prototype_collapse_share"),
        }
        for row in pattern_family_table
        if bool(row.get("recurring_family"))
    ][:5]

    packet = {
        "pre_optuna_ready": verdict == "optuna_ready",
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "recurring_family_count": len(recurring_keys),
        "eligible_policy_family_count": len(eligible_pattern_keys),
        "eligible_row_count": len(policy_family_candidates),
        "top_recurring_families": top_recurring_families,
        "single_prototype_collapse_share": single_prototype_collapse_share,
        "next_optuna_target_scope": next_optuna_target_scope,
    }
    return {
        "forecast_rows": annotated_rows,
        "pattern_family_table": pattern_family_table,
        "policy_family_candidates": policy_family_candidates,
        "pre_optuna_packet": packet,
    }
