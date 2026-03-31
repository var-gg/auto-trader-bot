from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shared.domain.models import Side, SignalCandidate


@dataclass(frozen=True)
class QuotePolicyInput:
    side: Side
    calibrated_ev: float
    q10_return: float
    q50_return: float
    q90_return: float
    expected_mae: float
    expected_mfe: float
    fill_probability_proxy: float
    cost_bps: float
    uncertainty: float
    effective_sample_size: float
    atr_pct: float
    current_price: float
    expected_horizon_days: int
    regime_code: str = "UNKNOWN"
    sector_code: str = "UNKNOWN"
    decision_surface_summary: dict | None = None
    contract_missing_reasons: tuple[str, ...] = ()
    payload_sources: dict | None = None


@dataclass(frozen=True)
class QuotePolicyDecision:
    policy_name: str
    buy_gap: float
    sell_gap: float
    size_multiplier: float
    no_trade: bool
    diagnostics: dict


@dataclass(frozen=True)
class QuotePolicyConfig:
    ev_threshold: float = 0.005
    uncertainty_cap: float = 0.12
    min_effective_sample_size: float = 1.5
    min_fill_probability: float = 0.10
    gap_grid: tuple[float, ...] = (0.002, 0.004, 0.006, 0.010, 0.015, 0.020, 0.030, 0.040, 0.050)
    size_grid: tuple[float, ...] = (0.25, 0.50, 0.75, 1.00, 1.25, 1.50, 2.00)


def _to_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_float(*choices: tuple[str, Any]) -> tuple[float | None, str | None]:
    for source, value in choices:
        parsed = _to_float_or_none(value)
        if parsed is not None:
            return parsed, source
    return None, None


def _extract_side_mapping(container: dict | None, *keys: str) -> tuple[dict[str, Any], str | None]:
    if not isinstance(container, dict):
        return {}, None
    for key in keys:
        value = container.get(key)
        if isinstance(value, dict):
            return value, key
    return {}, None


def signal_to_policy_input(candidate: SignalCandidate) -> QuotePolicyInput:
    diagnostics = candidate.diagnostics or {} if isinstance(candidate.diagnostics, dict) else {}
    side_name = "buy" if candidate.side_bias == Side.BUY else "sell"
    legacy_side_name = "long" if candidate.side_bias == Side.BUY else "short"
    ev = diagnostics.get("ev", {}) if isinstance(diagnostics, dict) else {}
    row, ev_source_key = _extract_side_mapping(ev, side_name, legacy_side_name)
    surface = diagnostics.get("decision_surface", {}) if isinstance(diagnostics, dict) else {}
    surface_side, surface_source_key = _extract_side_mapping(surface, side_name, legacy_side_name)
    scorer = diagnostics.get("scorer_diagnostics", {}) if isinstance(diagnostics, dict) else {}
    scorer_side, scorer_source_key = _extract_side_mapping(scorer, side_name, legacy_side_name)
    chosen_payload = diagnostics.get("chosen_side_payload")
    chosen_payload_source = "chosen_side_payload"
    if not isinstance(chosen_payload, dict):
        chosen_payload = surface.get("chosen_payload")
        chosen_payload_source = "decision_surface.chosen_payload"
    chosen_payload = chosen_payload if isinstance(chosen_payload, dict) else {}
    chosen_payload_side = str(chosen_payload.get("chosen_side") or chosen_payload.get("side") or "").upper()
    chosen_payload_matches = not chosen_payload_side or chosen_payload_side == candidate.side_bias.value
    q10_return, q10_source = _pick_float(
        (f"{chosen_payload_source}.q10_return", chosen_payload.get("q10_return") if chosen_payload_matches else None),
        (f"{chosen_payload_source}.q10", chosen_payload.get("q10") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.q10", scorer_side.get("q10")),
        (f"decision_surface.{surface_source_key}.q10", surface_side.get("q10")),
        (f"ev.{ev_source_key}.q10_return", row.get("q10_return")),
    )
    q50_return, q50_source = _pick_float(
        (f"{chosen_payload_source}.q50_return", chosen_payload.get("q50_return") if chosen_payload_matches else None),
        (f"{chosen_payload_source}.q50", chosen_payload.get("q50") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.q50", scorer_side.get("q50")),
        (f"decision_surface.{surface_source_key}.q50", surface_side.get("q50")),
        (f"ev.{ev_source_key}.q50_return", row.get("q50_return")),
    )
    q90_return, q90_source = _pick_float(
        (f"{chosen_payload_source}.q90_return", chosen_payload.get("q90_return") if chosen_payload_matches else None),
        (f"{chosen_payload_source}.q90", chosen_payload.get("q90") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.q90", scorer_side.get("q90")),
        (f"decision_surface.{surface_source_key}.q90", surface_side.get("q90")),
        (f"ev.{ev_source_key}.q90_return", row.get("q90_return")),
    )
    expected_mae, expected_mae_source = _pick_float(
        (f"{chosen_payload_source}.expected_mae", chosen_payload.get("expected_mae") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.expected_mae", scorer_side.get("expected_mae")),
        (f"decision_surface.{surface_source_key}.expected_mae", surface_side.get("expected_mae")),
        (f"ev.{ev_source_key}.expected_mae", row.get("expected_mae")),
    )
    expected_mfe, expected_mfe_source = _pick_float(
        (f"{chosen_payload_source}.expected_mfe", chosen_payload.get("expected_mfe") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.expected_mfe", scorer_side.get("expected_mfe")),
        (f"decision_surface.{surface_source_key}.expected_mfe", surface_side.get("expected_mfe")),
        (f"ev.{ev_source_key}.expected_mfe", row.get("expected_mfe")),
    )
    effective_sample_size, ess_source = _pick_float(
        (f"{chosen_payload_source}.effective_sample_size", chosen_payload.get("effective_sample_size") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.effective_sample_size", scorer_side.get("effective_sample_size")),
        (f"scorer_diagnostics.{scorer_source_key}.n_eff", scorer_side.get("n_eff")),
        (f"decision_surface.{surface_source_key}.effective_sample_size", surface_side.get("effective_sample_size")),
        (f"ev.{ev_source_key}.effective_sample_size", row.get("effective_sample_size")),
    )
    uncertainty, uncertainty_source = _pick_float(
        (f"{chosen_payload_source}.uncertainty", chosen_payload.get("uncertainty") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.uncertainty", scorer_side.get("uncertainty")),
        (f"decision_surface.{surface_source_key}.uncertainty", surface_side.get("uncertainty")),
        (f"ev.{ev_source_key}.uncertainty", row.get("uncertainty")),
    )
    calibrated_ev, calibrated_ev_source = _pick_float(
        (f"ev.{ev_source_key}.calibrated_ev", row.get("calibrated_ev")),
        (f"ev.{ev_source_key}.expected_net_return", row.get("expected_net_return")),
        (f"{chosen_payload_source}.expected_net_return", chosen_payload.get("expected_net_return") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.expected_net_return", scorer_side.get("expected_net_return")),
        ("candidate.signal_strength", candidate.signal_strength),
    )
    fill_proxy, fill_source = _pick_float(
        (f"{chosen_payload_source}.fill_probability_proxy", chosen_payload.get("fill_probability_proxy") if chosen_payload_matches else None),
        (f"{chosen_payload_source}.p_target_first", chosen_payload.get("p_target_first") if chosen_payload_matches else None),
        (f"scorer_diagnostics.{scorer_source_key}.p_target_first", scorer_side.get("p_target_first")),
        (f"scorer_diagnostics.{scorer_source_key}.p_target", scorer_side.get("p_target")),
        (f"decision_surface.{surface_source_key}.p_target_first", surface_side.get("p_target_first")),
        (f"ev.{ev_source_key}.calibrated_win_prob", row.get("calibrated_win_prob")),
        ("candidate.confidence", candidate.confidence),
    )
    contract_missing_reasons: list[str] = []
    if q10_return is None or q50_return is None or q90_return is None:
        contract_missing_reasons.append("contract_invalid_missing_distribution")
    if effective_sample_size is None:
        contract_missing_reasons.append("contract_invalid_missing_effective_sample_size")
    if chosen_payload and not chosen_payload_matches:
        contract_missing_reasons.append("contract_invalid_chosen_side_mismatch")
    return QuotePolicyInput(
        side=candidate.side_bias,
        calibrated_ev=float(calibrated_ev or 0.0),
        q10_return=float(q10_return or 0.0),
        q50_return=float(q50_return or 0.0),
        q90_return=float(q90_return or 0.0),
        expected_mae=float(expected_mae or 0.0),
        expected_mfe=float(expected_mfe or 0.0),
        fill_probability_proxy=float(fill_proxy or 0.0),
        cost_bps=float(diagnostics.get("query", {}).get("estimated_cost_bps", 10.0) or 10.0),
        uncertainty=float(uncertainty or 0.0),
        effective_sample_size=float(effective_sample_size or 0.0),
        atr_pct=float(candidate.atr_pct or 0.02),
        current_price=float(candidate.current_price or 0.0),
        expected_horizon_days=int(candidate.expected_horizon_days or 5),
        regime_code=str(diagnostics.get("query", {}).get("regime_code") or "UNKNOWN"),
        sector_code=str(diagnostics.get("query", {}).get("sector_code") or "UNKNOWN"),
        decision_surface_summary=surface or {},
        contract_missing_reasons=tuple(dict.fromkeys(contract_missing_reasons)),
        payload_sources={
            "calibrated_ev": calibrated_ev_source,
            "q10_return": q10_source,
            "q50_return": q50_source,
            "q90_return": q90_source,
            "expected_mae": expected_mae_source,
            "expected_mfe": expected_mfe_source,
            "effective_sample_size": ess_source,
            "uncertainty": uncertainty_source,
            "fill_probability_proxy": fill_source,
        },
    )


def baseline_gap_policy(candidate: SignalCandidate) -> QuotePolicyDecision:
    atr_pct = float(candidate.atr_pct or 0.05)
    required = max(0.012, 0.4 * atr_pct)
    return QuotePolicyDecision(policy_name="gap_policy_baseline_v0", buy_gap=required, sell_gap=required, size_multiplier=1.0, no_trade=False, diagnostics={"required_gap": required, "atr_pct": atr_pct, "chosen_policy_reason": "baseline_static_gap"})


def _expected_fill_probability(fill_proxy: float, gap: float, atr_pct: float, uncertainty: float) -> float:
    gap_pressure = gap / max(atr_pct, 0.002)
    return max(0.01, min(0.99, fill_proxy * (1.0 / (1.0 + 1.6 * gap_pressure)) * (1.0 - 0.35 * min(uncertainty, 0.8))))


def _utility_for_candidate(policy_input: QuotePolicyInput, *, gap: float, size_multiplier: float) -> dict:
    fill_prob = _expected_fill_probability(policy_input.fill_probability_proxy, gap, policy_input.atr_pct, policy_input.uncertainty)
    cost_pct = policy_input.cost_bps / 10000.0
    adverse_gap_penalty = max(0.0, gap - max(policy_input.expected_mfe * 0.35, 0.0)) * 0.35
    retained_edge = policy_input.q50_return - gap - cost_pct - adverse_gap_penalty
    downside_penalty = abs(min(policy_input.q10_return, 0.0)) + 0.50 * max(policy_input.expected_mae, 0.0)
    upside_credit = max(policy_input.q90_return, 0.0) + 0.25 * max(policy_input.expected_mfe, 0.0)
    execution_utility = fill_prob * (retained_edge - 0.60 * downside_penalty + 0.30 * upside_credit)
    lower_return_proxy = 0.60 * policy_input.q50_return + 0.40 * policy_input.q10_return
    confidence_bound = fill_prob * (lower_return_proxy + 0.35 * max(policy_input.expected_mfe, 0.0) - 0.35 * max(policy_input.expected_mae, 0.0) - gap - cost_pct) - 0.35 * policy_input.uncertainty
    effective_scale = min(1.5, max(policy_input.effective_sample_size, 0.25) / 2.0)
    utility = size_multiplier * effective_scale * execution_utility - 0.10 * size_multiplier * policy_input.uncertainty
    return {
        "gap": gap,
        "size_multiplier": size_multiplier,
        "fill_probability": fill_prob,
        "retained_edge": retained_edge,
        "execution_utility": execution_utility,
        "confidence_bound": confidence_bound,
        "utility": utility,
        "effective_scale": effective_scale,
    }


def optimize_quote_policy(policy_input: QuotePolicyInput, cfg: QuotePolicyConfig | None = None) -> QuotePolicyDecision:
    cfg = cfg or QuotePolicyConfig()
    candidates: list[dict] = []
    for gap in cfg.gap_grid:
        for size_multiplier in cfg.size_grid:
            candidates.append(_utility_for_candidate(policy_input, gap=gap, size_multiplier=size_multiplier))
    candidates.sort(key=lambda row: (row["utility"], row["confidence_bound"], row["fill_probability"]), reverse=True)
    best = candidates[0] if candidates else {"gap": max(0.012, 0.4 * policy_input.atr_pct), "size_multiplier": 0.0, "fill_probability": 0.0, "confidence_bound": -1.0, "utility": -1.0, "retained_edge": 0.0, "execution_utility": 0.0, "effective_scale": 0.0}
    reasons = list(policy_input.contract_missing_reasons)
    if policy_input.calibrated_ev <= cfg.ev_threshold:
        reasons.append("utility_non_positive")
    if best["utility"] <= 0.0:
        reasons.append("utility_non_positive")
    if best["confidence_bound"] <= 0.0:
        reasons.append("confidence_bound_non_positive")
    if policy_input.effective_sample_size < cfg.min_effective_sample_size:
        reasons.append("low_effective_sample_size")
    if policy_input.uncertainty > cfg.uncertainty_cap:
        reasons.append("high_uncertainty")
    if best["fill_probability"] < cfg.min_fill_probability:
        reasons.append("low_fill_probability")
    reasons = list(dict.fromkeys(reasons))
    no_trade = bool(reasons)
    chosen_gap = float(best["gap"])
    chosen_size = 0.0 if no_trade else float(best["size_multiplier"])
    policy_reason = "optimize_expected_fill_utility" if not no_trade else ",".join(reasons)
    kill_reason_hint = None
    if policy_input.contract_missing_reasons:
        kill_reason_hint = str(policy_input.contract_missing_reasons[0])
    elif no_trade:
        kill_reason_hint = "quote_policy_no_trade"
    return QuotePolicyDecision(
        policy_name="quote_policy_v1",
        buy_gap=chosen_gap if policy_input.side == Side.BUY else max(0.002, min(policy_input.atr_pct * 0.25, chosen_gap * 0.8 + 0.002)),
        sell_gap=chosen_gap if policy_input.side == Side.SELL else max(0.002, min(policy_input.atr_pct * 0.25, chosen_gap * 0.8 + 0.002)),
        size_multiplier=chosen_size,
        no_trade=no_trade,
        diagnostics={
            "calibrated_ev": policy_input.calibrated_ev,
            "q10_return": policy_input.q10_return,
            "q50_return": policy_input.q50_return,
            "q90_return": policy_input.q90_return,
            "expected_mae": policy_input.expected_mae,
            "expected_mfe": policy_input.expected_mfe,
            "fill_probability_proxy": policy_input.fill_probability_proxy,
            "cost_bps": policy_input.cost_bps,
            "uncertainty": policy_input.uncertainty,
            "effective_sample_size": policy_input.effective_sample_size,
            "atr_pct": policy_input.atr_pct,
            "regime_code": policy_input.regime_code,
            "sector_code": policy_input.sector_code,
            "decision_surface_summary": policy_input.decision_surface_summary or {},
            "contract_missing_reasons": list(policy_input.contract_missing_reasons),
            "payload_sources": policy_input.payload_sources or {},
            "kill_reason_hint": kill_reason_hint,
            "chosen_policy_reason": policy_reason,
            "optimizer_best": best,
            "optimizer_top3": candidates[:3],
        },
    )


def quote_policy_v1(policy_input: QuotePolicyInput, cfg: QuotePolicyConfig | None = None) -> QuotePolicyDecision:
    return optimize_quote_policy(policy_input, cfg)


def compare_policy_ab(candidate: SignalCandidate, cfg: QuotePolicyConfig | None = None) -> dict:
    baseline = baseline_gap_policy(candidate)
    v1 = quote_policy_v1(signal_to_policy_input(candidate), cfg)
    return {
        "baseline": baseline.diagnostics | {"policy_name": baseline.policy_name, "buy_gap": baseline.buy_gap, "sell_gap": baseline.sell_gap, "size_multiplier": baseline.size_multiplier, "no_trade": baseline.no_trade},
        "quote_policy_v1": v1.diagnostics | {"policy_name": v1.policy_name, "buy_gap": v1.buy_gap, "sell_gap": v1.sell_gap, "size_multiplier": v1.size_multiplier, "no_trade": v1.no_trade},
    }
