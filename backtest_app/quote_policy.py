from __future__ import annotations

from dataclasses import dataclass

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


def signal_to_policy_input(candidate: SignalCandidate) -> QuotePolicyInput:
    diagnostics = candidate.diagnostics or {} if isinstance(candidate.diagnostics, dict) else {}
    side_key = "long" if candidate.side_bias == Side.BUY else "short"
    ev = diagnostics.get("ev", {}) if isinstance(diagnostics, dict) else {}
    row = ev.get(side_key, {}) if isinstance(ev, dict) else {}
    surface = diagnostics.get("decision_surface", {}) if isinstance(diagnostics, dict) else {}
    surface_side = (surface.get("buy") if candidate.side_bias == Side.BUY else surface.get("sell")) or {}
    fill_proxy = float(surface_side.get("p_target_first", 0.0) or row.get("calibrated_win_prob", 0.0) or candidate.confidence or 0.0)
    return QuotePolicyInput(
        side=candidate.side_bias,
        calibrated_ev=float(row.get("calibrated_ev", surface_side.get("expected_net_return", candidate.signal_strength)) or 0.0),
        q10_return=float(surface_side.get("q10", row.get("q10_return", 0.0)) or 0.0),
        q50_return=float(surface_side.get("q50", row.get("q50_return", 0.0)) or 0.0),
        q90_return=float(surface_side.get("q90", row.get("q90_return", candidate.signal_strength)) or 0.0),
        expected_mae=float(surface_side.get("expected_mae", row.get("expected_mae", 0.0)) or 0.0),
        expected_mfe=float(surface_side.get("expected_mfe", row.get("expected_mfe", 0.0)) or 0.0),
        fill_probability_proxy=fill_proxy,
        cost_bps=float(diagnostics.get("query", {}).get("estimated_cost_bps", 10.0) or 10.0),
        uncertainty=float(surface_side.get("uncertainty", row.get("uncertainty", 0.0)) or 0.0),
        effective_sample_size=float(surface_side.get("effective_sample_size", row.get("effective_sample_size", 0.0)) or 0.0),
        atr_pct=float(candidate.atr_pct or 0.02),
        current_price=float(candidate.current_price or 0.0),
        expected_horizon_days=int(candidate.expected_horizon_days or 5),
        regime_code=str(diagnostics.get("query", {}).get("regime_code") or "UNKNOWN"),
        sector_code=str(diagnostics.get("query", {}).get("sector_code") or "UNKNOWN"),
        decision_surface_summary=surface or {},
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
    reasons = []
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
    no_trade = bool(reasons)
    chosen_gap = float(best["gap"])
    chosen_size = 0.0 if no_trade else float(best["size_multiplier"])
    policy_reason = "optimize_expected_fill_utility" if not no_trade else ",".join(reasons)
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
