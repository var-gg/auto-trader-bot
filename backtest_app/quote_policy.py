from __future__ import annotations

from dataclasses import dataclass

from shared.domain.models import Side, SignalCandidate


@dataclass(frozen=True)
class QuotePolicyInput:
    side: Side
    calibrated_ev: float
    expected_mae: float
    expected_mfe: float
    uncertainty: float
    effective_sample_size: float
    atr_pct: float
    current_price: float


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
    buy_gap_ev_coef: float = 0.35
    buy_gap_mae_coef: float = 0.80
    sell_gap_ev_coef: float = 0.35
    sell_gap_mfe_coef: float = 0.30
    uncertainty_cap: float = 0.12


def signal_to_policy_input(candidate: SignalCandidate) -> QuotePolicyInput:
    ev = (candidate.diagnostics or {}).get("ev", {}) if isinstance(candidate.diagnostics, dict) else {}
    side_key = "long" if candidate.side_bias == Side.BUY else "short"
    row = ev.get(side_key, {}) if isinstance(ev, dict) else {}
    return QuotePolicyInput(
        side=candidate.side_bias,
        calibrated_ev=float(row.get("calibrated_ev", candidate.signal_strength) or 0.0),
        expected_mae=float(row.get("expected_mae", 0.0) or 0.0),
        expected_mfe=float(row.get("expected_mfe", 0.0) or 0.0),
        uncertainty=float(row.get("uncertainty", 0.0) or 0.0),
        effective_sample_size=float(row.get("effective_sample_size", 0.0) or 0.0),
        atr_pct=float(candidate.atr_pct or 0.02),
        current_price=float(candidate.current_price or 0.0),
    )


def baseline_gap_policy(candidate: SignalCandidate) -> QuotePolicyDecision:
    atr_pct = float(candidate.atr_pct or 0.05)
    required = max(0.012, 0.4 * atr_pct)
    return QuotePolicyDecision(
        policy_name="gap_policy_baseline_v0",
        buy_gap=required,
        sell_gap=required,
        size_multiplier=1.0,
        no_trade=False,
        diagnostics={"required_gap": required, "atr_pct": atr_pct},
    )


def quote_policy_v1(policy_input: QuotePolicyInput, cfg: QuotePolicyConfig | None = None) -> QuotePolicyDecision:
    cfg = cfg or QuotePolicyConfig()
    no_trade = policy_input.calibrated_ev < cfg.ev_threshold or policy_input.uncertainty > cfg.uncertainty_cap or policy_input.effective_sample_size < 1.5
    buy_gap = max(0.002, min(0.05, policy_input.atr_pct * (cfg.buy_gap_mae_coef * max(policy_input.expected_mae, 0.0) + 0.25) - cfg.buy_gap_ev_coef * policy_input.calibrated_ev))
    sell_gap = max(0.002, min(0.05, policy_input.atr_pct * (0.20 + cfg.sell_gap_mfe_coef * max(policy_input.expected_mfe, 0.0)) - cfg.sell_gap_ev_coef * policy_input.calibrated_ev))
    size_multiplier = max(0.0, min(2.0, (policy_input.calibrated_ev / max(cfg.ev_threshold, 1e-6)) * (1.0 - min(policy_input.uncertainty, 0.9)) * min(1.5, max(policy_input.effective_sample_size, 0.5) / 2.0)))
    return QuotePolicyDecision(
        policy_name="quote_policy_v1",
        buy_gap=buy_gap,
        sell_gap=sell_gap,
        size_multiplier=0.0 if no_trade else size_multiplier,
        no_trade=no_trade,
        diagnostics={
            "calibrated_ev": policy_input.calibrated_ev,
            "expected_mae": policy_input.expected_mae,
            "expected_mfe": policy_input.expected_mfe,
            "uncertainty": policy_input.uncertainty,
            "effective_sample_size": policy_input.effective_sample_size,
            "atr_pct": policy_input.atr_pct,
        },
    )


def compare_policy_ab(candidate: SignalCandidate, cfg: QuotePolicyConfig | None = None) -> dict:
    baseline = baseline_gap_policy(candidate)
    v1 = quote_policy_v1(signal_to_policy_input(candidate), cfg)
    return {
        "baseline": baseline.diagnostics | {"policy_name": baseline.policy_name, "buy_gap": baseline.buy_gap, "sell_gap": baseline.sell_gap, "size_multiplier": baseline.size_multiplier, "no_trade": baseline.no_trade},
        "quote_policy_v1": v1.diagnostics | {"policy_name": v1.policy_name, "buy_gap": v1.buy_gap, "sell_gap": v1.sell_gap, "size_multiplier": v1.size_multiplier, "no_trade": v1.no_trade},
    }
