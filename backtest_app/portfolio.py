from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from shared.domain.models import SignalCandidate, Side

from backtest_app.quote_policy import quote_policy_v1, signal_to_policy_input


@dataclass(frozen=True)
class PortfolioConfig:
    top_n: int = 5
    min_ev_threshold: float = 0.0
    risk_budget_fraction: float = 0.95
    max_sector_positions: int = 2
    max_correlated_group_positions: int = 1
    max_turnover_names: int = 5
    base_risk_unit: float = 1.0


@dataclass(frozen=True)
class PortfolioDecision:
    candidate: SignalCandidate
    selected: bool
    side: Side
    size_multiplier: float
    requested_budget: float
    expected_horizon_days: int
    kill_reason: str | None
    diagnostics: dict


def _candidate_ev(candidate: SignalCandidate) -> float:
    ev = candidate.diagnostics.get("ev", {}) if isinstance(candidate.diagnostics, dict) else {}
    side_key = "long" if candidate.side_bias == Side.BUY else "short"
    return float(((ev.get(side_key) or {}).get("calibrated_ev", candidate.signal_strength)) or 0.0)


def _candidate_uncertainty(candidate: SignalCandidate) -> float:
    ev = candidate.diagnostics.get("ev", {}) if isinstance(candidate.diagnostics, dict) else {}
    side_key = "long" if candidate.side_bias == Side.BUY else "short"
    return float(((ev.get(side_key) or {}).get("uncertainty", 0.0)) or 0.0)


def _candidate_regime(candidate: SignalCandidate) -> str:
    query = ((candidate.diagnostics or {}).get("query") or {}) if isinstance(candidate.diagnostics, dict) else {}
    return str(query.get("regime_code") or "UNKNOWN")


def _candidate_sector(candidate: SignalCandidate) -> str:
    query = ((candidate.diagnostics or {}).get("query") or {}) if isinstance(candidate.diagnostics, dict) else {}
    return str(query.get("sector_code") or "UNKNOWN")


def _correlation_bucket(candidate: SignalCandidate) -> str:
    sector = _candidate_sector(candidate)
    return f"{candidate.side_bias.value}:{sector}"


def rank_candidates_cross_sectional(candidates: Sequence[SignalCandidate]) -> list[SignalCandidate]:
    return sorted(list(candidates), key=lambda c: (_candidate_ev(c), c.confidence or 0.0), reverse=True)


def build_portfolio_decisions(*, candidates: Sequence[SignalCandidate], initial_capital: float, cfg: PortfolioConfig | None = None) -> list[PortfolioDecision]:
    cfg = cfg or PortfolioConfig()
    ranked = rank_candidates_cross_sectional(candidates)
    selected: list[PortfolioDecision] = []
    sector_counts: Dict[str, int] = {}
    corr_counts: Dict[str, int] = {}
    turnover_used = 0
    risk_budget_total = initial_capital * cfg.risk_budget_fraction

    for cand in ranked:
        ev = _candidate_ev(cand)
        uncertainty = _candidate_uncertainty(cand)
        sector = _candidate_sector(cand)
        corr = _correlation_bucket(cand)
        volatility_scale = 1.0 / max(float(cand.atr_pct or 0.02), 0.01)
        confidence_scale = max(0.1, min(1.5, float(cand.confidence or 0.0)))
        uncertainty_scale = max(0.1, 1.0 - uncertainty)
        raw_size = cfg.base_risk_unit * volatility_scale * confidence_scale * uncertainty_scale
        policy = quote_policy_v1(signal_to_policy_input(cand))
        size_multiplier = max(0.0, min(2.0, policy.size_multiplier if not policy.no_trade else 0.0))
        requested_budget = risk_budget_total / max(cfg.top_n, 1) * size_multiplier
        kill_reason = None
        if policy.no_trade:
            kill_reason = "quote_policy_no_trade"
        elif ev < cfg.min_ev_threshold:
            kill_reason = "below_ev_threshold"
        elif sector_counts.get(sector, 0) >= cfg.max_sector_positions:
            kill_reason = "sector_cap"
        elif corr_counts.get(corr, 0) >= cfg.max_correlated_group_positions:
            kill_reason = "correlated_cap"
        elif turnover_used >= cfg.max_turnover_names:
            kill_reason = "turnover_budget"
        elif len([d for d in selected if d.selected]) >= cfg.top_n:
            kill_reason = "top_n_limit"

        decision = PortfolioDecision(
            candidate=cand,
            selected=kill_reason is None,
            side=cand.side_bias,
            size_multiplier=size_multiplier,
            requested_budget=requested_budget if kill_reason is None else 0.0,
            expected_horizon_days=int(cand.expected_horizon_days or 5),
            kill_reason=kill_reason,
            diagnostics={
                "calibrated_ev": ev,
                "uncertainty": uncertainty,
                "sector_code": sector,
                "regime_code": _candidate_regime(cand),
                "correlation_bucket": corr,
                "volatility_scale": volatility_scale,
                "confidence_scale": confidence_scale,
                "uncertainty_scale": uncertainty_scale,
                "raw_size": raw_size,
                "size_multiplier": size_multiplier,
                "requested_budget": requested_budget,
                "quote_policy": policy.diagnostics | {"policy_name": policy.policy_name, "buy_gap": policy.buy_gap, "sell_gap": policy.sell_gap, "size_multiplier": policy.size_multiplier, "no_trade": policy.no_trade},
            },
        )
        selected.append(decision)
        if kill_reason is None:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            corr_counts[corr] = corr_counts.get(corr, 0) + 1
            turnover_used += 1
    return selected
