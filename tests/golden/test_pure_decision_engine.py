from __future__ import annotations

from datetime import datetime, timezone

from shared.domain.execution import allocate_symbol_budgets, build_order_plan_from_candidate, classify_unfilled_reason, label_outcome_from_pnl_bps
from shared.domain.models import ExecutionVenue, MarketCode, OutcomeLabel, Side, SignalCandidate
from shared.domain.signals import compute_pm_signal, normalize_ranked_candidates

from tests.golden.golden_utils import assert_matches_golden, load_fixture


def _to_signal_candidates(rows, side: Side):
    return normalize_ranked_candidates(rows, market=MarketCode.US, side=side)


def test_domain_pm_signal_matches_golden():
    fixture = load_fixture("pm_signal_fixture.json")
    up = _to_signal_candidates(fixture["up_ranked"], Side.BUY)
    down = _to_signal_candidates(fixture["dn_ranked"], Side.SELL)
    signal_1d, best_target, reason = compute_pm_signal(up, down, fixture["q_ctx"], fixture["config"])
    actual = {
        "signal_1d": signal_1d,
        "best_target": best_target,
        "reason": reason,
        "up_count": len(up),
        "down_count": len(down),
    }
    assert_matches_golden("pm_signal_v1.golden.json", actual)


def test_domain_allocate_and_plan_matches_open_golden():
    fixture = load_fixture("pm_open_candidates.json")
    candidates = [
        SignalCandidate(
            symbol=row["symbol"],
            ticker_id=row["ticker_id"],
            market=MarketCode.US,
            side_bias=Side.BUY,
            signal_strength=float(row["signal_strength"]),
            current_price=float(row["current_price"]),
            atr_pct=float(row["atr_pct"]),
            outcome_label=OutcomeLabel(row["tb_label"]),
            reverse_breach_day=row.get("reverse_breach_day"),
            provenance={"has_long_recommendation": row.get("has_long_recommendation", False)},
            diagnostics={"iae_1_3": row.get("iae_1_3")},
        )
        for row in fixture["candidates"]
    ]
    tuning = {
        "SOFT_CAP_MULT": 1.5,
        "MAX_SYMBOL_WEIGHT": 0.30,
        "MIN_LADDER_LEGS": 3,
        "GRANULARITY_PENALTY_POW": 2.0,
        "RP_ALPHA": 1.0,
        "RP_BETA": 1.0,
    }
    selected, budget_map, _skipped = allocate_symbol_budgets(candidates, fixture["caps"]["swing_cap_cash"], "US", tuning)
    actual = {"selected_symbols": [c.symbol for c in selected], "budget_map": {str(k): v for k, v in budget_map.items()}, "ladder_shapes": {}}
    for c in selected:
        plan, skip = build_order_plan_from_candidate(
            c,
            generated_at=datetime(2026, 3, 24, 0, 0, tzinfo=timezone.utc),
            market="US",
            side=Side.BUY,
            tuning=fixture["ladder_params"]["buy"],
            budget=budget_map[c.ticker_id],
            venue=ExecutionVenue.BACKTEST,
            rationale_prefix="domain-plan",
        )
        assert skip is None
        actual["ladder_shapes"][c.symbol] = {
            "legs": len(plan.legs),
            "quantities": [leg.quantity for leg in plan.legs],
            "prices": [leg.limit_price for leg in plan.legs],
        }
    assert_matches_golden("pm_open_plan.golden.json", actual)


def test_domain_outcome_helpers_are_pure():
    assert label_outcome_from_pnl_bps(10.0) == OutcomeLabel.WIN
    assert label_outcome_from_pnl_bps(-10.0) == OutcomeLabel.LOSS
    assert label_outcome_from_pnl_bps(0.0) == OutcomeLabel.FLAT
    reason = classify_unfilled_reason("OPSQ2001", "주문가능수량이 부족합니다.")
    assert reason == {"reason_code": "OPSQ2001", "reason_text": "주문가능수량이 부족합니다."}
