from datetime import date, datetime

from backtest_app.quote_policy import QuotePolicyInput, compare_policy_ab, quote_policy_v1, signal_to_policy_input
from shared.domain.execution.planning import build_order_plan_from_candidate
from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate


def _cand(ev=0.02, mae=0.01, mfe=0.03, unc=0.02, neff=3.0, atr=0.02, q10=-0.01, q50=0.02, q90=0.05, fill=0.7):
    return SignalCandidate(
        symbol="AAPL",
        ticker_id=1,
        market=MarketCode.US,
        side_bias=Side.BUY,
        signal_strength=ev,
        confidence=0.8,
        anchor_date=date(2026, 1, 1),
        reference_date=date(2026, 1, 1),
        current_price=100.0,
        atr_pct=atr,
        target_return_pct=0.04,
        max_reverse_pct=0.03,
        expected_horizon_days=5,
        outcome_label=OutcomeLabel.UNKNOWN,
        diagnostics={
            "query": {"regime_code": "RISK_ON", "sector_code": "TECH", "estimated_cost_bps": 10.0},
            "ev": {"long": {"calibrated_ev": ev, "expected_mae": mae, "expected_mfe": mfe, "uncertainty": unc, "effective_sample_size": neff}},
            "decision_surface": {"buy": {"q10": q10, "q50": q50, "q90": q90, "expected_mae": mae, "expected_mfe": mfe, "p_target_first": fill, "effective_sample_size": neff, "uncertainty": unc}},
        },
    )


def test_quote_policy_v1_translates_signal_outputs():
    decision = quote_policy_v1(signal_to_policy_input(_cand()))
    assert decision.buy_gap > 0.0
    assert decision.sell_gap > 0.0
    assert decision.size_multiplier > 0.0
    assert decision.no_trade is False
    assert decision.diagnostics["chosen_policy_reason"] == "optimize_expected_fill_utility"


def test_quote_policy_ab_contains_baseline_and_v1():
    ab = compare_policy_ab(_cand())
    assert "baseline" in ab
    assert "quote_policy_v1" in ab
    assert ab["baseline"]["policy_name"] != ab["quote_policy_v1"]["policy_name"]


def test_quote_policy_v1_blocks_low_ev_high_uncertainty_or_bad_bound():
    decision = quote_policy_v1(signal_to_policy_input(_cand(ev=0.001, unc=0.3, neff=1.0, q10=-0.04, q50=0.001, q90=0.01, fill=0.2)))
    assert decision.no_trade is True
    assert decision.size_multiplier == 0.0


def test_quote_policy_changes_gap_and_size_with_uncertainty_and_fill_proxy():
    strong_fill = quote_policy_v1(signal_to_policy_input(_cand(fill=0.85, unc=0.02, q50=0.03, q90=0.07)))
    weak_fill = quote_policy_v1(signal_to_policy_input(_cand(fill=0.20, unc=0.10, q50=0.03, q90=0.07)))
    assert strong_fill.size_multiplier >= weak_fill.size_multiplier
    assert strong_fill.buy_gap <= weak_fill.buy_gap or weak_fill.no_trade


def test_order_plan_metadata_keeps_policy_reason_and_surface_summary():
    candidate = _cand()
    policy = quote_policy_v1(signal_to_policy_input(candidate))
    plan, skip = build_order_plan_from_candidate(candidate, generated_at=datetime(2026, 1, 2, 0, 0, 0), market="US", side=Side.BUY, tuning={"MIN_TICK_GAP": 1, "ADAPTIVE_BASE_LEGS": 2, "ADAPTIVE_LEG_BOOST": 1.0, "MIN_TOTAL_SPREAD_PCT": 0.01, "ADAPTIVE_STRENGTH_SCALE": 0.1, "FIRST_LEG_BASE_PCT": 0.012, "FIRST_LEG_MIN_PCT": 0.006, "FIRST_LEG_MAX_PCT": 0.05, "FIRST_LEG_GAIN_WEIGHT": 0.6, "FIRST_LEG_ATR_WEIGHT": 0.5, "FIRST_LEG_REQ_FLOOR_PCT": 0.012, "MIN_FIRST_LEG_GAP_PCT": 0.03, "STRICT_MIN_FIRST_GAP": True, "ADAPTIVE_MAX_STEP_PCT": 0.06, "ADAPTIVE_FRAC_ALPHA": 1.25, "ADAPTIVE_GAIN_SCALE": 0.1, "MIN_LOT_QTY": 1}, budget=1000.0, quote_policy=policy.diagnostics | {"buy_gap": policy.buy_gap, "sell_gap": policy.sell_gap, "chosen_policy_reason": policy.diagnostics["chosen_policy_reason"]})
    assert skip is None
    assert plan is not None
    assert plan.metadata["chosen_policy_reason"]
    assert "decision_surface_summary" in plan.metadata
