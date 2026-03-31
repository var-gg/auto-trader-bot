from datetime import date, datetime

from backtest_app.portfolio import build_portfolio_decisions
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


def _research_cand():
    return SignalCandidate(
        symbol="MSFT",
        ticker_id=2,
        market=MarketCode.US,
        side_bias=Side.SELL,
        signal_strength=0.03,
        confidence=0.65,
        anchor_date=date(2026, 1, 2),
        reference_date=date(2026, 1, 2),
        current_price=210.0,
        atr_pct=0.025,
        target_return_pct=0.04,
        max_reverse_pct=0.03,
        expected_horizon_days=5,
        outcome_label=OutcomeLabel.UNKNOWN,
        diagnostics={
            "query": {"regime_code": "RISK_OFF", "sector_code": "TECH", "estimated_cost_bps": 9.0},
            "decision_surface": {
                "chosen_side": "SELL",
                "decision_rule": {"winner": "SELL", "chosen_effective_sample_size": 4.5, "chosen_lower_bound": 0.012},
            },
            "chosen_side_payload": {
                "chosen_side": "SELL",
                "expected_net_return": 0.031,
                "q10_return": 0.012,
                "q50_return": 0.031,
                "q90_return": 0.064,
                "expected_mae": 0.008,
                "expected_mfe": 0.021,
                "effective_sample_size": 4.5,
                "uncertainty": 0.018,
                "fill_probability_proxy": 0.62,
            },
            "ev": {
                "buy": {"expected_net_return": -0.004, "effective_sample_size": 3.9, "uncertainty": 0.03},
                "sell": {"expected_net_return": 0.031, "effective_sample_size": 4.5, "uncertainty": 0.018},
            },
            "scorer_diagnostics": {
                "buy": {"q10": -0.03, "q50": -0.004, "q90": 0.01, "expected_mae": 0.012, "expected_mfe": 0.01, "n_eff": 3.9, "uncertainty": 0.03},
                "sell": {"q10": 0.012, "q50": 0.031, "q90": 0.064, "expected_mae": 0.008, "expected_mfe": 0.021, "n_eff": 4.5, "uncertainty": 0.018, "p_target": 0.62},
            },
        },
    )


def _broken_research_cand():
    return SignalCandidate(
        symbol="JPM",
        ticker_id=3,
        market=MarketCode.US,
        side_bias=Side.SELL,
        signal_strength=0.02,
        confidence=0.6,
        anchor_date=date(2026, 1, 3),
        reference_date=date(2026, 1, 3),
        current_price=150.0,
        atr_pct=0.02,
        target_return_pct=0.04,
        max_reverse_pct=0.03,
        expected_horizon_days=5,
        outcome_label=OutcomeLabel.UNKNOWN,
        diagnostics={
            "query": {"regime_code": "RISK_ON", "sector_code": "FIN", "estimated_cost_bps": 10.0},
            "decision_surface": {
                "chosen_side": "SELL",
                "decision_rule": {"winner": "SELL", "chosen_effective_sample_size": 4.75, "chosen_lower_bound": 0.028},
            },
            "scorer_diagnostics": {
                "sell": {"q90": 0.061, "expected_mae": 0.01, "expected_mfe": 0.02, "uncertainty": 0.015, "p_target": 0.55},
            },
            "ev": {
                "sell": {"expected_net_return": 0.029, "uncertainty": 0.015},
            },
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


def test_signal_to_policy_input_supports_research_style_payload():
    policy_input = signal_to_policy_input(_research_cand())
    assert policy_input.side == Side.SELL
    assert policy_input.q10_return == 0.012
    assert policy_input.q50_return == 0.031
    assert policy_input.q90_return == 0.064
    assert policy_input.effective_sample_size == 4.5
    assert policy_input.contract_missing_reasons == ()


def test_quote_policy_marks_contract_invalid_when_distribution_payload_is_missing():
    decision = quote_policy_v1(signal_to_policy_input(_broken_research_cand()))
    assert decision.no_trade is True
    assert "contract_invalid_missing_distribution" in decision.diagnostics["contract_missing_reasons"]
    assert "contract_invalid_missing_effective_sample_size" in decision.diagnostics["contract_missing_reasons"]
    assert decision.diagnostics["kill_reason_hint"] == "contract_invalid_missing_distribution"


def test_portfolio_surfaces_contract_invalid_kill_reason():
    decision = build_portfolio_decisions(candidates=[_broken_research_cand()], initial_capital=10000.0)[0]
    assert decision.selected is False
    assert decision.kill_reason == "contract_invalid_missing_distribution"
