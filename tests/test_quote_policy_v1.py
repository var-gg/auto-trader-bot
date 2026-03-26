from datetime import date

from backtest_app.quote_policy import compare_policy_ab, quote_policy_v1, signal_to_policy_input
from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate


def _cand(ev=0.02, mae=0.01, mfe=0.03, unc=0.02, neff=3.0, atr=0.02):
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
        diagnostics={"ev": {"long": {"calibrated_ev": ev, "expected_mae": mae, "expected_mfe": mfe, "uncertainty": unc, "effective_sample_size": neff}}},
    )


def test_quote_policy_v1_translates_signal_outputs():
    decision = quote_policy_v1(signal_to_policy_input(_cand()))
    assert decision.buy_gap > 0.0
    assert decision.sell_gap > 0.0
    assert decision.size_multiplier > 0.0
    assert decision.no_trade is False


def test_quote_policy_ab_contains_baseline_and_v1():
    ab = compare_policy_ab(_cand())
    assert "baseline" in ab
    assert "quote_policy_v1" in ab
    assert ab["baseline"]["policy_name"] != ab["quote_policy_v1"]["policy_name"]


def test_quote_policy_v1_blocks_low_ev_high_uncertainty():
    decision = quote_policy_v1(signal_to_policy_input(_cand(ev=0.001, unc=0.3, neff=1.0)))
    assert decision.no_trade is True
    assert decision.size_multiplier == 0.0
