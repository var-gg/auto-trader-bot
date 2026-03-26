from datetime import date

from backtest_app.portfolio import PortfolioConfig, build_portfolio_decisions
from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate


def _cand(symbol, side, ev, conf, atr, sector, regime="RISK_ON"):
    return SignalCandidate(
        symbol=symbol,
        ticker_id=1,
        market=MarketCode.US,
        side_bias=side,
        signal_strength=ev,
        confidence=conf,
        anchor_date=date(2026, 1, 1),
        reference_date=date(2026, 1, 1),
        current_price=100.0,
        atr_pct=atr,
        target_return_pct=0.04,
        max_reverse_pct=0.03,
        expected_horizon_days=5,
        outcome_label=OutcomeLabel.UNKNOWN,
        diagnostics={"query": {"sector_code": sector, "regime_code": regime}, "ev": {"long": {"calibrated_ev": ev, "uncertainty": 0.02, "abstain_reasons": []}, "short": {"calibrated_ev": ev, "uncertainty": 0.02, "abstain_reasons": []}}},
    )


def test_portfolio_layer_selects_top_ev_with_caps_and_sizing():
    candidates = [
        _cand("AAPL", Side.BUY, 0.12, 0.9, 0.02, "TECH"),
        _cand("MSFT", Side.BUY, 0.11, 0.8, 0.02, "TECH"),
        _cand("XOM", Side.BUY, 0.10, 0.7, 0.03, "ENERGY"),
        _cand("CVX", Side.BUY, 0.09, 0.6, 0.03, "ENERGY"),
    ]
    decisions = build_portfolio_decisions(candidates=candidates, initial_capital=10000.0, cfg=PortfolioConfig(top_n=3, max_sector_positions=1, max_correlated_group_positions=1))
    selected = [d for d in decisions if d.selected]
    assert len(selected) == 2
    assert selected[0].requested_budget > 0
    assert any(d.kill_reason == "sector_cap" for d in decisions if not d.selected)
