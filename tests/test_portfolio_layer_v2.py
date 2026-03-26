from datetime import date

from backtest_app.portfolio import PortfolioConfig, build_portfolio_decisions
from shared.domain.models import MarketCode, OutcomeLabel, Side, SignalCandidate


def _cand(symbol, side, ev, conf, atr, sector, regime="RISK_ON", overlap=0.0, unc=0.02, fill=0.7):
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
        diagnostics={
            "query": {"sector_code": sector, "regime_code": regime, "overlap_penalty": overlap, "estimated_cost_bps": 10.0},
            "ev": {"long": {"calibrated_ev": ev, "expected_mae": 0.01, "expected_mfe": 0.03, "uncertainty": unc, "effective_sample_size": 3.0, "abstain_reasons": []}, "short": {"calibrated_ev": ev, "expected_mae": 0.01, "expected_mfe": 0.03, "uncertainty": unc, "effective_sample_size": 3.0, "abstain_reasons": []}},
            "decision_surface": {"buy": {"q10": -0.01, "q50": ev, "q90": ev + 0.03, "expected_mae": 0.01, "expected_mfe": 0.03, "p_target_first": fill, "effective_sample_size": 3.0, "uncertainty": unc}, "sell": {"q10": -0.02, "q50": 0.0, "q90": 0.01, "expected_mae": 0.02, "expected_mfe": 0.01, "p_target_first": 0.3, "effective_sample_size": 3.0, "uncertainty": unc}},
        },
    )


def test_portfolio_layer_selects_top_utility_with_caps_and_sizing():
    candidates = [
        _cand("AAPL", Side.BUY, 0.12, 0.9, 0.02, "TECH", overlap=0.0, fill=0.85),
        _cand("MSFT", Side.BUY, 0.11, 0.8, 0.02, "TECH", overlap=0.4, fill=0.50),
        _cand("XOM", Side.BUY, 0.10, 0.7, 0.03, "ENERGY", overlap=0.0, fill=0.80),
        _cand("CVX", Side.BUY, 0.09, 0.6, 0.03, "ENERGY", overlap=0.2, fill=0.35, unc=0.10),
    ]
    decisions = build_portfolio_decisions(candidates=candidates, initial_capital=10000.0, cfg=PortfolioConfig(top_n=3, max_sector_positions=1, max_correlated_group_positions=1))
    selected = [d for d in decisions if d.selected]
    assert len(selected) == 2
    assert selected[0].requested_budget > 0
    assert "quote_policy" in selected[0].diagnostics
    assert "ranking_utility" in selected[0].diagnostics
    assert any(d.kill_reason == "sector_cap" for d in decisions if not d.selected)


def test_portfolio_ranking_penalizes_overlap_and_low_confidence_bound():
    candidates = [
        _cand("AAA", Side.BUY, 0.10, 0.9, 0.02, "TECH", overlap=0.0, fill=0.8),
        _cand("BBB", Side.BUY, 0.11, 0.9, 0.02, "ENERGY", overlap=0.8, fill=0.2, unc=0.12),
    ]
    decisions = build_portfolio_decisions(candidates=candidates, initial_capital=10000.0, cfg=PortfolioConfig(top_n=2, max_sector_positions=2, max_correlated_group_positions=2))
    by_symbol = {d.candidate.symbol: d for d in decisions}
    assert by_symbol["AAA"].diagnostics["ranking_utility"] >= by_symbol["BBB"].diagnostics["ranking_utility"]
