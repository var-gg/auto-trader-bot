from backtest_app.research.models import StatePrototype
from backtest_app.research.repository import ExactCosineCandidateIndex
from backtest_app.research.scoring import CalibrationModel, EVConfig, apply_calibration_to_test, build_decision_surface, estimate_expected_value, fit_calibration, fit_calibration_on_fold
from backtest_app.validation import rejection_reasons


def _side_stats(*, target, stop, flat, ambiguous, no_trade, hup, hdown, ret, mae=0.01, mfe=0.06, unc=0.01, disp=0.02, supp=5, decayed=2.5, fresh=5):
    total = target + stop + flat + ambiguous + no_trade
    return {"support_count": supp, "decayed_support": decayed, "mean_return_pct": ret, "median_return_pct": ret, "mae_mean_pct": mae, "mfe_mean_pct": mfe, "return_q10_pct": ret - 0.02, "return_q50_pct": ret, "return_q90_pct": ret + 0.02, "return_dispersion": disp, "uncertainty": unc, "freshness_days": fresh, "target_first_count": target, "stop_first_count": stop, "flat_count": flat, "ambiguous_count": ambiguous, "no_trade_count": no_trade, "horizon_up_count": hup, "horizon_down_count": hdown, "p_target_first": target / total, "p_stop_first": stop / total, "p_flat": flat / total, "p_ambiguous": ambiguous / total, "p_no_trade": no_trade / total}


def _proto(pid, emb, *, regime="RISK_ON", sector="TECH", buy=None, sell=None, supp=5, decayed=2.5, fresh=5):
    buy = buy or _side_stats(target=4, stop=1, flat=0, ambiguous=0, no_trade=0, hup=3, hdown=1, ret=0.08, supp=supp, decayed=decayed, fresh=fresh)
    sell = sell or _side_stats(target=1, stop=4, flat=0, ambiguous=0, no_trade=0, hup=1, hdown=3, ret=-0.02, supp=supp, decayed=decayed, fresh=fresh)
    return StatePrototype(prototype_id=pid, anchor_code="SIM_V2", embedding=emb, member_count=supp, representative_symbol="AAPL", representative_date="2026-01-01", representative_hash=f"hash-{pid}", anchor_quality=0.8, regime_code=regime, sector_code=sector, liquidity_score=0.9, support_count=supp, decayed_support=decayed, freshness_days=fresh, side_stats={"BUY": buy, "SELL": sell}, metadata={"prior_buckets": {"regime": [regime], "sector": [sector], "liquidity": ["HIGH"]}})


def test_fit_calibration_is_monotonic():
    model = fit_calibration(scores=[0.1, 0.2, 0.3, 0.4], targets=[0, 0, 1, 1])
    assert model.calibrate_prob(0.4) >= model.calibrate_prob(0.2)


def test_fold_calibration_uses_train_only_and_applies_to_test_only():
    fold = fit_calibration_on_fold(fold_id="wf-1", raw_scores=[0.1, 0.2, 0.8, 0.9], targets=[0, 0, 1, 1], train_indices=[0, 1], test_indices=[2, 3])
    applied = apply_calibration_to_test(raw_scores=[0.1, 0.2, 0.8, 0.9], raw_probs=[0.2, 0.3, 0.7, 0.8], fold=fold)
    assert applied["artifact"]["train_size"] == 2
    assert applied["artifact"]["test_size"] == 2
    assert [row["index"] for row in applied["calibrated_scores"]] == [2, 3]


def test_estimate_expected_value_prefers_higher_ev_side():
    pool = [_proto("p1", [1.0, 0.0]), _proto("p2", [0.95, 0.05], buy=_side_stats(target=3, stop=1, flat=1, ambiguous=0, no_trade=0, hup=2, hdown=1, ret=0.05))]
    calib = CalibrationModel(method="logistic", slope=2.0, intercept=0.0)
    long_ev = estimate_expected_value(side="BUY", query_embedding=[1.0, 0.0], candidates=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2), candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    short_ev = estimate_expected_value(side="SELL", query_embedding=[1.0, 0.0], candidates=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2), candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2), candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    assert long_ev.calibrated_ev > short_ev.calibrated_ev
    assert surface.buy.utility["p_target_first"] > surface.sell.utility["p_target_first"]
    assert surface.chosen_side == "BUY"


def test_decision_surface_prefers_sell_when_sell_distribution_better():
    pool = [_proto("p1", [1.0, 0.0], buy=_side_stats(target=1, stop=4, flat=0, ambiguous=0, no_trade=0, hup=0, hdown=3, ret=-0.01), sell=_side_stats(target=4, stop=1, flat=0, ambiguous=0, no_trade=0, hup=3, hdown=0, ret=0.06))]
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2, min_effective_sample_size=1.0), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    assert surface.sell.expected_net_return > surface.buy.expected_net_return
    assert surface.chosen_side == "SELL"


def test_estimate_expected_value_abstains_on_ambiguous_or_no_trade_share():
    protos = [_proto("p1", [1.0, 0.0], buy=_side_stats(target=1, stop=1, flat=0, ambiguous=2, no_trade=2, hup=1, hdown=1, ret=0.001, unc=0.04))]
    ev = estimate_expected_value(side="BUY", query_embedding=[1.0, 0.0], candidates=protos, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=1, min_expected_utility=0.01, max_uncertainty=0.05, min_effective_sample_size=2.0), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=protos, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=1, min_expected_utility=0.01, max_uncertainty=0.05, min_effective_sample_size=2.0), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    assert ev.abstained is True
    assert surface.abstain is True
    assert "high_ambiguous_share" in surface.abstain_reasons or "high_no_trade_share" in surface.abstain_reasons


def test_decision_surface_preserves_nonzero_flat_probability():
    pool = [_proto("p1", [1.0, 0.0], buy=_side_stats(target=1, stop=1, flat=3, ambiguous=0, no_trade=0, hup=1, hdown=1, ret=0.005), sell=_side_stats(target=1, stop=1, flat=2, ambiguous=0, no_trade=1, hup=1, hdown=1, ret=0.0))]
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=1, min_effective_sample_size=1.0, max_uncertainty=0.2), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    assert surface.buy.p_flat > 0.0
    assert surface.sell.p_flat > 0.0


def test_rejection_reasons_uses_fold_calibration_contract_labels():
    reasons = rejection_reasons({"expectancy_after_cost": -0.01, "psr": 0.4, "dsr": 0.4, "score_decile_monotonicity": False, "calibration_error": 0.4})
    assert "non_positive_expectancy" in reasons
    assert "low_psr_or_dsr" in reasons
    assert "non_monotonic_score_buckets" in reasons
    assert "high_calibration_error" in reasons
