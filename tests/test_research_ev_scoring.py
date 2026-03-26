from backtest_app.research.models import PrototypeAnchor
from backtest_app.research.repository import ExactCosineCandidateIndex
from backtest_app.research.scoring import CalibrationModel, EVConfig, apply_calibration_to_test, build_decision_surface, estimate_expected_value, fit_calibration, fit_calibration_on_fold
from backtest_app.validation import rejection_reasons


def _proto(pid, emb, *, side="BUY", regime="RISK_ON", sector="TECH", ret=0.05, win=0.7, mae=-0.01, mfe=0.06, unc=0.01, disp=0.02, supp=3, decayed=2.5, fresh=5):
    return PrototypeAnchor(prototype_id=pid, anchor_code="SIM_V2", side=side, embedding=emb, member_count=supp, representative_symbol="AAPL", representative_date="2026-01-01", anchor_quality=0.8, regime_code=regime, sector_code=sector, liquidity_score=0.9, support_count=supp, decayed_support=decayed, mean_return_pct=ret, median_return_pct=ret, win_rate=win, mae_mean_pct=mae, mfe_mean_pct=mfe, return_dispersion=disp, uncertainty=unc, freshness_days=fresh, regime_bucket=regime, sector_bucket=sector, liquidity_bucket="HIGH")


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
    longs = [_proto("p1", [1.0, 0.0], side="BUY", ret=0.08, win=0.8), _proto("p2", [0.95, 0.05], side="BUY", ret=0.05, win=0.7)]
    shorts = [_proto("p3", [1.0, 0.0], side="SELL", ret=0.01, win=0.55, mae=-0.03, unc=0.03)]
    calib = CalibrationModel(method="logistic", slope=2.0, intercept=0.0)
    long_ev = estimate_expected_value(side="BUY", query_embedding=[1.0, 0.0], candidates=longs, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2), candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    short_ev = estimate_expected_value(side="SELL", query_embedding=[1.0, 0.0], candidates=shorts, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2), candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    surface = build_decision_surface(query_embedding=[1.0, 0.0], buy_candidates=longs, sell_candidates=shorts, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2), candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    assert long_ev.calibrated_ev > short_ev.calibrated_ev
    assert surface.buy.expected_net_return > surface.sell.expected_net_return
    assert surface.chosen_side == "BUY"
    assert surface.buy.top_matches


def test_decision_surface_prefers_sell_when_sell_distribution_better():
    longs = [_proto("p1", [1.0, 0.0], side="BUY", ret=0.01, win=0.52, mae=-0.04, unc=0.02)]
    shorts = [_proto("p2", [1.0, 0.0], side="SELL", ret=0.06, win=0.75, mae=-0.01, mfe=0.05, unc=0.01)]
    surface = build_decision_surface(query_embedding=[1.0, 0.0], buy_candidates=longs, sell_candidates=shorts, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2, min_effective_sample_size=1.0), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    assert surface.sell.expected_net_return > surface.buy.expected_net_return
    assert surface.chosen_side == "SELL"


def test_estimate_expected_value_abstains_on_low_ev_or_low_neff():
    protos = [_proto("p1", [1.0, 0.0], side="BUY", ret=0.001, win=0.51, unc=0.2, supp=1, decayed=0.5)]
    ev = estimate_expected_value(side="BUY", query_embedding=[1.0, 0.0], candidates=protos, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=1, min_expected_utility=0.01, max_uncertainty=0.05, min_effective_sample_size=2.0), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    surface = build_decision_surface(query_embedding=[1.0, 0.0], buy_candidates=protos, sell_candidates=[], regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=1, min_expected_utility=0.01, max_uncertainty=0.05, min_effective_sample_size=2.0), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    assert ev.abstained is True
    assert surface.abstain is True


def test_rejection_reasons_uses_fold_calibration_contract_labels():
    reasons = rejection_reasons({"expectancy_after_cost": -0.01, "psr": 0.4, "dsr": 0.4, "score_decile_monotonicity": False, "calibration_error": 0.4})
    assert "non_positive_expectancy" in reasons
    assert "low_psr_or_dsr" in reasons
    assert "non_monotonic_score_buckets" in reasons
    assert "high_calibration_error" in reasons
