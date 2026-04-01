from backtest_app.research.models import StatePrototype
from backtest_app.research.repository import ExactCosineCandidateIndex
from backtest_app.research.scoring import CalibrationModel, EVConfig, apply_calibration_to_test, build_decision_surface, estimate_expected_value, fit_calibration, fit_calibration_on_fold
from backtest_app.validation import rejection_reasons


def _side_stats(*, target, stop, flat, ambiguous, no_trade, hup, hdown, ret, mae=0.01, mfe=0.06, unc=0.01, disp=0.02, supp=5, decayed=2.5, fresh=5):
    total = target + stop + flat + ambiguous + no_trade
    return {"support_count": supp, "decayed_support": decayed, "mean_return_pct": ret, "median_return_pct": ret, "mae_mean_pct": mae, "mfe_mean_pct": mfe, "return_q10_pct": ret - 0.02, "return_q50_pct": ret, "return_q90_pct": ret + 0.02, "return_dispersion": disp, "uncertainty": unc, "freshness_days": fresh, "target_first_count": target, "stop_first_count": stop, "flat_count": flat, "ambiguous_count": ambiguous, "no_trade_count": no_trade, "horizon_up_count": hup, "horizon_down_count": hdown, "p_target_first": target / total, "p_stop_first": stop / total, "p_flat": flat / total, "p_ambiguous": ambiguous / total, "p_no_trade": no_trade / total}


def _member(ref, emb, *, buy_return=0.08, sell_return=-0.02):
    return {
        "ref": ref,
        "transformed_features": {f"f{i}": float(value) for i, value in enumerate(emb)},
        "side_outcomes": {
            "BUY": {
                "after_cost_return_pct": buy_return,
                "mae_pct": 0.01,
                "mfe_pct": 0.06,
                "close_return_d2_pct": buy_return * 0.6,
                "close_return_d3_pct": buy_return * 0.8,
                "resolved_by_d2": False,
                "resolved_by_d3": True,
                "first_touch_label": "UP_FIRST" if buy_return > 0 else "DOWN_FIRST",
            },
            "SELL": {
                "after_cost_return_pct": sell_return,
                "mae_pct": 0.01,
                "mfe_pct": 0.06,
                "close_return_d2_pct": sell_return * 0.6,
                "close_return_d3_pct": sell_return * 0.8,
                "resolved_by_d2": False,
                "resolved_by_d3": True,
                "first_touch_label": "UP_FIRST" if sell_return > 0 else "DOWN_FIRST",
            },
        },
    }


def _payload_from_label(label: str, value: float) -> dict:
    normalized = label.upper()
    return {
        "after_cost_return_pct": value,
        "mae_pct": 0.01,
        "mfe_pct": 0.06,
        "close_return_d2_pct": value * 0.6,
        "close_return_d3_pct": value * 0.8,
        "resolved_by_d2": normalized in {"UP_FIRST", "DOWN_FIRST"},
        "resolved_by_d3": normalized != "NO_TRADE",
        "flat": normalized == "FLAT",
        "ambiguous": normalized == "AMBIGUOUS",
        "no_trade": normalized == "NO_TRADE",
        "first_touch_label": normalized,
    }


def _payloads_from_stats(prefix: str, stats: dict, *, positive_value: float, negative_value: float) -> list[dict]:
    payloads: list[dict] = []
    payloads.extend(_payload_from_label("UP_FIRST", positive_value) for _ in range(int(stats.get("target_first_count", 0) or 0)))
    payloads.extend(_payload_from_label("DOWN_FIRST", negative_value) for _ in range(int(stats.get("stop_first_count", 0) or 0)))
    payloads.extend(_payload_from_label("FLAT", 0.0) for _ in range(int(stats.get("flat_count", 0) or 0)))
    payloads.extend(_payload_from_label("AMBIGUOUS", 0.0) for _ in range(int(stats.get("ambiguous_count", 0) or 0)))
    payloads.extend(_payload_from_label("NO_TRADE", 0.0) for _ in range(int(stats.get("no_trade_count", 0) or 0)))
    if payloads:
        return payloads
    fallback = float(stats.get("mean_return_pct", 0.0) or 0.0)
    label = "UP_FIRST" if fallback >= 0.0 else "DOWN_FIRST"
    return [_payload_from_label(label, fallback)]


def _lineage_from_side_stats(pid: str, emb: list[float], buy: dict, sell: dict) -> list[dict]:
    buy_payloads = _payloads_from_stats(f"{pid}-buy", buy, positive_value=max(float(buy.get("mean_return_pct", 0.0) or 0.0), 0.01), negative_value=min(float(buy.get("mean_return_pct", -0.02) or -0.02), -0.01))
    sell_payloads = _payloads_from_stats(f"{pid}-sell", sell, positive_value=max(float(sell.get("mean_return_pct", 0.0) or 0.0), 0.01), negative_value=min(float(sell.get("mean_return_pct", -0.02) or -0.02), -0.01))
    count = max(len(buy_payloads), len(sell_payloads))
    lineage: list[dict] = []
    for idx in range(count):
        lineage.append(
            {
                "ref": f"{pid}-member-{idx}:2026-01-{idx + 1:02d}",
                "transformed_features": {f"f{i}": float(value) for i, value in enumerate(emb)},
                "side_outcomes": {
                    "BUY": dict(buy_payloads[min(idx, len(buy_payloads) - 1)]),
                    "SELL": dict(sell_payloads[min(idx, len(sell_payloads) - 1)]),
                },
            }
        )
    return lineage


def _proto(pid, emb, *, regime="RISK_ON", sector="TECH", buy=None, sell=None, supp=5, decayed=2.5, fresh=5, lineage=None):
    buy = buy or _side_stats(target=4, stop=1, flat=0, ambiguous=0, no_trade=0, hup=3, hdown=1, ret=0.08, supp=supp, decayed=decayed, fresh=fresh)
    sell = sell or _side_stats(target=1, stop=4, flat=0, ambiguous=0, no_trade=0, hup=1, hdown=3, ret=-0.02, supp=supp, decayed=decayed, fresh=fresh)
    resolved_lineage = list(lineage or _lineage_from_side_stats(pid, emb, buy, sell))
    return StatePrototype(prototype_id=pid, anchor_code="SIM_V2", embedding=emb, member_count=max(supp, len(resolved_lineage)), representative_symbol="AAPL", representative_date="2026-01-01", representative_hash=f"hash-{pid}", anchor_quality=0.8, regime_code=regime, sector_code=sector, liquidity_score=0.9, support_count=max(supp, len(resolved_lineage)), decayed_support=decayed, freshness_days=fresh, side_stats={"BUY": buy, "SELL": sell}, prototype_membership={"lineage": resolved_lineage}, metadata={"prior_buckets": {"regime": [regime], "sector": [sector], "liquidity": ["HIGH"]}})


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
    ev_cfg = EVConfig(top_k=2, min_effective_sample_size=1.0, min_expected_utility=0.0, min_regime_alignment=0.0, max_uncertainty=1.0, max_return_interval_width=1.0, abstain_margin=0.0, diagnostic_disable_lower_bound_gate=True)
    long_ev = estimate_expected_value(side="BUY", query_embedding=[1.0, 0.0], candidates=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    short_ev = estimate_expected_value(side="SELL", query_embedding=[1.0, 0.0], candidates=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calib)
    assert long_ev.calibrated_ev > short_ev.calibrated_ev
    assert surface.buy.utility["p_target_first"] > surface.sell.utility["p_target_first"]
    assert surface.chosen_side == "BUY"
    assert long_ev.top_matches[0]["representative_hash"] == "hash-p1"
    assert long_ev.diagnostics["telemetry"]["top1_weight_share"] > 0.0
    assert long_ev.diagnostics["telemetry"]["consensus_signature"]


def test_decision_surface_prefers_sell_when_sell_distribution_better():
    pool = [_proto("p1", [1.0, 0.0], buy=_side_stats(target=1, stop=4, flat=0, ambiguous=0, no_trade=0, hup=0, hdown=3, ret=-0.01), sell=_side_stats(target=4, stop=1, flat=0, ambiguous=0, no_trade=0, hup=3, hdown=0, ret=0.06))]
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(top_k=2, min_effective_sample_size=1.0, min_expected_utility=0.0, min_regime_alignment=0.0, max_uncertainty=1.0, max_return_interval_width=1.0, abstain_margin=0.0, diagnostic_disable_lower_bound_gate=True), candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
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


def test_diagnostic_disable_ess_gate_is_consistent_between_surface_and_ev():
    pool = [_proto("p1", [1.0, 0.0])]
    ev_cfg = EVConfig(
        top_k=1,
        min_effective_sample_size=2.0,
        min_expected_utility=0.0,
        max_uncertainty=0.2,
        min_regime_alignment=0.0,
        max_return_interval_width=1.0,
        diagnostic_disable_lower_bound_gate=True,
        diagnostic_disable_ess_gate=True,
    )
    ev = estimate_expected_value(side="BUY", query_embedding=[1.0, 0.0], candidates=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=pool, regime_code="RISK_ON", sector_code="TECH", ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=CalibrationModel(method="identity"))
    assert "low_neff" not in ev.abstain_reasons
    assert "low_ess" not in surface.diagnostics["side_gate_eval"]["buy_reasons"]


def test_estimate_expected_value_uses_member_mixture_not_single_prototype_echo():
    lineage = [
        _member("AAPL:2026-01-01", [1.0, 0.0], buy_return=0.08),
        _member("MSFT:2026-01-02", [0.98, 0.02], buy_return=0.06),
        _member("NVDA:2026-01-03", [0.97, 0.03], buy_return=0.04),
    ]
    pool = [_proto("p1", [1.0, 0.0], supp=9, decayed=5.0, lineage=lineage)]
    ev = estimate_expected_value(
        side="BUY",
        query_embedding=[1.0, 0.0],
        candidates=pool,
        regime_code="RISK_ON",
        sector_code="TECH",
        ev_config=EVConfig(top_k=1, prototype_retrieval_k=4, member_retrieval_k=12, min_effective_sample_size=1.0),
        candidate_index=ExactCosineCandidateIndex(),
        calibration=CalibrationModel(method="identity"),
        query_date="2026-01-10",
    )
    telemetry = ev.diagnostics["telemetry"]
    assert telemetry["member_pre_truncation_count"] == 3
    assert telemetry["member_candidate_count"] == 3
    assert telemetry["member_mixture_ess"] > 1.0
    assert telemetry["member_top1_weight_share"] < 1.0
    assert telemetry["member_consensus_signature"]
    assert any(match["member_key"].endswith(":BUY") for match in ev.diagnostics["member_top_matches"])
    assert ev.expected_net_return > 0.0


def test_rejection_reasons_uses_fold_calibration_contract_labels():
    reasons = rejection_reasons({"expectancy_after_cost": -0.01, "psr": 0.4, "dsr": 0.4, "score_decile_monotonicity": False, "calibration_error": 0.4})
    assert "non_positive_expectancy" in reasons
    assert "low_psr_or_dsr" in reasons
    assert "non_monotonic_score_buckets" in reasons
    assert "high_calibration_error" in reasons
