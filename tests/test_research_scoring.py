from backtest_app.research.models import EventOutcomeRecord, ResearchAnchor
from backtest_app.research.prototype import PrototypeConfig, build_anchor_prototypes, build_state_prototypes_from_event_memory
from backtest_app.research.repository import ExactCosineCandidateIndex
from backtest_app.research.scoring import EVConfig, ScoringConfig, build_decision_surface, score_candidates_exact


def _event(symbol, event_date, outcome_end_date, embedding, *, buy_counts=None, sell_counts=None, regime="RISK_ON", sector="TECH", liq=0.9):
    buy_counts = buy_counts or {"target_first_count": 3, "stop_first_count": 1, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 2, "horizon_down_count": 1, "after_cost_return_pct": 0.08, "mae_pct": -0.01, "mfe_pct": 0.04}
    sell_counts = sell_counts or {"target_first_count": 1, "stop_first_count": 3, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 0, "horizon_down_count": 2, "after_cost_return_pct": -0.03, "mae_pct": -0.03, "mfe_pct": 0.01}
    return EventOutcomeRecord(symbol=symbol, event_date=event_date, outcome_end_date=outcome_end_date, schema_version="v1", path_summary={"regime_code": regime, "sector_code": sector, "liquidity_bucket": "HIGH", "embedding": embedding}, side_outcomes={"BUY": buy_counts, "SELL": sell_counts}, diagnostics={"regime_code": regime, "sector_code": sector, "embedding": embedding, "quality_score": 0.9, "liquidity_score": liq})


def test_build_anchor_prototypes_legacy_wrapper_preserves_single_anchor_shape():
    anchors = [
        ResearchAnchor(symbol="AAPL", anchor_code="EARNINGS", reference_date="2026-01-01", embedding=[1.0, 0.0], anchor_quality=0.8, regime_code="RISK_ON", sector_code="TECH", liquidity_score=0.9),
        ResearchAnchor(symbol="MSFT", anchor_code="EARNINGS", reference_date="2026-01-02", embedding=[0.999, 0.001], anchor_quality=0.7, regime_code="RISK_ON", sector_code="TECH", liquidity_score=0.8),
    ]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.99))
    assert len(prototypes) == 2
    assert all(p.metadata["legacy_wrapper"] for p in prototypes)


def test_score_candidates_exact_combines_similarity_quality_and_filters_from_state_memory():
    events = [
        _event("AAPL", "2026-01-01", "2026-01-05", [1.0, 0.0]),
        _event("XOM", "2026-01-01", "2026-01-05", [0.7, 0.7], regime="RISK_OFF", sector="ENERGY", liq=0.2),
    ]
    prototypes = build_state_prototypes_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    scored = score_candidates_exact(query_embedding=[1.0, 0.0], candidates=prototypes, regime_code="RISK_ON", sector_code="TECH", config=ScoringConfig(min_liquidity_score=0.5), candidate_index=ExactCosineCandidateIndex(), side="BUY")
    assert len(scored) == 1
    assert scored[0].score > 0.1


def test_decision_surface_buy_favorable_distribution():
    protos = build_state_prototypes_from_event_memory(event_records=[_event("AAPL", "2026-01-01", "2026-01-05", [1.0, 0.0])], as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=protos, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(max_uncertainty=0.2, min_effective_sample_size=1.0), candidate_index=ExactCosineCandidateIndex())
    assert surface.chosen_side == "BUY"
    assert surface.buy.utility["p_target_first"] > surface.sell.utility["p_target_first"]


def test_decision_surface_sell_favorable_distribution():
    sell_better = _event("AAPL", "2026-01-01", "2026-01-05", [1.0, 0.0], buy_counts={"target_first_count": 1, "stop_first_count": 3, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 0, "horizon_down_count": 2, "after_cost_return_pct": -0.01, "mae_pct": -0.03, "mfe_pct": 0.01}, sell_counts={"target_first_count": 3, "stop_first_count": 1, "flat_count": 0, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 2, "horizon_down_count": 0, "after_cost_return_pct": 0.06, "mae_pct": -0.01, "mfe_pct": 0.05})
    protos = build_state_prototypes_from_event_memory(event_records=[sell_better], as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=protos, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(max_uncertainty=0.2, min_effective_sample_size=1.0), candidate_index=ExactCosineCandidateIndex())
    assert surface.chosen_side == "SELL"


def test_decision_surface_abstains_when_ambiguous_share_too_high():
    ambiguous = _event("AAPL", "2026-01-01", "2026-01-05", [1.0, 0.0], buy_counts={"target_first_count": 1, "stop_first_count": 1, "flat_count": 0, "ambiguous_count": 3, "no_trade_count": 0, "horizon_up_count": 1, "horizon_down_count": 1, "after_cost_return_pct": 0.01, "mae_pct": -0.02, "mfe_pct": 0.02})
    protos = build_state_prototypes_from_event_memory(event_records=[ambiguous], as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=protos, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(max_uncertainty=0.2), candidate_index=ExactCosineCandidateIndex())
    assert surface.abstain is True
    assert "high_ambiguous_share" in surface.abstain_reasons


def test_decision_surface_keeps_nonzero_flat_probability():
    flat_heavy = _event("AAPL", "2026-01-01", "2026-01-05", [1.0, 0.0], buy_counts={"target_first_count": 1, "stop_first_count": 1, "flat_count": 3, "ambiguous_count": 0, "no_trade_count": 0, "horizon_up_count": 1, "horizon_down_count": 1, "after_cost_return_pct": 0.005, "mae_pct": -0.01, "mfe_pct": 0.01})
    protos = build_state_prototypes_from_event_memory(event_records=[flat_heavy], as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=protos, regime_code="RISK_ON", sector_code="TECH", ev_config=EVConfig(max_uncertainty=0.2), candidate_index=ExactCosineCandidateIndex())
    assert surface.buy.p_flat > 0.0
