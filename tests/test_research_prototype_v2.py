from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research.models import EventOutcomeRecord, ResearchAnchor
from backtest_app.research.prototype import PrototypeConfig, build_anchor_prototypes, build_prototype_snapshot_from_event_memory, build_state_prototypes_from_event_memory
from backtest_app.research.repository import ExactCosineCandidateIndex, load_prototypes_asof
from backtest_app.research.scoring import ScoringConfig, build_decision_surface, score_candidates_exact


def _anchor(symbol, ref_date, embedding, *, side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.05, mae=-0.01, mfe=0.07, quality=0.8):
    return ResearchAnchor(symbol=symbol, anchor_code="SIM_V2", reference_date=ref_date, anchor_date=ref_date, side=side, embedding=embedding, shape_vector=embedding, ctx_vector=[], vector_version="research_similarity_v2_multiscale", embedding_model="manual-multiscale", vector_dim=len(embedding), anchor_quality=quality, mae_pct=mae, mfe_pct=mfe, days_to_hit=2, after_cost_return_pct=ret, realized_return_pct=ret, regime_code=regime, sector_code=sector, liquidity_score=liq, metadata={"feature_version": "multiscale_manual_v2"})


def _event(symbol, event_date, outcome_end_date, embedding=None, buy_ret=0.03, sell_ret=-0.03):
    embedding = embedding or [1.0, 0.0]
    return EventOutcomeRecord(symbol=symbol, event_date=event_date, outcome_end_date=outcome_end_date, schema_version="event_outcome_v1", path_summary={"regime_code": "RISK_ON", "sector_code": "TECH", "liquidity_bucket": "HIGH", "embedding": embedding}, side_outcomes={"BUY": {"after_cost_return_pct": buy_ret, "mae_pct": -0.01, "mfe_pct": 0.04}, "SELL": {"after_cost_return_pct": sell_ret, "mae_pct": -0.04, "mfe_pct": 0.01}}, diagnostics={"regime_code": "RISK_ON", "sector_code": "TECH", "embedding": embedding, "quality_score": 0.9, "liquidity_score": 0.9})


def test_build_anchor_prototypes_legacy_wrapper_still_works():
    anchors = [_anchor("AAPL", "2026-01-01", [1.0, 0.0], side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.08)]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.98, min_support_count=1, memory_version="memory_asof_v1"), as_of_date="2026-01-10")
    assert len(prototypes) == 1
    assert prototypes[0].metadata["legacy_wrapper"] is True


def test_build_state_prototypes_from_event_memory_keeps_buy_and_sell_inside_same_prototype():
    events = [_event("AAPL", "2026-01-01", "2026-01-05", embedding=[1.0, 0.0], buy_ret=0.04, sell_ret=-0.04), _event("MSFT", "2026-01-02", "2026-01-06", embedding=[0.99, 0.01], buy_ret=0.02, sell_ret=-0.02)]
    prototypes = build_state_prototypes_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    assert len(prototypes) == 1
    proto = prototypes[0]
    assert "BUY" in proto.side_stats
    assert "SELL" in proto.side_stats
    assert proto.side_stats["BUY"]["support_count"] == 2
    assert proto.side_stats["SELL"]["support_count"] == 2
    assert proto.metadata["spec_hash"] == "spec-1"
    assert proto.representative_hash
    assert proto.prototype_membership["member_refs"]


def test_score_candidates_exact_and_surface_use_same_prototype_pool_for_both_sides():
    events = [_event("AAPL", "2026-01-01", "2026-01-05", embedding=[1.0, 0.0], buy_ret=0.08, sell_ret=-0.01), _event("MSFT", "2026-01-02", "2026-01-06", embedding=[0.98, 0.02], buy_ret=0.06, sell_ret=-0.02)]
    prototypes = build_state_prototypes_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    buy_scores = score_candidates_exact(query_embedding=[1.0, 0.0], candidates=prototypes, regime_code="RISK_ON", sector_code="TECH", config=ScoringConfig(min_liquidity_score=0.1, min_support_count=1), candidate_index=ExactCosineCandidateIndex(), side="BUY")
    sell_scores = score_candidates_exact(query_embedding=[1.0, 0.0], candidates=prototypes, regime_code="RISK_ON", sector_code="TECH", config=ScoringConfig(min_liquidity_score=0.1, min_support_count=1), candidate_index=ExactCosineCandidateIndex(), side="SELL")
    surface = build_decision_surface(query_embedding=[1.0, 0.0], prototype_pool=prototypes, regime_code="RISK_ON", sector_code="TECH", candidate_index=ExactCosineCandidateIndex())
    assert buy_scores and sell_scores
    assert buy_scores[0].prototype_id == sell_scores[0].prototype_id
    assert surface.diagnostics["shared_neighbor_pool"] is True


def test_build_prototype_snapshot_from_event_memory_is_deterministic_and_keeps_lineage(tmp_path):
    events = [_event("AAPL", "2026-01-01", "2026-01-05", buy_ret=0.04, sell_ret=-0.04), _event("MSFT", "2026-01-02", "2026-01-06", buy_ret=0.02, sell_ret=-0.02)]
    snap1 = build_prototype_snapshot_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    snap2 = build_prototype_snapshot_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1", spec_hash="spec-1")
    assert snap1["prototypes"][0]["prototype_id"] == snap2["prototypes"][0]["prototype_id"]
    assert snap1["prototypes"][0]["side_stats"]["BUY"]["support_count"] == 2
    assert snap1["prototypes"][0]["side_stats"]["SELL"]["support_count"] == 2
    store = JsonResearchArtifactStore(str(tmp_path))
    store.save_prototype_snapshot(run_id="r1", as_of_date="2026-01-10", memory_version="memory_asof_v1", payload=snap1)
    loaded = load_prototypes_asof(artifact_store=store, run_id="r1", as_of_date="2026-01-10", memory_version="memory_asof_v1")
    assert loaded
    assert loaded[0].prototype_id == snap1["prototypes"][0]["prototype_id"]
