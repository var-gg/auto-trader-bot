from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research.models import EventOutcomeRecord, ResearchAnchor
from backtest_app.research.prototype import PrototypeConfig, build_anchor_prototypes, build_prototype_snapshot_from_event_memory
from backtest_app.research.repository import ExactCosineCandidateIndex, load_prototypes_asof
from backtest_app.research.scoring import ScoringConfig, score_candidates_exact


def _anchor(symbol, ref_date, embedding, *, side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.05, mae=-0.01, mfe=0.07, quality=0.8):
    return ResearchAnchor(symbol=symbol, anchor_code="SIM_V2", reference_date=ref_date, anchor_date=ref_date, side=side, embedding=embedding, shape_vector=embedding, ctx_vector=[], vector_version="research_similarity_v2_multiscale", embedding_model="manual-multiscale", vector_dim=len(embedding), anchor_quality=quality, mae_pct=mae, mfe_pct=mfe, days_to_hit=2, after_cost_return_pct=ret, realized_return_pct=ret, regime_code=regime, sector_code=sector, liquidity_score=liq, metadata={"feature_version": "multiscale_manual_v2"})


def _event(symbol, event_date, outcome_end_date, buy_ret=0.03, sell_ret=-0.03):
    return EventOutcomeRecord(symbol=symbol, event_date=event_date, outcome_end_date=outcome_end_date, schema_version="event_outcome_v1", path_summary={"regime_code": "RISK_ON", "sector_code": "TECH", "liquidity_bucket": "HIGH"}, side_outcomes={"BUY": {"after_cost_return_pct": buy_ret, "mae_pct": -0.01, "mfe_pct": 0.04}, "SELL": {"after_cost_return_pct": sell_ret, "mae_pct": -0.04, "mfe_pct": 0.01}}, diagnostics={"regime_code": "RISK_ON", "sector_code": "TECH"})


def test_build_anchor_prototypes_clusters_by_side_regime_sector_liquidity_and_keeps_stats():
    anchors = [_anchor("AAPL", "2026-01-01", [1.0, 0.0], side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.08), _anchor("MSFT", "2026-01-02", [0.99, 0.01], side="BUY", regime="RISK_ON", sector="TECH", liq=0.85, ret=0.06), _anchor("XOM", "2026-01-02", [0.7, 0.7], side="BUY", regime="RISK_OFF", sector="ENERGY", liq=0.85, ret=0.01)]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.98, min_support_count=1, memory_version="memory_asof_v1"), as_of_date="2026-01-10")
    assert len(prototypes) == 2
    tech = [p for p in prototypes if p.sector_bucket == "TECH"][0]
    assert tech.support_count == 2
    assert tech.decayed_support > 0
    assert tech.mean_return_pct > 0
    assert tech.win_rate == 1.0
    assert tech.metadata["representative_kind"] == "medoid"
    assert tech.regime_bucket == "RISK_ON"
    assert tech.metadata["return_q10_pct"] <= tech.metadata["return_q90_pct"]
    assert tech.prototype_id.startswith("2026-01-10:memory_asof_v1:")


def test_build_anchor_prototypes_prunes_low_support_and_prevents_cross_regime_merge():
    anchors = [_anchor("AAPL", "2026-01-01", [1.0, 0.0], regime="RISK_ON"), _anchor("XOM", "2026-01-02", [1.0, 0.0], regime="RISK_OFF")]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.99, min_support_count=2))
    assert prototypes == []


def test_score_candidates_exact_uses_prototype_memory_stats():
    anchors = [_anchor("AAPL", "2026-01-01", [1.0, 0.0], regime="RISK_ON", sector="TECH", liq=0.9, ret=0.08), _anchor("MSFT", "2026-01-02", [0.99, 0.01], regime="RISK_ON", sector="TECH", liq=0.9, ret=0.07), _anchor("XOM", "2026-01-02", [0.7, 0.7], regime="RISK_ON", sector="ENERGY", liq=0.9, ret=0.01)]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.98, min_support_count=1))
    scored = score_candidates_exact(query_embedding=[1.0, 0.0], candidates=prototypes, regime_code="RISK_ON", sector_code="TECH", config=ScoringConfig(min_liquidity_score=0.1, min_support_count=1), candidate_index=ExactCosineCandidateIndex())
    assert scored
    assert scored[0].diagnostics["support_count"] >= 1
    assert scored[0].diagnostics["regime_bucket"] == "RISK_ON"


def test_build_prototype_snapshot_from_event_memory_is_deterministic_and_keeps_lineage(tmp_path):
    events = [_event("AAPL", "2026-01-01", "2026-01-05", buy_ret=0.04, sell_ret=-0.04), _event("MSFT", "2026-01-02", "2026-01-06", buy_ret=0.02, sell_ret=-0.02)]
    snap1 = build_prototype_snapshot_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1")
    snap2 = build_prototype_snapshot_from_event_memory(event_records=events, as_of_date="2026-01-10", memory_version="memory_asof_v1")
    assert snap1["prototypes"][0]["prototype_id"] == snap2["prototypes"][0]["prototype_id"]
    assert snap1["prototypes"][0]["stats"]["BUY"]["support_count"] == 2
    assert snap1["prototypes"][0]["stats"]["SELL"]["support_count"] == 2
    assert snap1["lineage"][snap1["prototypes"][0]["prototype_id"]]
    assert all(m["outcome_end_date"] < "2026-01-10" for m in snap1["lineage"][snap1["prototypes"][0]["prototype_id"]])
    store = JsonResearchArtifactStore(str(tmp_path))
    store.save_prototype_snapshot(run_id="r1", as_of_date="2026-01-10", memory_version="memory_asof_v1", payload=snap1)
    loaded = load_prototypes_asof(artifact_store=store, run_id="r1", as_of_date="2026-01-10", memory_version="memory_asof_v1")
    assert loaded
    assert loaded[0]["prototype_id"] == snap1["prototypes"][0]["prototype_id"]
