from backtest_app.research.models import ResearchAnchor
from backtest_app.research.prototype import PrototypeConfig, build_anchor_prototypes
from backtest_app.research.repository import ExactCosineCandidateIndex
from backtest_app.research.scoring import ScoringConfig, score_candidates_exact


def _anchor(symbol, ref_date, embedding, *, side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.05, mae=-0.01, mfe=0.07, quality=0.8):
    return ResearchAnchor(
        symbol=symbol,
        anchor_code="SIM_V2",
        reference_date=ref_date,
        anchor_date=ref_date,
        side=side,
        embedding=embedding,
        shape_vector=embedding,
        ctx_vector=[],
        vector_version="research_similarity_v2_multiscale",
        embedding_model="manual-multiscale",
        vector_dim=len(embedding),
        anchor_quality=quality,
        mae_pct=mae,
        mfe_pct=mfe,
        days_to_hit=2,
        after_cost_return_pct=ret,
        realized_return_pct=ret,
        regime_code=regime,
        sector_code=sector,
        liquidity_score=liq,
        metadata={"feature_version": "multiscale_manual_v2"},
    )


def test_build_anchor_prototypes_clusters_by_side_regime_sector_liquidity_and_keeps_stats():
    anchors = [
        _anchor("AAPL", "2026-01-01", [1.0, 0.0], side="BUY", regime="RISK_ON", sector="TECH", liq=0.9, ret=0.08),
        _anchor("MSFT", "2026-01-02", [0.99, 0.01], side="BUY", regime="RISK_ON", sector="TECH", liq=0.85, ret=0.06),
        _anchor("XOM", "2026-01-02", [0.7, 0.7], side="BUY", regime="RISK_OFF", sector="ENERGY", liq=0.85, ret=0.01),
    ]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.98, min_support_count=1))
    assert len(prototypes) == 2
    tech = [p for p in prototypes if p.sector_bucket == "TECH"][0]
    assert tech.support_count == 2
    assert tech.decayed_support > 0
    assert tech.mean_return_pct > 0
    assert tech.win_rate == 1.0
    assert tech.metadata["representative_kind"] == "medoid"
    assert tech.regime_bucket == "RISK_ON"


def test_build_anchor_prototypes_prunes_low_support_and_prevents_cross_regime_merge():
    anchors = [
        _anchor("AAPL", "2026-01-01", [1.0, 0.0], regime="RISK_ON"),
        _anchor("XOM", "2026-01-02", [1.0, 0.0], regime="RISK_OFF"),
    ]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.99, min_support_count=2))
    assert prototypes == []


def test_score_candidates_exact_uses_prototype_memory_stats():
    anchors = [
        _anchor("AAPL", "2026-01-01", [1.0, 0.0], regime="RISK_ON", sector="TECH", liq=0.9, ret=0.08),
        _anchor("MSFT", "2026-01-02", [0.99, 0.01], regime="RISK_ON", sector="TECH", liq=0.9, ret=0.07),
        _anchor("XOM", "2026-01-02", [0.7, 0.7], regime="RISK_ON", sector="ENERGY", liq=0.9, ret=0.01),
    ]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.98, min_support_count=1))
    scored = score_candidates_exact(
        query_embedding=[1.0, 0.0],
        candidates=prototypes,
        regime_code="RISK_ON",
        sector_code="TECH",
        config=ScoringConfig(min_liquidity_score=0.1, min_support_count=1),
        candidate_index=ExactCosineCandidateIndex(),
    )
    assert scored
    assert scored[0].diagnostics["support_count"] >= 1
    assert scored[0].diagnostics["regime_bucket"] == "RISK_ON"
