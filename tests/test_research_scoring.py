from backtest_app.research.models import ResearchAnchor
from backtest_app.research.prototype import PrototypeConfig, build_anchor_prototypes
from backtest_app.research.repository import ExactCosineCandidateIndex
from backtest_app.research.scoring import ScoringConfig, score_candidates_exact


def test_build_anchor_prototypes_dedups_similar_members():
    anchors = [
        ResearchAnchor(symbol="AAPL", anchor_code="EARNINGS", reference_date="2026-01-01", embedding=[1.0, 0.0], anchor_quality=0.8, regime_code="RISK_ON", sector_code="TECH", liquidity_score=0.9),
        ResearchAnchor(symbol="MSFT", anchor_code="EARNINGS", reference_date="2026-01-02", embedding=[0.999, 0.001], anchor_quality=0.7, regime_code="RISK_ON", sector_code="TECH", liquidity_score=0.8),
    ]
    prototypes = build_anchor_prototypes(anchors, PrototypeConfig(dedup_similarity_threshold=0.99))
    assert len(prototypes) == 1
    assert prototypes[0].member_count == 2


def test_score_candidates_exact_combines_similarity_quality_and_filters():
    anchors = [
        ResearchAnchor(symbol="AAPL", anchor_code="EARNINGS", reference_date="2026-01-01", embedding=[1.0, 0.0], anchor_quality=0.9, regime_code="RISK_ON", sector_code="TECH", liquidity_score=0.9),
        ResearchAnchor(symbol="XOM", anchor_code="OIL", reference_date="2026-01-01", embedding=[0.7, 0.7], anchor_quality=0.6, regime_code="RISK_OFF", sector_code="ENERGY", liquidity_score=0.2),
    ]
    prototypes = build_anchor_prototypes(anchors)
    scored = score_candidates_exact(
        query_embedding=[1.0, 0.0],
        candidates=prototypes,
        regime_code="RISK_ON",
        sector_code="TECH",
        config=ScoringConfig(min_liquidity_score=0.5),
        candidate_index=ExactCosineCandidateIndex(),
    )
    assert len(scored) == 1
    assert scored[0].anchor_code == "EARNINGS"
    assert scored[0].score > 0.8
