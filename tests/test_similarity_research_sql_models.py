from backtest_app.research.sql_models import AnchorEventRecord, AnchorVectorRecord


def test_anchor_event_record_exposes_similarity_metrics():
    columns = AnchorEventRecord.__table__.c
    for name in (
        "mae_pct",
        "mfe_pct",
        "days_to_hit",
        "after_cost_return_pct",
        "quality_score",
        "regime_code",
        "sector_code",
        "liquidity_score",
        "prototype_id",
        "prototype_membership",
    ):
        assert name in columns


def test_anchor_vector_record_exposes_similarity_vectors():
    columns = AnchorVectorRecord.__table__.c
    for name in (
        "shape_vector",
        "ctx_vector",
        "vector_dim",
        "vector_version",
        "embedding_model",
        "shape_vector_dim",
        "ctx_vector_dim",
        "prototype_membership",
    ):
        assert name in columns
