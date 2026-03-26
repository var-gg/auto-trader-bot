from __future__ import annotations

from sqlalchemy import BigInteger, Date, DateTime, ForeignKey, Index, Integer, Numeric, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from pgvector.sqlalchemy import Vector

from backtest_app.db.sql_base import ResearchBase


class AnchorEventRecord(ResearchBase):
    __tablename__ = "anchor_event"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "symbol",
            "anchor_code",
            "event_time",
            "config_version",
            "label_version",
            name="uq_anchor_event_canonical",
        ),
        Index("ix_anchor_event_symbol_time", "symbol", "event_time"),
        Index("ix_anchor_event_market_refdate", "market", "reference_date"),
        Index("ix_anchor_event_run_id", "run_id"),
        Index("ix_anchor_event_ticker_refdate", "ticker_id", "reference_date"),
        Index("ix_anchor_event_quality_score", "quality_score", "reference_date"),
        Index("ix_anchor_event_regime_sector", "regime_code", "sector_code", "reference_date"),
        Index("ix_anchor_event_prototype_id", "prototype_id"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("trading.anchor_label_run.id", ondelete="CASCADE"), nullable=False)
    ticker_id: Mapped[int | None] = mapped_column(Integer)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    market: Mapped[str] = mapped_column(Text, nullable=False)
    anchor_code: Mapped[str] = mapped_column(Text, nullable=False)
    event_time: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    anchor_date: Mapped[object | None] = mapped_column(Date)
    reference_date: Mapped[object] = mapped_column(Date, nullable=False)
    side_bias: Mapped[str | None] = mapped_column(Text)
    config_version: Mapped[str] = mapped_column(Text, nullable=False)
    label_version: Mapped[str] = mapped_column(Text, nullable=False)
    horizon_days: Mapped[int | None] = mapped_column(Integer)
    target_return_pct: Mapped[float | None] = mapped_column(Numeric(18, 8))
    max_reverse_pct: Mapped[float | None] = mapped_column(Numeric(18, 8))
    outcome_label: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Numeric(18, 8))
    mae_pct: Mapped[float | None] = mapped_column(Numeric(18, 8))
    mfe_pct: Mapped[float | None] = mapped_column(Numeric(18, 8))
    days_to_hit: Mapped[int | None] = mapped_column(Integer)
    after_cost_return_pct: Mapped[float | None] = mapped_column(Numeric(18, 8))
    quality_score: Mapped[float | None] = mapped_column(Numeric(18, 8))
    regime_code: Mapped[str | None] = mapped_column(Text)
    sector_code: Mapped[str | None] = mapped_column(Text)
    liquidity_score: Mapped[float | None] = mapped_column(Numeric(18, 8))
    prototype_id: Mapped[str | None] = mapped_column(Text)
    prototype_membership: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    # Contract: {schema_version, raw_path_summary, side_outcomes:{BUY:{...},SELL:{...}}}
    event_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    diagnostics: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class PrototypeRunRecord(ResearchBase):
    __tablename__ = "prototype_run"
    __table_args__ = (
        UniqueConstraint("run_id", "as_of_date", "memory_version", name="uq_prototype_run_canonical"),
        Index("ix_prototype_run_asof", "as_of_date", "memory_version"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("trading.anchor_label_run.id", ondelete="CASCADE"), nullable=False)
    as_of_date: Mapped[object] = mapped_column(Date, nullable=False)
    memory_version: Mapped[str] = mapped_column(Text, nullable=False)
    prototype_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    lineage_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class PrototypeRecord(ResearchBase):
    __tablename__ = "prototype_record"
    __table_args__ = (
        UniqueConstraint("prototype_run_id", "prototype_id", name="uq_prototype_record_canonical"),
        Index("ix_prototype_record_pid", "prototype_id"),
        Index("ix_prototype_record_run", "prototype_run_id"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    prototype_run_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("trading.prototype_run.id", ondelete="CASCADE"), nullable=False)
    prototype_id: Mapped[str] = mapped_column(Text, nullable=False)
    as_of_date: Mapped[object] = mapped_column(Date, nullable=False)
    memory_version: Mapped[str] = mapped_column(Text, nullable=False)
    cluster_key: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    representative_hash: Mapped[str | None] = mapped_column(Text)
    stats_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    membership_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class AnchorVectorRecord(ResearchBase):
    __tablename__ = "anchor_vector"
    __table_args__ = (
        UniqueConstraint("anchor_event_id", "embedding_model", "embedding_version", name="uq_anchor_vector_canonical"),
        Index("ix_anchor_vector_anchor_code", "anchor_code", "embedding_model", "embedding_version"),
        Index("ix_anchor_vector_event_id", "anchor_event_id"),
        Index("ix_anchor_vector_vector_version", "anchor_code", "embedding_model", "vector_version"),
        Index("ix_anchor_vector_dims", "vector_dim", "shape_vector_dim", "ctx_vector_dim"),
        {"schema": "trading"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    anchor_event_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("trading.anchor_event.id", ondelete="CASCADE"))
    anchor_code: Mapped[str] = mapped_column(Text, nullable=False)
    config_version: Mapped[str] = mapped_column(Text, nullable=False)
    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    embedding_version: Mapped[str] = mapped_column(Text, nullable=False)
    vector_version: Mapped[str | None] = mapped_column(Text)
    vector_dim: Mapped[int | None] = mapped_column(Integer)
    shape_vector_dim: Mapped[int | None] = mapped_column(Integer)
    ctx_vector_dim: Mapped[int | None] = mapped_column(Integer)
    embedding_vector: Mapped[object | None] = mapped_column(Vector())
    shape_vector: Mapped[object | None] = mapped_column(Vector())
    ctx_vector: Mapped[object | None] = mapped_column(Vector())
    prototype_membership: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
