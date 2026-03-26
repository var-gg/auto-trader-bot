from __future__ import annotations

from sqlalchemy import BigInteger, DateTime, Index, Integer, Numeric, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backtest_app.db.sql_base import ResearchBase


class BacktestRunRecord(ResearchBase):
    __tablename__ = "backtest_run"
    __table_args__ = (
        UniqueConstraint("run_key", name="uq_backtest_run_run_key"),
        Index("ix_backtest_run_scenario_started_at", "scenario_id", "started_at"),
        Index("ix_backtest_run_status_started_at", "status", "started_at"),
        Index("ix_backtest_run_strategy_market_started_at", "strategy_id", "market", "started_at"),
        Index("ix_backtest_run_data_source_started_at", "data_source", "started_at"),
        {"schema": "bt_result"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_key: Mapped[str] = mapped_column(Text, nullable=False)
    scenario_id: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_id: Mapped[str | None] = mapped_column(Text)
    market: Mapped[str | None] = mapped_column(Text)
    config_version: Mapped[str] = mapped_column(Text, nullable=False)
    data_source: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[object | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'CREATED'"))
    initial_capital: Mapped[float | None] = mapped_column(Numeric(18, 6))
    params_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    summary_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class BacktestTradeRecord(ResearchBase):
    __tablename__ = "backtest_trade"
    __table_args__ = (
        Index("ix_backtest_trade_run_id", "run_id"),
        Index("ix_backtest_trade_symbol_opened_at", "symbol", "opened_at"),
        Index("ix_backtest_trade_ticker_opened_at", "ticker_id", "opened_at"),
        Index("ix_backtest_trade_plan_key", "plan_key"),
        Index("ix_backtest_trade_fill_status", "fill_status", "closed_at"),
        {"schema": "bt_result"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    plan_key: Mapped[str | None] = mapped_column(Text)
    ticker_id: Mapped[int | None] = mapped_column(Integer)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    opened_at: Mapped[object | None] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[object | None] = mapped_column(DateTime(timezone=True))
    quantity: Mapped[float | None] = mapped_column(Numeric(18, 8))
    entry_price: Mapped[float | None] = mapped_column(Numeric(18, 8))
    exit_price: Mapped[float | None] = mapped_column(Numeric(18, 8))
    gross_pnl: Mapped[float | None] = mapped_column(Numeric(18, 8))
    net_pnl: Mapped[float | None] = mapped_column(Numeric(18, 8))
    return_pct: Mapped[float | None] = mapped_column(Numeric(18, 8))
    fill_status: Mapped[str | None] = mapped_column(Text)
    trade_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class BacktestMetricRecord(ResearchBase):
    __tablename__ = "backtest_metric"
    __table_args__ = (
        UniqueConstraint("run_id", "metric_group", "metric_name", "config_version", name="uq_backtest_metric_canonical"),
        Index("ix_backtest_metric_run_group", "run_id", "metric_group"),
        Index("ix_backtest_metric_name", "metric_name"),
        Index("ix_backtest_metric_group_name", "metric_group", "metric_name", "created_at"),
        {"schema": "bt_result"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    metric_group: Mapped[str] = mapped_column(Text, nullable=False)
    metric_name: Mapped[str] = mapped_column(Text, nullable=False)
    metric_value: Mapped[float | None] = mapped_column(Numeric(24, 10))
    metric_text: Mapped[str | None] = mapped_column(Text)
    config_version: Mapped[str] = mapped_column(Text, nullable=False)
    metric_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb"))
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
