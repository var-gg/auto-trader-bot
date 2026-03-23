"""add pm decision history tables

Revision ID: 7aa47b9ec4da
Revises: 1ae400988501
Create Date: 2026-02-26 18:02:43.066102

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '7aa47b9ec4da'
down_revision: Union[str, Sequence[str], None] = '1ae400988501'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "trading"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "pm_signal_run_header",
        sa.Column("run_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_key", sa.String(length=64), nullable=False),
        sa.Column("session_type", sa.String(length=16), nullable=False),
        sa.Column("anchor_date", sa.Date(), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("config_id", sa.Integer(), nullable=True),
        sa.Column("policy_version", sa.String(length=32), nullable=True),
        sa.Column("code_version", sa.String(length=64), nullable=True),
        sa.Column("country", sa.String(length=8), nullable=True),
        sa.Column("regime_code", sa.String(length=32), nullable=True),
        sa.Column("regime_features", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("run_meta", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["config_id"], [f"{SCHEMA}.optuna_vector_config.id"], ondelete="SET NULL"),
        sa.CheckConstraint("session_type IN ('KR_OPEN', 'US_OPEN', 'ADHOC', 'RETRY')", name="ck_pm_signal_run_header_session_type"),
        sa.UniqueConstraint("run_key", name="uq_pm_signal_run_header_run_key"),
        schema=SCHEMA,
    )
    op.create_index("ix_pm_signal_run_header_anchor_executed", "pm_signal_run_header", ["anchor_date", "executed_at"], unique=False, schema=SCHEMA)

    op.create_table(
        "pm_signal_snapshot_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("ticker_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("company_name", sa.Text(), nullable=True),
        sa.Column("signal_1d", sa.Float(precision=24), nullable=False),
        sa.Column("best_target_id", sa.Integer(), nullable=True),
        sa.Column("best_direction", sa.String(length=8), nullable=True),
        sa.Column("p_up", sa.Float(), nullable=True),
        sa.Column("p_down", sa.Float(), nullable=True),
        sa.Column("margin", sa.Float(), nullable=True),
        sa.Column("top_up_score", sa.Float(), nullable=True),
        sa.Column("top_down_score", sa.Float(), nullable=True),
        sa.Column("reason_code", sa.String(length=32), nullable=True),
        sa.Column("reason_text", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["run_id"], [f"{SCHEMA}.pm_signal_run_header.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ticker_id"], [f"{SCHEMA}.ticker.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["best_target_id"], [f"{SCHEMA}.optuna_target_vectors.id"], ondelete="SET NULL"),
        sa.CheckConstraint("best_direction IN ('UP', 'DOWN')", name="ck_pm_signal_snapshot_history_best_direction"),
        schema=SCHEMA,
    )
    op.create_index("ix_pm_signal_snapshot_history_run", "pm_signal_snapshot_history", ["run_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_pm_signal_snapshot_history_ticker_run", "pm_signal_snapshot_history", ["ticker_id", "run_id"], unique=False, schema=SCHEMA)

    op.create_table(
        "pm_candidate_decision_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("ticker_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("passed_gate", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("excluded_reason_code", sa.String(length=32), nullable=True),
        sa.Column("excluded_reason_text", sa.Text(), nullable=True),
        sa.Column("action_code", sa.String(length=16), nullable=False, server_default="SKIP"),
        sa.Column("discount_bps_suggested", sa.Integer(), nullable=True),
        sa.Column("ladder_params_used", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("risk_limits_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("explanation_short", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["run_id"], [f"{SCHEMA}.pm_signal_run_header.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ticker_id"], [f"{SCHEMA}.ticker.id"], ondelete="RESTRICT"),
        sa.CheckConstraint("action_code IN ('BUY', 'SKIP', 'HOLD', 'REDUCE')", name="ck_pm_candidate_decision_history_action_code"),
        sa.UniqueConstraint("run_id", "ticker_id", name="uq_pm_candidate_decision_history_run_ticker"),
        schema=SCHEMA,
    )
    op.create_index("ix_pm_candidate_decision_history_run", "pm_candidate_decision_history", ["run_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_pm_candidate_decision_history_action", "pm_candidate_decision_history", ["action_code", "run_id"], unique=False, schema=SCHEMA)

    op.create_table(
        "pm_order_execution_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("ticker_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("action_code", sa.String(length=16), nullable=False),
        sa.Column("order_outcome_code", sa.String(length=32), nullable=False),
        sa.Column("order_id", sa.String(length=64), nullable=True),
        sa.Column("order_type", sa.String(length=16), nullable=True),
        sa.Column("intent_qty", sa.Float(), nullable=True),
        sa.Column("intent_price", sa.Float(), nullable=True),
        sa.Column("filled_qty", sa.Float(), nullable=True),
        sa.Column("avg_fill_price", sa.Float(), nullable=True),
        sa.Column("slippage_bps", sa.Float(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["run_id"], [f"{SCHEMA}.pm_signal_run_header.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ticker_id"], [f"{SCHEMA}.ticker.id"], ondelete="RESTRICT"),
        sa.CheckConstraint("action_code IN ('BUY', 'SKIP', 'HOLD', 'REDUCE')", name="ck_pm_order_execution_history_action_code"),
        schema=SCHEMA,
    )
    op.create_index("ix_pm_order_execution_history_run", "pm_order_execution_history", ["run_id"], unique=False, schema=SCHEMA)
    op.create_index("ix_pm_order_execution_history_outcome", "pm_order_execution_history", ["order_outcome_code", "run_id"], unique=False, schema=SCHEMA)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_pm_order_execution_history_outcome", table_name="pm_order_execution_history", schema=SCHEMA)
    op.drop_index("ix_pm_order_execution_history_run", table_name="pm_order_execution_history", schema=SCHEMA)
    op.drop_table("pm_order_execution_history", schema=SCHEMA)

    op.drop_index("ix_pm_candidate_decision_history_action", table_name="pm_candidate_decision_history", schema=SCHEMA)
    op.drop_index("ix_pm_candidate_decision_history_run", table_name="pm_candidate_decision_history", schema=SCHEMA)
    op.drop_table("pm_candidate_decision_history", schema=SCHEMA)

    op.drop_index("ix_pm_signal_snapshot_history_ticker_run", table_name="pm_signal_snapshot_history", schema=SCHEMA)
    op.drop_index("ix_pm_signal_snapshot_history_run", table_name="pm_signal_snapshot_history", schema=SCHEMA)
    op.drop_table("pm_signal_snapshot_history", schema=SCHEMA)

    op.drop_index("ix_pm_signal_run_header_anchor_executed", table_name="pm_signal_run_header", schema=SCHEMA)
    op.drop_table("pm_signal_run_header", schema=SCHEMA)
