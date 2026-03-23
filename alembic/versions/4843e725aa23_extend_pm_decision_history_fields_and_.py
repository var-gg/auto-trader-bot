"""extend pm decision history fields and add tplus outcome table

Revision ID: 4843e725aa23
Revises: 7aa47b9ec4da
Create Date: 2026-02-27 11:31:38.193590

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4843e725aa23'
down_revision: Union[str, Sequence[str], None] = '7aa47b9ec4da'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "trading"


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "pm_candidate_decision_history",
        sa.Column("intended_limit_price", sa.Float(precision=53), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "pm_candidate_decision_history",
        sa.Column("submitted_price", sa.Float(precision=53), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "pm_candidate_decision_history",
        sa.Column("unfilled_reason_code", sa.String(length=32), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "pm_candidate_decision_history",
        sa.Column("unfilled_reason_text", sa.Text(), nullable=True),
        schema=SCHEMA,
    )

    op.add_column(
        "pm_order_execution_history",
        sa.Column("intended_limit_price", sa.Float(precision=53), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "pm_order_execution_history",
        sa.Column("submitted_price", sa.Float(precision=53), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "pm_order_execution_history",
        sa.Column("unfilled_reason_code", sa.String(length=32), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "pm_order_execution_history",
        sa.Column("unfilled_reason_text", sa.Text(), nullable=True),
        schema=SCHEMA,
    )

    op.create_table(
        "pm_outcome_tplus_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("ticker_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("horizon_days", sa.Integer(), nullable=False),
        sa.Column("ref_price", sa.Float(precision=53), nullable=True),
        sa.Column("outcome_price", sa.Float(precision=53), nullable=True),
        sa.Column("pnl_bps", sa.Float(precision=53), nullable=True),
        sa.Column("label_code", sa.String(length=16), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["run_id"], [f"{SCHEMA}.pm_signal_run_header.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ticker_id"], [f"{SCHEMA}.ticker.id"], ondelete="RESTRICT"),
        sa.CheckConstraint("horizon_days IN (1, 3, 5)", name="ck_pm_outcome_tplus_history_horizon_days"),
        sa.CheckConstraint("label_code IN ('WIN', 'LOSS', 'FLAT')", name="ck_pm_outcome_tplus_history_label_code"),
        sa.UniqueConstraint("run_id", "ticker_id", "horizon_days", name="uq_pm_outcome_tplus_history_run_ticker_horizon"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_pm_outcome_tplus_history_run",
        "pm_outcome_tplus_history",
        ["run_id"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_pm_outcome_tplus_history_run", table_name="pm_outcome_tplus_history", schema=SCHEMA)
    op.drop_table("pm_outcome_tplus_history", schema=SCHEMA)

    op.drop_column("pm_order_execution_history", "unfilled_reason_text", schema=SCHEMA)
    op.drop_column("pm_order_execution_history", "unfilled_reason_code", schema=SCHEMA)
    op.drop_column("pm_order_execution_history", "submitted_price", schema=SCHEMA)
    op.drop_column("pm_order_execution_history", "intended_limit_price", schema=SCHEMA)

    op.drop_column("pm_candidate_decision_history", "unfilled_reason_text", schema=SCHEMA)
    op.drop_column("pm_candidate_decision_history", "unfilled_reason_code", schema=SCHEMA)
    op.drop_column("pm_candidate_decision_history", "submitted_price", schema=SCHEMA)
    op.drop_column("pm_candidate_decision_history", "intended_limit_price", schema=SCHEMA)
