"""add market headline risk snapshot

Revision ID: b91c2aa4d9f1
Revises: 4843e725aa23
Create Date: 2026-03-04 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'b91c2aa4d9f1'
down_revision: Union[str, Sequence[str], None] = '4843e725aa23'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'market_headline_risk_snapshot',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('market_scope', sa.String(length=16), nullable=False),
        sa.Column('as_of_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('window_minutes', sa.Integer(), nullable=False),
        sa.Column('risk_score', sa.Integer(), nullable=False),
        sa.Column('confidence', sa.Float(), nullable=False),
        sa.Column('shock_type', sa.String(length=32), nullable=False),
        sa.Column('severity_band', sa.String(length=16), nullable=False),
        sa.Column('discount_multiplier', sa.Float(), nullable=False),
        sa.Column('ttl_minutes', sa.Integer(), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('source_provider', sa.String(length=32), nullable=False),
        sa.Column('model_name', sa.String(length=128), nullable=True),
        sa.Column('raw_response', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('reason_short', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.CheckConstraint("market_scope IN ('KR','US','GLOBAL')", name='ck_mrhs_market_scope'),
        sa.CheckConstraint('risk_score >= 0 AND risk_score <= 100', name='ck_mrhs_risk_score_range'),
        sa.CheckConstraint('confidence >= 0 AND confidence <= 1', name='ck_mrhs_confidence_range'),
        sa.CheckConstraint('discount_multiplier >= 1.0', name='ck_mrhs_discount_multiplier_min'),
        sa.PrimaryKeyConstraint('id'),
        schema='trading'
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_mrhs_scope_asof_desc "
        "ON trading.market_headline_risk_snapshot (market_scope, as_of_at DESC)"
    )
    op.create_index('ix_mrhs_expires_at', 'market_headline_risk_snapshot', ['expires_at'], unique=False, schema='trading')


def downgrade() -> None:
    op.drop_index('ix_mrhs_expires_at', table_name='market_headline_risk_snapshot', schema='trading')
    op.drop_index('ix_mrhs_scope_asof_desc', table_name='market_headline_risk_snapshot', schema='trading')
    op.drop_table('market_headline_risk_snapshot', schema='trading')
