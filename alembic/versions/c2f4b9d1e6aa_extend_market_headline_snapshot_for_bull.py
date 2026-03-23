"""extend market headline snapshot for bull regime

Revision ID: c2f4b9d1e6aa
Revises: b91c2aa4d9f1
Create Date: 2026-03-04 13:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c2f4b9d1e6aa'
down_revision: Union[str, Sequence[str], None] = 'b91c2aa4d9f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('market_headline_risk_snapshot', sa.Column('sell_markup_multiplier', sa.Float(), nullable=False, server_default='1.0'), schema='trading')
    op.add_column('market_headline_risk_snapshot', sa.Column('regime_score', sa.Integer(), nullable=False, server_default='0'), schema='trading')
    op.create_check_constraint('ck_mrhs_sell_markup_multiplier_min', 'market_headline_risk_snapshot', 'sell_markup_multiplier >= 1.0', schema='trading')
    op.create_check_constraint('ck_mrhs_regime_score_range', 'market_headline_risk_snapshot', 'regime_score >= -100 AND regime_score <= 100', schema='trading')


def downgrade() -> None:
    op.drop_constraint('ck_mrhs_regime_score_range', 'market_headline_risk_snapshot', schema='trading', type_='check')
    op.drop_constraint('ck_mrhs_sell_markup_multiplier_min', 'market_headline_risk_snapshot', schema='trading', type_='check')
    op.drop_column('market_headline_risk_snapshot', 'regime_score', schema='trading')
    op.drop_column('market_headline_risk_snapshot', 'sell_markup_multiplier', schema='trading')
