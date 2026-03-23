"""add enum_upper

Revision ID: d4a8df41d878
Revises: fd7bf907382e
Create Date: 2025-09-19 00:37:07.875536

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4a8df41d878'
down_revision: Union[str, Sequence[str], None] = 'fd7bf907382e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. 기존 FK 전부 삭제
    op.drop_constraint("fk_news_ticker_ticker_id", "news_ticker", schema="trading", type_="foreignkey")
    op.drop_constraint("fk_ticker_i18n_ticker_id", "ticker_i18n", schema="trading", type_="foreignkey")
    op.drop_constraint("fk_ticker_industry_ticker_id", "ticker_industry", schema="trading", type_="foreignkey")
    op.drop_constraint("fk_ticker_theme_ticker_id", "ticker_theme", schema="trading", type_="foreignkey")

    # 2. 불필요한 Unique 제약조건 삭제
    op.drop_constraint("uq_ticker_id", "ticker", schema="trading", type_="unique")

    # 3. FK들을 다시 생성하되 PK(id)를 참조하도록 변경
    op.create_foreign_key(
        "fk_news_ticker_ticker_id", 
        "news_ticker", "ticker", 
        ["ticker_id"], ["id"], 
        source_schema="trading", referent_schema="trading"
    )
    op.create_foreign_key(
        "fk_ticker_i18n_ticker_id", 
        "ticker_i18n", "ticker", 
        ["ticker_id"], ["id"], 
        source_schema="trading", referent_schema="trading"
    )
    op.create_foreign_key(
        "fk_ticker_industry_ticker_id", 
        "ticker_industry", "ticker", 
        ["ticker_id"], ["id"], 
        source_schema="trading", referent_schema="trading"
    )
    op.create_foreign_key(
        "fk_ticker_theme_ticker_id", 
        "ticker_theme", "ticker", 
        ["ticker_id"], ["id"], 
        source_schema="trading", referent_schema="trading"
    )



def downgrade() -> None:
    """Downgrade schema."""
    
