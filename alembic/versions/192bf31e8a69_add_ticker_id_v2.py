"""add ticker_id v2 (safe phase2 cleanup)

Revision ID: 192bf31e8a69
Revises: d41a059bf0e0
Create Date: 2025-09-18 22:49:11.221602
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "192bf31e8a69"
down_revision: Union[str, Sequence[str], None] = "d41a059bf0e0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Phase2 (안전판):
    - 심볼 기반 FK/컬럼이 '있으면'만 제거 (IF EXISTS)
    - ticker PK를 symbol → id 로 전환 (스키마 명시 필수)
    - Phase1에서 이미 ticker_id FK가 생성/백필되어 있다는 전제
    """

    # 0) 심볼 기반 FK들 제거 (존재할 때만)
    op.execute("ALTER TABLE trading.ticker_i18n        DROP CONSTRAINT IF EXISTS ticker_i18n_ticker_symbol_fkey")
    op.execute("ALTER TABLE trading.ticker_industry    DROP CONSTRAINT IF EXISTS ticker_industry_ticker_symbol_fkey")
    op.execute("ALTER TABLE trading.ticker_theme       DROP CONSTRAINT IF EXISTS ticker_theme_ticker_symbol_fkey")
    op.execute("ALTER TABLE trading.ticker_fundamentals DROP CONSTRAINT IF EXISTS ticker_fundamentals_ticker_symbol_fkey")
    op.execute("ALTER TABLE trading.news_ticker        DROP CONSTRAINT IF EXISTS news_ticker_ticker_symbol_fkey")

    # 1) ticker PK 전환 (symbol → id)
    op.drop_constraint("ticker_pkey", "ticker", schema="trading", type_="primary")
    op.create_primary_key("ticker_pkey", "ticker", ["id"], schema="trading")

    # 2) 심볼 기반 컬럼 정리 (존재할 때만)
    op.execute("ALTER TABLE trading.ticker_i18n        DROP COLUMN IF EXISTS ticker_symbol")
    op.execute("ALTER TABLE trading.ticker_industry    DROP COLUMN IF EXISTS ticker_symbol")
    op.execute("ALTER TABLE trading.ticker_theme       DROP COLUMN IF EXISTS ticker_symbol")
    op.execute("ALTER TABLE trading.ticker_fundamentals DROP COLUMN IF EXISTS ticker_symbol")

    # news_ticker 잔여 컬럼 정리
    op.execute("ALTER TABLE trading.news_ticker        DROP COLUMN IF EXISTS ticker_symbol")
    op.execute("ALTER TABLE trading.news_ticker        DROP COLUMN IF EXISTS ticker_exchange")
    op.execute("ALTER TABLE trading.news_ticker        DROP COLUMN IF EXISTS ticker_country")

    # ⚠️ Phase1에서 ticker_id 기반 FK는 이미 만들어져 있으므로 여기서 FK 생성은 하지 않습니다.


def downgrade() -> None:
    """
    되돌리기(참고용): id PK → symbol PK 복구 + 심볼 기반 FK/컬럼 복원
    (실운영에선 거의 사용하지 않겠지만, 논리적으로 맞춰둠)
    """
    # 1) ticker PK 복원
    op.drop_constraint("ticker_pkey", "ticker", schema="trading", type_="primary")
    op.create_primary_key("ticker_pkey", "ticker", ["symbol"], schema="trading")

    # 2) 컬럼 복원
    op.add_column("ticker_i18n",        sa.Column("ticker_symbol", sa.String(), nullable=True), schema="trading")
    op.add_column("ticker_industry",    sa.Column("ticker_symbol", sa.String(), nullable=True), schema="trading")
    op.add_column("ticker_theme",       sa.Column("ticker_symbol", sa.String(), nullable=True), schema="trading")
    op.add_column("ticker_fundamentals", sa.Column("ticker_symbol", sa.String(), nullable=True), schema="trading")

    op.add_column("news_ticker", sa.Column("ticker_symbol",  sa.String(), nullable=True), schema="trading")
    op.add_column("news_ticker", sa.Column("ticker_exchange", sa.String(), nullable=True), schema="trading")
    op.add_column("news_ticker", sa.Column("ticker_country",  sa.String(), nullable=True), schema="trading")

    # 3) 심볼 기반 FK 복구
    op.create_foreign_key("ticker_i18n_ticker_symbol_fkey",        "ticker_i18n",        "ticker", ["ticker_symbol"], ["symbol"],  source_schema="trading", referent_schema="trading")
    op.create_foreign_key("ticker_industry_ticker_symbol_fkey",    "ticker_industry",    "ticker", ["ticker_symbol"], ["symbol"],  source_schema="trading", referent_schema="trading")
    op.create_foreign_key("ticker_theme_ticker_symbol_fkey",       "ticker_theme",       "ticker", ["ticker_symbol"], ["symbol"],  source_schema="trading", referent_schema="trading")
    op.create_foreign_key("ticker_fundamentals_ticker_symbol_fkey","ticker_fundamentals","ticker", ["ticker_symbol"], ["symbol"],  source_schema="trading", referent_schema="trading")
    op.create_foreign_key("news_ticker_ticker_symbol_fkey",        "news_ticker",        "ticker", ["ticker_symbol"], ["symbol"],  source_schema="trading", referent_schema="trading", ondelete="CASCADE")
