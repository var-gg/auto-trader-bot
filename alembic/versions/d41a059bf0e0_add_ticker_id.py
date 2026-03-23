"""add ticker_id with sequence backfill

Revision ID: d41a059bf0e0
Revises: a56b3d8daeb0
Create Date: 2025-09-18 22:32:24.560639
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "d41a059bf0e0"
down_revision: Union[str, Sequence[str], None] = "a56b3d8daeb0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema with ticker_id backfill."""

    # 1) ticker.id 추가 (nullable=True로 먼저)
    op.add_column("ticker", sa.Column("id", sa.Integer(), nullable=True), schema="trading")

    # 2) 시퀀스 생성 및 기본값 연결
    op.execute("CREATE SEQUENCE IF NOT EXISTS trading.ticker_id_seq OWNED BY trading.ticker.id")
    op.execute("ALTER TABLE trading.ticker ALTER COLUMN id SET DEFAULT nextval('trading.ticker_id_seq')")

    # 3) 기존 데이터 백필 (id가 NULL인 row 채우기)
    op.execute("""
        UPDATE trading.ticker
        SET id = nextval('trading.ticker_id_seq')
        WHERE id IS NULL
    """)

    # 4) NOT NULL 전환
    op.alter_column("ticker", "id", nullable=False, schema="trading")

    # 4.5) ticker.id 에 UNIQUE 제약 추가 (FK 타겟으로 쓰기 위해)
    op.create_unique_constraint("uq_ticker_id", "ticker", ["id"], schema="trading")

    # 5) news_ticker 컬럼 추가
    op.add_column("news_ticker", sa.Column("ticker_id", sa.Integer(), nullable=True), schema="trading")
    op.add_column("news_ticker", sa.Column("ticker_exchange", sa.String(), nullable=True), schema="trading")
    op.add_column("news_ticker", sa.Column("ticker_country", sa.String(), nullable=True), schema="trading")

    # 6) ticker_i18n, ticker_industry, ticker_theme 에 ticker_id 추가
    op.add_column("ticker_i18n", sa.Column("ticker_id", sa.Integer(), nullable=True), schema="trading")
    op.add_column("ticker_industry", sa.Column("ticker_id", sa.Integer(), nullable=True), schema="trading")
    op.add_column("ticker_theme", sa.Column("ticker_id", sa.Integer(), nullable=True), schema="trading")

    # 7) 백필: 기존 symbol → id 매핑
    op.execute("""
        UPDATE trading.ticker_i18n ti
        SET ticker_id = t.id
        FROM trading.ticker t
        WHERE ti.ticker_symbol = t.symbol
    """)
    op.execute("""
        UPDATE trading.ticker_industry ti
        SET ticker_id = t.id
        FROM trading.ticker t
        WHERE ti.ticker_symbol = t.symbol
    """)
    op.execute("""
        UPDATE trading.ticker_theme tt
        SET ticker_id = t.id
        FROM trading.ticker t
        WHERE tt.ticker_symbol = t.symbol
    """)
    op.execute("""
        UPDATE trading.news_ticker nt
        SET ticker_id = t.id,
            ticker_exchange = t.exchange,
            ticker_country = t.country
        FROM trading.ticker t
        WHERE nt.ticker_symbol = t.symbol
    """)

    # 8) NOT NULL 전환
    op.alter_column("ticker_i18n", "ticker_id", nullable=False, schema="trading")
    op.alter_column("ticker_industry", "ticker_id", nullable=False, schema="trading")
    op.alter_column("ticker_theme", "ticker_id", nullable=False, schema="trading")
    op.alter_column("news_ticker", "ticker_id", nullable=False, schema="trading")
    op.alter_column("news_ticker", "ticker_exchange", nullable=False, schema="trading")

    # 9) FK & UniqueConstraint 정리
    # 기존 news_ticker → ticker.symbol FK 제거
    op.drop_constraint("news_ticker_ticker_symbol_fkey", "news_ticker", schema="trading", type_="foreignkey")

    # news_ticker unique (news_id, ticker_id)
    op.drop_constraint("uq_news_ticker__news_symbol", "news_ticker", schema="trading", type_="unique")
    op.create_unique_constraint("uq_news_ticker__news_ticker", "news_ticker", ["news_id", "ticker_id"], schema="trading")

    # 새 FK들
    op.create_foreign_key("fk_news_ticker_ticker_id", "news_ticker", "ticker",
                          ["ticker_id"], ["id"],
                          source_schema="trading", referent_schema="trading",
                          ondelete="CASCADE")
    op.create_foreign_key("fk_ticker_i18n_ticker_id", "ticker_i18n", "ticker",
                          ["ticker_id"], ["id"],
                          source_schema="trading", referent_schema="trading",
                          ondelete="CASCADE")
    op.create_foreign_key("fk_ticker_industry_ticker_id", "ticker_industry", "ticker",
                          ["ticker_id"], ["id"],
                          source_schema="trading", referent_schema="trading",
                          ondelete="CASCADE")
    op.create_foreign_key("fk_ticker_theme_ticker_id", "ticker_theme", "ticker",
                          ["ticker_id"], ["id"],
                          source_schema="trading", referent_schema="trading",
                          ondelete="CASCADE")

    # 10) 유니크 유지
    op.create_unique_constraint("uq_ticker_symbol_exchange", "ticker", ["symbol", "exchange"], schema="trading")


def downgrade() -> None:
    """Downgrade schema (reverse backfill)."""

    # FK 제거
    op.drop_constraint("fk_ticker_theme_ticker_id", "ticker_theme", schema="trading", type_="foreignkey")
    op.drop_constraint("fk_ticker_industry_ticker_id", "ticker_industry", schema="trading", type_="foreignkey")
    op.drop_constraint("fk_ticker_i18n_ticker_id", "ticker_i18n", schema="trading", type_="foreignkey")
    op.drop_constraint("fk_news_ticker_ticker_id", "news_ticker", schema="trading", type_="foreignkey")

    # unique 제거
    op.drop_constraint("uq_news_ticker__news_ticker", "news_ticker", schema="trading", type_="unique")
    op.create_unique_constraint("uq_news_ticker__news_symbol", "news_ticker", ["news_id", "ticker_symbol"], schema="trading")

    # 컬럼 제거
    op.drop_column("news_ticker", "ticker_country", schema="trading")
    op.drop_column("news_ticker", "ticker_exchange", schema="trading")
    op.drop_column("news_ticker", "ticker_id", schema="trading")
    op.drop_column("ticker_theme", "ticker_id", schema="trading")
    op.drop_column("ticker_industry", "ticker_id", schema="trading")
    op.drop_column("ticker_i18n", "ticker_id", schema="trading")
    op.drop_constraint("uq_ticker_symbol_exchange", "ticker", schema="trading", type_="unique")
    op.drop_column("ticker", "id", schema="trading")

    # FK 복원
    op.create_foreign_key("news_ticker_ticker_symbol_fkey", "news_ticker", "ticker",
                          ["ticker_symbol"], ["symbol"],
                          source_schema="trading", referent_schema="trading",
                          ondelete="CASCADE")

    # 시퀀스 삭제
    op.execute("DROP SEQUENCE IF EXISTS trading.ticker_id_seq")
