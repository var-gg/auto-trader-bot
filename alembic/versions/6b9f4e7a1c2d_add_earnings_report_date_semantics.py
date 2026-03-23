"""add earnings report date semantics

Revision ID: 6b9f4e7a1c2d
Revises: 29d2b24e7565
Create Date: 2026-03-13 17:58:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6b9f4e7a1c2d'
down_revision: Union[str, Sequence[str], None] = '29d2b24e7565'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('earnings_event', sa.Column('confirmed_report_date', sa.Date(), nullable=True, comment='실제 또는 고신뢰 확정 실적 발표일'), schema='trading')
    op.add_column('earnings_event', sa.Column('expected_report_date_start', sa.Date(), nullable=True, comment='예상 실적 발표 시작일'), schema='trading')
    op.add_column('earnings_event', sa.Column('expected_report_date_end', sa.Date(), nullable=True, comment='예상 실적 발표 종료일'), schema='trading')
    op.add_column('earnings_event', sa.Column('report_date_confidence', sa.Float(), nullable=True, comment='발표일 신뢰도(0~1)'), schema='trading')
    op.add_column('earnings_event', sa.Column('report_date_kind', sa.String(length=20), nullable=True, comment='발표일 의미(confirmed/expected/legacy)'), schema='trading')

    # 기초 backfill: 기존 report_date/status를 새 의미 필드로 이관
    op.execute("""
        UPDATE trading.earnings_event
        SET confirmed_report_date = report_date,
            report_date_confidence = COALESCE(report_date_confidence, 0.95),
            report_date_kind = COALESCE(report_date_kind, 'confirmed')
        WHERE report_date IS NOT NULL
          AND actual_eps IS NOT NULL
          AND confirmed_report_date IS NULL
    """)

    op.execute("""
        UPDATE trading.earnings_event
        SET expected_report_date_start = report_date,
            expected_report_date_end = report_date,
            report_date_confidence = COALESCE(report_date_confidence, 0.60),
            report_date_kind = COALESCE(report_date_kind, 'expected')
        WHERE report_date IS NOT NULL
          AND actual_eps IS NULL
          AND expected_report_date_start IS NULL
          AND confirmed_report_date IS NULL
    """)

    op.execute("""
        UPDATE trading.earnings_event
        SET report_date_kind = COALESCE(report_date_kind, 'legacy')
        WHERE report_date IS NOT NULL
          AND report_date_kind IS NULL
    """)


def downgrade() -> None:
    op.drop_column('earnings_event', 'report_date_kind', schema='trading')
    op.drop_column('earnings_event', 'report_date_confidence', schema='trading')
    op.drop_column('earnings_event', 'expected_report_date_end', schema='trading')
    op.drop_column('earnings_event', 'expected_report_date_start', schema='trading')
    op.drop_column('earnings_event', 'confirmed_report_date', schema='trading')
