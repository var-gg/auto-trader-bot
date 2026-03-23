"""merge heads

Revision ID: 4a72c8635c56
Revises: 6f7fc27e2699, fa5b928617aa
Create Date: 2025-10-14 03:23:33.184931

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4a72c8635c56'
down_revision: Union[str, Sequence[str], None] = ('6f7fc27e2699', 'fa5b928617aa')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
