"""make user.username nullable

Revision ID: b2f4c9d1e7a0
Revises: d1a7b9f4a22e
Create Date: 2026-03-24

Username в Telegram может отсутствовать, поэтому колонка должна допускать NULL.
Отдельная миграция нужна, т.к. в create_user_table username был NOT NULL.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'b2f4c9d1e7a0'
down_revision: str | None = 'd1a7b9f4a22e'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('user') as batch_op:
        batch_op.alter_column(
            'username',
            existing_type=sa.String(),
            nullable=True,
        )


def downgrade() -> None:
    with op.batch_alter_table('user') as batch_op:
        batch_op.alter_column(
            'username',
            existing_type=sa.String(),
            nullable=False,
        )
