"""add active to binance_kline_alert_subscriptions

Revision ID: d1a7b9f4a22e
Revises: c0ffee1234ab
Create Date: 2026-03-23

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d1a7b9f4a22e"
down_revision: Union[str, None] = "c0ffee1234ab"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "binance_kline_alert_subscriptions",
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
    )


def downgrade() -> None:
    op.drop_column("binance_kline_alert_subscriptions", "active")

