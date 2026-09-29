"""add active to binance_kline_alert_subscriptions

Revision ID: d1a7b9f4a22e
Revises: c0ffee1234ab
Create Date: 2026-03-23

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d1a7b9f4a22e"
down_revision: str | None = "c0ffee1234ab"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "binance_kline_alert_subscriptions",
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
    )


def downgrade() -> None:
    op.drop_column("binance_kline_alert_subscriptions", "active")

