"""create binance_kline_alert_subscriptions tbl

Revision ID: c0ffee1234ab
Revises: 117418b29850
Create Date: 2026-03-20

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c0ffee1234ab"
down_revision: Union[str, None] = "117418b29850"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "binance_kline_alert_subscriptions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("window_size", sa.String(length=20), nullable=False),
        sa.Column("diff_percent", sa.Float(), nullable=False),
        sa.Column("create_at", sa.DateTime(), server_default=sa.text("(CURRENT_TIMESTAMP)"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "window_size", name="uq_user_window_size"),
    )


def downgrade() -> None:
    op.drop_table("binance_kline_alert_subscriptions")

