"""add provider auth metadata columns

Revision ID: 7a3d2d8f1b6c
Revises: 2f4b6d9a8c13
Create Date: 2026-03-15 12:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7a3d2d8f1b6c"
down_revision: Union[str, Sequence[str], None] = "2f4b6d9a8c13"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "user_provider_accounts",
        sa.Column("granted_scope", sa.Text(), nullable=True),
        schema="app",
    )
    op.add_column(
        "user_provider_accounts",
        sa.Column("last_auth_at", sa.TIMESTAMP(timezone=True), nullable=True),
        schema="app",
    )
    op.add_column(
        "user_provider_accounts",
        sa.Column("last_refresh_at", sa.TIMESTAMP(timezone=True), nullable=True),
        schema="app",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("user_provider_accounts", "last_refresh_at", schema="app")
    op.drop_column("user_provider_accounts", "last_auth_at", schema="app")
    op.drop_column("user_provider_accounts", "granted_scope", schema="app")
