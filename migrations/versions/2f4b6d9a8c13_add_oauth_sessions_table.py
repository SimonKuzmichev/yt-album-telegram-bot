"""add oauth sessions table

Revision ID: 2f4b6d9a8c13
Revises: 4c1f9a52b7d2
Create Date: 2026-03-15 17:01:30.543020

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2f4b6d9a8c13"
down_revision: Union[str, Sequence[str], None] = "4c1f9a52b7d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "oauth_sessions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("code_verifier", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("requested_chat_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.users.id"],
            ondelete="CASCADE",
            name="oauth_sessions_user_id_fkey",
        ),
        sa.UniqueConstraint("state", name="uq_oauth_sessions_state"),
        schema="app",
    )

    op.create_index(
        "idx_oauth_sessions_provider_status_expires_at",
        "oauth_sessions",
        ["provider", "status", "expires_at"],
        unique=False,
        schema="app",
    )
    op.create_index(
        "idx_oauth_sessions_user_id_created_at",
        "oauth_sessions",
        ["user_id", "created_at"],
        unique=False,
        schema="app",
    )
    op.create_check_constraint(
        "ck_oauth_sessions_status",
        "oauth_sessions",
        "status IN ('pending', 'consumed', 'expired', 'failed')",
        schema="app",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "ck_oauth_sessions_status",
        "oauth_sessions",
        schema="app",
        type_="check",
    )
    op.drop_index(
        "idx_oauth_sessions_user_id_created_at",
        table_name="oauth_sessions",
        schema="app",
    )
    op.drop_index(
        "idx_oauth_sessions_provider_status_expires_at",
        table_name="oauth_sessions",
        schema="app",
    )
    op.drop_table("oauth_sessions", schema="app")
