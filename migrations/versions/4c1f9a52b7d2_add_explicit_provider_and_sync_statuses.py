"""add explicit provider and sync statuses

Revision ID: 4c1f9a52b7d2
Revises: 00fce10546a1
Create Date: 2026-03-11 10:42:13.602409

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4c1f9a52b7d2"
down_revision: Union[str, Sequence[str], None] = "00fce10546a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "user_provider_sync_state",
        sa.Column("last_sync_result", sa.Text(), nullable=True),
        schema="app",
    )

    op.execute(
        """
        UPDATE app.user_provider_accounts
        SET status = CASE
            WHEN status = 'active' THEN 'connected'
            ELSE status
        END;
        """
    )

    op.execute(
        """
        UPDATE app.user_provider_sync_state AS s
        SET last_sync_result = CASE
            WHEN a.status = 'needs_reauth' AND s.last_error IS NOT NULL THEN 'auth_error'
            WHEN s.last_error IS NOT NULL THEN 'transient_error'
            WHEN s.last_successful_sync_at IS NOT NULL AND COALESCE(s.library_item_count, 0) = 0 THEN 'empty_library'
            WHEN s.last_successful_sync_at IS NOT NULL THEN 'ok'
            ELSE NULL
        END
        FROM app.user_provider_accounts AS a
        WHERE a.id = s.user_provider_account_id;
        """
    )

    op.alter_column(
        "user_provider_accounts",
        "status",
        schema="app",
        existing_type=sa.Text(),
        server_default=sa.text("'connected'"),
    )

    op.create_check_constraint(
        "ck_user_provider_accounts_status",
        "user_provider_accounts",
        "status IN ('pending', 'connected', 'needs_reauth', 'disabled')",
        schema="app",
    )
    op.create_check_constraint(
        "ck_user_provider_sync_state_last_sync_result",
        "user_provider_sync_state",
        "last_sync_result IS NULL OR last_sync_result IN ('ok', 'transient_error', 'auth_error', 'empty_library')",
        schema="app",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "ck_user_provider_sync_state_last_sync_result",
        "user_provider_sync_state",
        schema="app",
        type_="check",
    )
    op.drop_constraint(
        "ck_user_provider_accounts_status",
        "user_provider_accounts",
        schema="app",
        type_="check",
    )

    op.alter_column(
        "user_provider_accounts",
        "status",
        schema="app",
        existing_type=sa.Text(),
        server_default=sa.text("'active'"),
    )

    op.execute(
        """
        UPDATE app.user_provider_accounts
        SET status = CASE
            WHEN status = 'connected' THEN 'active'
            ELSE status
        END;
        """
    )

    op.drop_column("user_provider_sync_state", "last_sync_result", schema="app")
