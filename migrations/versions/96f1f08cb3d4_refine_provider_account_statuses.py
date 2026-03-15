"""refine provider account statuses

Revision ID: 96f1f08cb3d4
Revises: 7a3d2d8f1b6c
Create Date: 2026-03-15 13:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "96f1f08cb3d4"
down_revision: Union[str, Sequence[str], None] = "7a3d2d8f1b6c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        UPDATE app.user_provider_accounts
        SET status = 'pending_oauth'
        WHERE status = 'pending';
        """
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
        server_default=sa.text("'connected'"),
    )
    op.create_check_constraint(
        "ck_user_provider_accounts_status",
        "user_provider_accounts",
        "status IN ('pending_oauth', 'connected', 'token_expired', 'needs_reauth', 'disabled')",
        schema="app",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "ck_user_provider_accounts_status",
        "user_provider_accounts",
        schema="app",
        type_="check",
    )
    op.create_check_constraint(
        "ck_user_provider_accounts_status",
        "user_provider_accounts",
        "status IN ('pending', 'connected', 'needs_reauth', 'disabled')",
        schema="app",
    )
    op.execute(
        """
        UPDATE app.user_provider_accounts
        SET status = 'pending'
        WHERE status = 'pending_oauth';
        """
    )
