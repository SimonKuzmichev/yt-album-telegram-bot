"""add processing oauth session status

Revision ID: a5b8c13d9f24
Revises: 96f1f08cb3d4
Create Date: 2026-03-16 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a5b8c13d9f24"
down_revision: Union[str, Sequence[str], None] = "96f1f08cb3d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(
        "ck_oauth_sessions_status",
        "oauth_sessions",
        schema="app",
        type_="check",
    )
    op.create_check_constraint(
        "ck_oauth_sessions_status",
        "oauth_sessions",
        "status IN ('pending', 'processing', 'consumed', 'expired', 'failed')",
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
    op.create_check_constraint(
        "ck_oauth_sessions_status",
        "oauth_sessions",
        "status IN ('pending', 'consumed', 'expired', 'failed')",
        schema="app",
    )
