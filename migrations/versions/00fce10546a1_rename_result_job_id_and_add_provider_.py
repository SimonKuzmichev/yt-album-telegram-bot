"""rename result_job_id and add provider account tables

Revision ID: 00fce10546a1
Revises: fcd3705b4835
Create Date: 2026-03-10 09:08:40.047880

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy.dialects.postgresql import JSONB
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '00fce10546a1'
down_revision: Union[str, Sequence[str], None] = 'fcd3705b4835'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1) Rename result_job_id -> job_id
    op.execute(
        """
        ALTER TABLE app.idempotency_keys
        RENAME COLUMN result_job_id TO job_id;
        """
    )

    # 2) app.user_provider_accounts
    op.create_table(
        "user_provider_accounts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")),
        sa.Column("credentials_encrypted", sa.Text(), nullable=True),
        sa.Column("token_expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app.users.id"],
            ondelete="CASCADE",
            name="user_provider_accounts_user_id_fkey",
        ),
        sa.UniqueConstraint(
            "user_id",
            "provider",
            name="uq_user_provider_accounts_user_provider",
        ),
        schema="app",
    )

    op.create_index(
        "idx_user_provider_accounts_user_active",
        "user_provider_accounts",
        ["user_id", "is_active"],
        unique=False,
        schema="app",
    )

    op.create_index(
        "idx_user_provider_accounts_provider_status",
        "user_provider_accounts",
        ["provider", "status"],
        unique=False,
        schema="app",
    )
    op.create_index(
        "uq_user_provider_accounts_one_active_per_user",
        "user_provider_accounts",
        ["user_id"],
        unique=True,
        schema="app",
        postgresql_where=sa.text("is_active = TRUE"),
    )

    # 3) app.user_provider_sync_state
    op.create_table(
        "user_provider_sync_state",
        sa.Column("user_provider_account_id", sa.BigInteger(), nullable=False),
        sa.Column("last_sync_started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_sync_finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_successful_sync_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("library_item_count", sa.Integer(), nullable=True),
        sa.Column("provider_cursor", sa.Text(), nullable=True),
        sa.Column("provider_etag", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["user_provider_account_id"],
            ["app.user_provider_accounts.id"],
            ondelete="CASCADE",
            name="user_provider_sync_state_account_id_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "user_provider_account_id",
            name="user_provider_sync_state_pkey",
        ),
        schema="app",
    )

    # 4) app.user_library_albums
    op.create_table(
        "user_library_albums",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True, nullable=False),
        sa.Column("user_provider_account_id", sa.BigInteger(), nullable=False),
        sa.Column("provider_album_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("artist", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("release_year", sa.Integer(), nullable=True),
        sa.Column("raw_payload_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("first_seen_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("is_available", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")),
        sa.ForeignKeyConstraint(
            ["user_provider_account_id"],
            ["app.user_provider_accounts.id"],
            ondelete="CASCADE",
            name="user_library_albums_account_id_fkey",
        ),
        sa.UniqueConstraint(
            "user_provider_account_id",
            "provider_album_id",
            name="uq_user_library_albums_account_provider_album",
        ),
        schema="app",
    )

    op.create_index(
        "idx_user_library_albums_account_available",
        "user_library_albums",
        ["user_provider_account_id", "is_available"],
        unique=False,
        schema="app",
    )

    op.create_index(
        "idx_user_library_albums_last_seen_at",
        "user_library_albums",
        ["last_seen_at"],
        unique=False,
        schema="app",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "idx_user_library_albums_last_seen_at",
        table_name="user_library_albums",
        schema="app",
    )
    op.drop_index(
        "idx_user_library_albums_account_available",
        table_name="user_library_albums",
        schema="app",
    )
    op.drop_table("user_library_albums", schema="app")

    op.drop_table("user_provider_sync_state", schema="app")

    op.drop_index(
        "idx_user_provider_accounts_provider_status",
        table_name="user_provider_accounts",
        schema="app",
    )
    op.drop_index(
        "uq_user_provider_accounts_one_active_per_user",
        table_name="user_provider_accounts",
        schema="app",
    )
    op.drop_index(
        "idx_user_provider_accounts_user_active",
        table_name="user_provider_accounts",
        schema="app",
    )
    op.drop_table("user_provider_accounts", schema="app")

    op.execute(
        """
        ALTER TABLE app.idempotency_keys
        RENAME COLUMN job_id TO result_job_id;
        """
    )
