"""baseline schema

Revision ID: 9109d701540d
Revises: 
Create Date: 2026-03-06 14:35:32.160345

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9109d701540d'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # UUID helper for gen_random_uuid()
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    op.execute("CREATE SCHEMA IF NOT EXISTS app;")

    op.execute("""
    CREATE TABLE IF NOT EXISTS app.users (
      id                BIGSERIAL PRIMARY KEY,
      telegram_user_id  BIGINT NOT NULL UNIQUE,
      telegram_chat_id  BIGINT NOT NULL,
      status            TEXT NOT NULL DEFAULT 'pending',
      allowlisted       BOOLEAN NOT NULL DEFAULT FALSE,
      created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)

    op.execute("""
    CREATE TABLE IF NOT EXISTS app.user_settings (
      user_id           BIGINT PRIMARY KEY REFERENCES app.users(id) ON DELETE CASCADE,
      timezone          TEXT NOT NULL DEFAULT 'UTC',
      daily_time_local  TIME NOT NULL DEFAULT '09:00',
      created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)

    op.execute("""
    CREATE TABLE IF NOT EXISTS app.jobs (
      id              UUID PRIMARY KEY,
      user_id         BIGINT NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      job_type        TEXT NOT NULL,
      run_at          TIMESTAMPTZ NOT NULL,
      status          TEXT NOT NULL DEFAULT 'queued',
      attempt         INTEGER NOT NULL DEFAULT 0,
      max_attempts    INTEGER NOT NULL DEFAULT 10,
      locked_by       TEXT,
      locked_at       TIMESTAMPTZ,
      payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
      last_error      TEXT,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)

    op.execute("CREATE INDEX IF NOT EXISTS idx_jobs_runnable ON app.jobs (status, run_at);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_jobs_user ON app.jobs (user_id, status);")

    op.execute("""
    CREATE TABLE IF NOT EXISTS app.idempotency_keys (
      key           TEXT PRIMARY KEY,
      user_id       BIGINT NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      job_type      TEXT NOT NULL,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      expires_at    TIMESTAMPTZ,
      result_job_id UUID
    );
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_idem_user ON app.idempotency_keys (user_id);")

    op.execute("""
    CREATE TABLE IF NOT EXISTS app.delivery_history (
      id           BIGSERIAL PRIMARY KEY,
      user_id      BIGINT NOT NULL REFERENCES app.users(id) ON DELETE CASCADE,
      cycle_id     UUID NOT NULL,
      album_id     TEXT NOT NULL,
      delivered_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_cycle
    ON app.delivery_history (user_id, cycle_id, album_id);
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop in reverse dependency order
    op.execute("DROP SCHEMA IF EXISTS app CASCADE;")
