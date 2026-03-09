"""replace cycle_id with cycle_number and add username

Revision ID: fcd3705b4835
Revises: 9109d701540d
Create Date: 2026-03-06 15:28:57.050168

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fcd3705b4835'
down_revision: Union[str, Sequence[str], None] = '9109d701540d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1) Add username to users
    # Keep it nullable at first to avoid breaking existing rows.
    op.execute("""
    ALTER TABLE app.users
    ADD COLUMN IF NOT EXISTS username TEXT;
    """)

    # 2) Add cycle_number to delivery_history
    op.execute("""
    ALTER TABLE app.delivery_history
    ADD COLUMN IF NOT EXISTS cycle_number BIGINT;
    """)

    # 3) Backfill cycle_number
    # Mapping rule: per user, assign 1..N to distinct cycle_id values, ordered by first delivery time.
    # This preserves "cycles are grouped", but the actual numbering is derived, not original.
    op.execute("""
    WITH cycles AS (
      SELECT
        user_id,
        cycle_id,
        MIN(delivered_at) AS first_delivered_at
      FROM app.delivery_history
      GROUP BY user_id, cycle_id
    ),
    numbered AS (
      SELECT
        user_id,
        cycle_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY first_delivered_at, cycle_id) AS cycle_number
      FROM cycles
    )
    UPDATE app.delivery_history dh
    SET cycle_number = n.cycle_number
    FROM numbered n
    WHERE dh.user_id = n.user_id
      AND dh.cycle_id = n.cycle_id
      AND dh.cycle_number IS NULL;
    """)

    # If the table was empty, cycle_number might still be NULL; default to 1 for future inserts
    op.execute("""
    ALTER TABLE app.delivery_history
    ALTER COLUMN cycle_number SET DEFAULT 1;
    """)

    # Make it NOT NULL once backfilled (safe even if table empty)
    op.execute("""
    ALTER TABLE app.delivery_history
    ALTER COLUMN cycle_number SET NOT NULL;
    """)

    # 4) Replace the uniqueness constraint/index
    # Drop old unique index on (user_id, cycle_id, album_id) if it exists
    op.execute("""
    DROP INDEX IF EXISTS app.uq_delivery_cycle;
    """)

    # Create new unique index on (user_id, cycle_number, album_id)
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_cycle_number
    ON app.delivery_history (user_id, cycle_number, album_id);
    """)

    # 5) Drop cycle_id column
    op.execute("""
    ALTER TABLE app.delivery_history
    DROP COLUMN IF EXISTS cycle_id;
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Recreate cycle_id (UUID) column
    op.execute("""
    ALTER TABLE app.delivery_history
    ADD COLUMN IF NOT EXISTS cycle_id UUID;
    """)

    # Backfill cycle_id with a deterministic UUID per (user_id, cycle_number)
    # Requires pgcrypto for gen_random_uuid(); we will generate fresh UUIDs (can't restore originals).
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    op.execute("""
    WITH distinct_cycles AS (
      SELECT DISTINCT user_id, cycle_number
      FROM app.delivery_history
    ),
    mapped AS (
      SELECT user_id, cycle_number, gen_random_uuid() AS cycle_id
      FROM distinct_cycles
    )
    UPDATE app.delivery_history dh
    SET cycle_id = m.cycle_id
    FROM mapped m
    WHERE dh.user_id = m.user_id
      AND dh.cycle_number = m.cycle_number
      AND dh.cycle_id IS NULL;
    """)

    op.execute("""
    ALTER TABLE app.delivery_history
    ALTER COLUMN cycle_id SET NOT NULL;
    """)

    # Swap indexes back
    op.execute("DROP INDEX IF EXISTS app.uq_delivery_cycle_number;")
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_cycle
    ON app.delivery_history (user_id, cycle_id, album_id);
    """)

    # Drop cycle_number
    op.execute("""
    ALTER TABLE app.delivery_history
    DROP COLUMN IF EXISTS cycle_number;
    """)

    # Drop username index and column
    op.execute("DROP INDEX IF EXISTS app.uq_users_username;")
    op.execute("""
    ALTER TABLE app.users
    DROP COLUMN IF EXISTS username;
    """)
