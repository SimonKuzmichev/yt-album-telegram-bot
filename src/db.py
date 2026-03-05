import logging
import os
from typing import Any, Dict

import psycopg
from psycopg.rows import dict_row


# NOTE: Schema/migration DDL is intentionally not managed here; Alembic owns it.
UserRow = Dict[str, Any]
logger = logging.getLogger(__name__)


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return database_url


def open_db_connection() -> psycopg.Connection:
    return psycopg.connect(get_database_url(), row_factory=dict_row)


def _upsert_user_tx(conn: psycopg.Connection, telegram_user_id: int, telegram_chat_id: int) -> UserRow:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO app.users (
                telegram_user_id,
                telegram_chat_id
            )
            VALUES (%s, %s)
            ON CONFLICT (telegram_user_id)
            DO UPDATE SET
                telegram_chat_id = EXCLUDED.telegram_chat_id,
                updated_at = NOW()
            RETURNING id, allowlisted, status
            """,
            (telegram_user_id, telegram_chat_id),
        )
        #TODO: check the return later
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to upsert user")
        return row


def upsert_user(telegram_user_id: int, telegram_chat_id: int) -> UserRow:
    logger.debug(
        "DB upsert_user started telegram_user_id=%s telegram_chat_id=%s",
        telegram_user_id,
        telegram_chat_id,
    )
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                row = _upsert_user_tx(
                    conn=conn,
                    telegram_user_id=telegram_user_id,
                    telegram_chat_id=telegram_chat_id,
                )
        logger.debug(
            "DB upsert_user done user_id=%s allowlisted=%s status=%s",
            row.get("id"),
            row.get("allowlisted"),
            row.get("status"),
        )
        return row
    except Exception:
        logger.exception(
            "DB upsert_user failed telegram_user_id=%s telegram_chat_id=%s",
            telegram_user_id,
            telegram_chat_id,
        )
        raise


def ensure_user_settings(user_id: int) -> None:
    logger.debug("DB ensure_user_settings started user_id=%s", user_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO app.user_settings (user_id)
                        VALUES (%s)
                        ON CONFLICT (user_id) DO NOTHING
                        """,
                        (user_id,),
                    )
        logger.debug("DB ensure_user_settings done user_id=%s", user_id)
    except Exception:
        logger.exception("DB ensure_user_settings failed user_id=%s", user_id)
        raise
