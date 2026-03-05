import logging
import os
from datetime import time as dt_time
from typing import Any, Dict, List, Optional

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


def _set_user_access_tx(
    conn: psycopg.Connection,
    telegram_user_id: int,
    allowlisted: bool,
    status: str,
) -> Optional[UserRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE app.users
            SET
                allowlisted = %s,
                status = %s,
                updated_at = NOW()
            WHERE telegram_user_id = %s
            RETURNING id, telegram_user_id, telegram_chat_id, allowlisted, status, updated_at
            """,
            (allowlisted, status, telegram_user_id),
        )
        return cur.fetchone()


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


def approve_user(telegram_user_id: int) -> Optional[UserRow]:
    logger.debug("DB approve_user started telegram_user_id=%s", telegram_user_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                row = _set_user_access_tx(
                    conn=conn,
                    telegram_user_id=telegram_user_id,
                    allowlisted=True,
                    status="active",
                )
        logger.debug("DB approve_user done telegram_user_id=%s found=%s", telegram_user_id, row is not None)
        return row
    except Exception:
        logger.exception("DB approve_user failed telegram_user_id=%s", telegram_user_id)
        raise


def block_user(telegram_user_id: int) -> Optional[UserRow]:
    logger.debug("DB block_user started telegram_user_id=%s", telegram_user_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                row = _set_user_access_tx(
                    conn=conn,
                    telegram_user_id=telegram_user_id,
                    allowlisted=False,
                    status="blocked",
                )
        logger.debug("DB block_user done telegram_user_id=%s found=%s", telegram_user_id, row is not None)
        return row
    except Exception:
        logger.exception("DB block_user failed telegram_user_id=%s", telegram_user_id)
        raise


def list_pending_users(limit: int = 20) -> List[UserRow]:
    logger.debug("DB list_pending_users started limit=%s", limit)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, telegram_user_id, telegram_chat_id, allowlisted, status, created_at
                        FROM app.users
                        WHERE status = 'pending'
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    rows = cur.fetchall()
        logger.debug("DB list_pending_users done count=%s", len(rows))
        return rows
    except Exception:
        logger.exception("DB list_pending_users failed limit=%s", limit)
        raise


def get_user_settings(user_id: int) -> UserRow:
    logger.debug("DB get_user_settings started user_id=%s", user_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT timezone, daily_time_local
                        FROM app.user_settings
                        WHERE user_id = %s
                        """,
                        (user_id,),
                    )
                    row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to load user settings")
        logger.debug("DB get_user_settings done user_id=%s", user_id)
        return row
    except Exception:
        logger.exception("DB get_user_settings failed user_id=%s", user_id)
        raise


def set_user_timezone(user_id: int, timezone: str) -> UserRow:
    logger.debug("DB set_user_timezone started user_id=%s timezone=%s", user_id, timezone)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE app.user_settings
                        SET
                            timezone = %s,
                            updated_at = NOW()
                        WHERE user_id = %s
                        RETURNING timezone, daily_time_local
                        """,
                        (timezone, user_id),
                    )
                    row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to update user timezone")
        logger.debug("DB set_user_timezone done user_id=%s timezone=%s", user_id, row.get("timezone"))
        return row
    except Exception:
        logger.exception("DB set_user_timezone failed user_id=%s timezone=%s", user_id, timezone)
        raise


def set_user_daily_time(user_id: int, daily_time_local: dt_time) -> UserRow:
    logger.debug("DB set_user_daily_time started user_id=%s daily_time_local=%s", user_id, daily_time_local)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE app.user_settings
                        SET
                            daily_time_local = %s,
                            updated_at = NOW()
                        WHERE user_id = %s
                        RETURNING timezone, daily_time_local
                        """,
                        (daily_time_local, user_id),
                    )
                    row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to update user daily time")
        logger.debug(
            "DB set_user_daily_time done user_id=%s daily_time_local=%s",
            user_id,
            row.get("daily_time_local"),
        )
        return row
    except Exception:
        logger.exception(
            "DB set_user_daily_time failed user_id=%s daily_time_local=%s",
            user_id,
            daily_time_local,
        )
        raise
