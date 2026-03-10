import logging
import os
from datetime import datetime, time as dt_time
from uuid import UUID
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


# NOTE: Schema/migration DDL is intentionally not managed here; Alembic owns it.
UserRow = Dict[str, Any]
JobRow = Dict[str, Any]
logger = logging.getLogger(__name__)


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return database_url


def open_db_connection() -> psycopg.Connection:
    return psycopg.connect(get_database_url(), row_factory=dict_row)


def _upsert_user_tx(
    conn: psycopg.Connection,
    telegram_user_id: int,
    telegram_chat_id: int,
    username: Optional[str],
) -> UserRow:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO app.users (
                telegram_user_id,
                telegram_chat_id,
                username
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (telegram_user_id)
            DO UPDATE SET
                telegram_chat_id = EXCLUDED.telegram_chat_id,
                username = EXCLUDED.username,
                updated_at = NOW()
            RETURNING id, allowlisted, status
            """,
            (telegram_user_id, telegram_chat_id, username),
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


def upsert_user(telegram_user_id: int, telegram_chat_id: int, username: Optional[str] = None) -> UserRow:
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
                    username=username,
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
                        SELECT id, telegram_user_id, telegram_chat_id, allowlisted, status, created_at, username
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


def get_user_timezone_by_chat_id(telegram_chat_id: int) -> Optional[str]:
    logger.debug("DB get_user_timezone_by_chat_id started telegram_chat_id=%s", telegram_chat_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT s.timezone
                        FROM app.users AS u
                        JOIN app.user_settings AS s ON s.user_id = u.id
                        WHERE u.telegram_chat_id = %s
                        ORDER BY u.id DESC
                        LIMIT 1
                        """,
                        (telegram_chat_id,),
                    )
                    row = cur.fetchone()
        timezone_name = str(row["timezone"]) if row and row.get("timezone") else None
        logger.debug(
            "DB get_user_timezone_by_chat_id done telegram_chat_id=%s timezone=%s",
            telegram_chat_id,
            timezone_name,
        )
        return timezone_name
    except Exception:
        logger.exception("DB get_user_timezone_by_chat_id failed telegram_chat_id=%s", telegram_chat_id)
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


def list_active_users_with_settings() -> List[UserRow]:
    logger.debug("DB list_active_users_with_settings started")
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            u.id AS user_id,
                            u.telegram_chat_id,
                            u.allowlisted,
                            u.status,
                            s.timezone,
                            s.daily_time_local
                        FROM app.users AS u
                        JOIN app.user_settings AS s ON s.user_id = u.id
                        WHERE u.allowlisted = TRUE
                          AND u.status = 'active'
                        ORDER BY u.id ASC
                        """
                    )
                    rows = cur.fetchall()
        logger.debug("DB list_active_users_with_settings done count=%s", len(rows))
        return rows
    except Exception:
        logger.exception("DB list_active_users_with_settings failed")
        raise


def try_insert_idempotency_key(
    key: str,
    user_id: int,
    job_type: str,
    expires_at: Optional[datetime] = None,
) -> bool:
    logger.debug("DB try_insert_idempotency_key started key=%s user_id=%s job_type=%s", key, user_id, job_type)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO app.idempotency_keys (key, user_id, job_type, expires_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (key) DO NOTHING
                        RETURNING key
                        """,
                        (key, user_id, job_type, expires_at),
                    )
                    row = cur.fetchone()
        created = row is not None
        logger.debug("DB try_insert_idempotency_key done key=%s created=%s", key, created)
        return created
    except Exception:
        logger.exception("DB try_insert_idempotency_key failed key=%s user_id=%s", key, user_id)
        raise


def enqueue_job_once(
    *,
    idempotency_key: str,
    idempotency_expires_at: Optional[datetime],
    job_id: UUID,
    user_id: int,
    job_type: str,
    run_at: datetime,
    payload: Optional[Dict[str, Any]] = None,
) -> Optional[JobRow]:
    logger.debug(
        "DB enqueue_job_once started key=%s job_id=%s user_id=%s job_type=%s run_at=%s",
        idempotency_key,
        job_id,
        user_id,
        job_type,
        run_at,
    )
    payload = payload or {}
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO app.idempotency_keys (key, user_id, job_type, expires_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (key) DO NOTHING
                        RETURNING key
                        """,
                        (idempotency_key, user_id, job_type, idempotency_expires_at),
                    )
                    idem_row = cur.fetchone()
                    if idem_row is None:
                        logger.debug(
                            "DB enqueue_job_once skipped duplicate key=%s user_id=%s job_type=%s",
                            idempotency_key,
                            user_id,
                            job_type,
                        )
                        return None

                    cur.execute(
                        """
                        INSERT INTO app.jobs (
                            id, user_id, job_type, run_at, status, payload
                        )
                        VALUES (%s, %s, %s, %s, 'queued', %s)
                        RETURNING *
                        """,
                        (job_id, user_id, job_type, run_at, Json(payload)),
                    )
                    row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to enqueue job")
        logger.debug("DB enqueue_job_once done key=%s job_id=%s", idempotency_key, job_id)
        return row
    except Exception:
        logger.exception(
            "DB enqueue_job_once failed key=%s job_id=%s user_id=%s",
            idempotency_key,
            job_id,
            user_id,
        )
        raise


def enqueue_job(
    job_id: UUID,
    user_id: int,
    job_type: str,
    run_at: datetime,
    payload: Optional[Dict[str, Any]] = None,
) -> JobRow:
    logger.debug("DB enqueue_job started job_id=%s user_id=%s job_type=%s run_at=%s", job_id, user_id, job_type, run_at)
    payload = payload or {}
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO app.jobs (
                            id, user_id, job_type, run_at, status, payload
                        )
                        VALUES (%s, %s, %s, %s, 'queued', %s)
                        RETURNING *
                        """,
                        (job_id, user_id, job_type, run_at, Json(payload)),
                    )
                    row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to enqueue job")
        logger.debug("DB enqueue_job done job_id=%s", job_id)
        return row
    except Exception:
        logger.exception("DB enqueue_job failed job_id=%s user_id=%s", job_id, user_id)
        raise


def claim_runnable_jobs(worker_id: str, batch_size: int = 10) -> List[JobRow]:
    logger.debug("DB claim_runnable_jobs started worker_id=%s batch_size=%s", worker_id, batch_size)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH picked AS (
                            SELECT id
                            FROM app.jobs
                            WHERE status = 'queued'
                              AND run_at <= NOW()
                            ORDER BY run_at ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT %s
                        )
                        UPDATE app.jobs AS j
                        SET
                            status = 'running',
                            locked_by = %s,
                            locked_at = NOW(),
                            updated_at = NOW()
                        FROM picked
                        WHERE j.id = picked.id
                        RETURNING j.*
                        """,
                        (batch_size, worker_id),
                    )
                    rows = cur.fetchall()
        logger.debug("DB claim_runnable_jobs done worker_id=%s claimed=%s", worker_id, len(rows))
        return rows
    except Exception:
        logger.exception("DB claim_runnable_jobs failed worker_id=%s", worker_id)
        raise


def requeue_stale_running_jobs(lease_seconds: int) -> int:
    logger.debug("DB requeue_stale_running_jobs started lease_seconds=%s", lease_seconds)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE app.jobs
                        SET
                            status = 'queued',
                            locked_by = NULL,
                            locked_at = NULL,
                            updated_at = NOW()
                        WHERE status = 'running'
                          AND locked_at IS NOT NULL
                          AND locked_at < NOW() - (%s * INTERVAL '1 second')
                        RETURNING id
                        """,
                        (lease_seconds,),
                    )
                    rows = cur.fetchall()
        count = len(rows)
        logger.debug("DB requeue_stale_running_jobs done requeued=%s", count)
        return count
    except Exception:
        logger.exception("DB requeue_stale_running_jobs failed lease_seconds=%s", lease_seconds)
        raise


def mark_job_succeeded(job_id: UUID, idempotency_key: Optional[str] = None) -> None:
    logger.debug("DB mark_job_succeeded started job_id=%s", job_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE app.jobs
                        SET
                            status = 'succeeded',
                            updated_at = NOW(),
                            locked_by = NULL,
                            locked_at = NULL
                        WHERE id = %s
                        """,
                        (job_id,),
                    )
                    if idempotency_key:
                        cur.execute(
                            """
                            UPDATE app.idempotency_keys
                            SET job_id = %s
                            WHERE key = %s
                            """,
                            (job_id, idempotency_key),
                        )
        logger.debug("DB mark_job_succeeded done job_id=%s", job_id)
    except Exception:
        logger.exception("DB mark_job_succeeded failed job_id=%s", job_id)
        raise


def mark_job_failed(job_id: UUID, error_text: str, next_run_at: datetime) -> JobRow:
    logger.debug("DB mark_job_failed started job_id=%s", job_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE app.jobs
                        SET
                            attempt = attempt + 1,
                            status = CASE
                                WHEN (attempt + 1) >= max_attempts THEN 'dead'
                                ELSE 'queued'
                            END,
                            run_at = CASE
                                WHEN (attempt + 1) >= max_attempts THEN run_at
                                ELSE %s
                            END,
                            last_error = %s,
                            updated_at = NOW(),
                            locked_by = NULL,
                            locked_at = NULL
                        WHERE id = %s
                        RETURNING id, status, attempt, max_attempts, run_at
                        """,
                        (next_run_at, error_text[:1000], job_id),
                    )
                    row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to update failed job state")
        logger.debug(
            "DB mark_job_failed done job_id=%s status=%s attempt=%s/%s",
            job_id,
            row.get("status"),
            row.get("attempt"),
            row.get("max_attempts"),
        )
        return row
    except Exception:
        logger.exception("DB mark_job_failed failed job_id=%s", job_id)
        raise


def insert_delivery_history(user_id: int, cycle_number: int, album_id: str) -> bool:
    logger.debug(
        "DB insert_delivery_history started user_id=%s cycle_number=%s album_id=%s",
        user_id,
        cycle_number,
        album_id,
    )
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO app.delivery_history (user_id, cycle_number, album_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """,
                        (user_id, cycle_number, album_id),
                    )
                    row = cur.fetchone()
        inserted = row is not None
        logger.debug(
            "DB insert_delivery_history done user_id=%s cycle_number=%s inserted=%s",
            user_id,
            cycle_number,
            inserted,
        )
        return inserted
    except Exception:
        logger.exception(
            "DB insert_delivery_history failed user_id=%s cycle_number=%s",
            user_id,
            cycle_number,
        )
        raise


def get_latest_cycle_number(user_id: int) -> Optional[int]:
    logger.debug("DB get_latest_cycle_number started user_id=%s", user_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT cycle_number
                        FROM app.delivery_history
                        WHERE user_id = %s
                        ORDER BY cycle_number DESC, delivered_at DESC, id DESC
                        LIMIT 1
                        """,
                        (user_id,),
                    )
                    row = cur.fetchone()
        cycle_number = int(row["cycle_number"]) if row else None
        logger.debug(
            "DB get_latest_cycle_number done user_id=%s cycle_number=%s",
            user_id,
            cycle_number,
        )
        return cycle_number
    except Exception:
        logger.exception("DB get_latest_cycle_number failed user_id=%s", user_id)
        raise


def list_cycle_album_ids(user_id: int, cycle_number: int) -> List[str]:
    logger.debug(
        "DB list_cycle_album_ids started user_id=%s cycle_number=%s",
        user_id,
        cycle_number,
    )
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT album_id
                        FROM app.delivery_history
                        WHERE user_id = %s
                          AND cycle_number = %s
                        """,
                        (user_id, cycle_number),
                    )
                    rows = cur.fetchall()
        album_ids = [str(r["album_id"]) for r in rows if r.get("album_id")]
        logger.debug(
            "DB list_cycle_album_ids done user_id=%s cycle_number=%s count=%s",
            user_id,
            cycle_number,
            len(album_ids),
        )
        return album_ids
    except Exception:
        logger.exception(
            "DB list_cycle_album_ids failed user_id=%s cycle_number=%s",
            user_id,
            cycle_number,
        )
        raise


def get_user_delivery_stats(user_id: int) -> UserRow:
    logger.debug("DB get_user_delivery_stats started user_id=%s", user_id)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            COUNT(*)::BIGINT AS total_deliveries,
                            MAX(delivered_at) AS last_delivered_at
                        FROM app.delivery_history
                        WHERE user_id = %s
                        """,
                        (user_id,),
                    )
                    summary = cur.fetchone()

                    cur.execute(
                        """
                        SELECT cycle_number
                        FROM app.delivery_history
                        WHERE user_id = %s
                        ORDER BY cycle_number DESC, delivered_at DESC, id DESC
                        LIMIT 1
                        """,
                        (user_id,),
                    )
                    latest = cur.fetchone()
                    latest_cycle_number = int(latest["cycle_number"]) if latest else None

                    latest_cycle_count = 0
                    if latest_cycle_number is not None:
                        cur.execute(
                            """
                            SELECT COUNT(*)::BIGINT AS cnt
                            FROM app.delivery_history
                            WHERE user_id = %s
                              AND cycle_number = %s
                            """,
                            (user_id, latest_cycle_number),
                        )
                        count_row = cur.fetchone() or {}
                        latest_cycle_count = int(count_row.get("cnt") or 0)

        result = {
            "total_deliveries": int((summary or {}).get("total_deliveries") or 0),
            "last_delivered_at": (summary or {}).get("last_delivered_at"),
            "latest_cycle_number": latest_cycle_number,
            "latest_cycle_count": latest_cycle_count,
        }
        logger.debug("DB get_user_delivery_stats done user_id=%s total=%s", user_id, result["total_deliveries"])
        return result
    except Exception:
        logger.exception("DB get_user_delivery_stats failed user_id=%s", user_id)
        raise


def list_recent_deliveries(user_id: int, limit: int = 5) -> List[UserRow]:
    logger.debug("DB list_recent_deliveries started user_id=%s limit=%s", user_id, limit)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT album_id, cycle_number, delivered_at
                        FROM app.delivery_history
                        WHERE user_id = %s
                        ORDER BY delivered_at DESC, id DESC
                        LIMIT %s
                        """,
                        (user_id, limit),
                    )
                    rows = cur.fetchall()
        logger.debug("DB list_recent_deliveries done user_id=%s count=%s", user_id, len(rows))
        return rows
    except Exception:
        logger.exception("DB list_recent_deliveries failed user_id=%s", user_id)
        raise


def get_admin_status_snapshot(pending_limit: int = 20) -> Dict[str, Any]:
    logger.debug("DB get_admin_status_snapshot started pending_limit=%s", pending_limit)
    try:
        with open_db_connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, telegram_user_id, telegram_chat_id, username, created_at
                        FROM app.users
                        WHERE status = 'pending'
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (pending_limit,),
                    )
                    pending_users = cur.fetchall()

                    cur.execute(
                        """
                        SELECT
                            COUNT(*) FILTER (WHERE status = 'queued')::BIGINT AS queued_jobs_count,
                            COUNT(*) FILTER (WHERE status = 'running')::BIGINT AS running_jobs_count,
                            COUNT(*) FILTER (WHERE status IN ('failed', 'dead'))::BIGINT AS failed_dead_jobs_count
                        FROM app.jobs
                        """
                    )
                    counts = cur.fetchone() or {}

                    cur.execute(
                        """
                        SELECT
                            u.id AS user_id,
                            u.telegram_user_id,
                            u.telegram_chat_id,
                            u.username,
                            latest.album_id,
                            latest.cycle_number,
                            latest.delivered_at
                        FROM app.users AS u
                        LEFT JOIN LATERAL (
                            SELECT album_id, cycle_number, delivered_at
                            FROM app.delivery_history AS dh
                            WHERE dh.user_id = u.id
                            ORDER BY delivered_at DESC, id DESC
                            LIMIT 1
                        ) AS latest ON TRUE
                        ORDER BY u.id ASC
                        """
                    )
                    last_delivery_per_user = cur.fetchall()

        result = {
            "pending_users": pending_users,
            "queued_jobs_count": int(counts.get("queued_jobs_count") or 0),
            "running_jobs_count": int(counts.get("running_jobs_count") or 0),
            "failed_dead_jobs_count": int(counts.get("failed_dead_jobs_count") or 0),
            "last_delivery_per_user": last_delivery_per_user,
        }
        logger.debug(
            "DB get_admin_status_snapshot done pending=%s queued=%s running=%s failed_dead=%s users=%s",
            len(pending_users),
            result["queued_jobs_count"],
            result["running_jobs_count"],
            result["failed_dead_jobs_count"],
            len(last_delivery_per_user),
        )
        return result
    except Exception:
        logger.exception("DB get_admin_status_snapshot failed pending_limit=%s", pending_limit)
        raise
