import asyncio
import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv
from telegram import Bot

from src.db import (
    PROVIDER_ACCOUNT_STATUS_CONNECTED,
    PROVIDER_ACCOUNT_STATUS_NEEDS_REAUTH,
    SYNC_RESULT_AUTH_ERROR,
    SYNC_RESULT_EMPTY_LIBRARY,
    SYNC_RESULT_OK,
    SYNC_RESULT_TRANSIENT_ERROR,
    claim_runnable_jobs,
    enqueue_job_once,
    get_active_user_provider_account,
    get_latest_cycle_number,
    get_user_provider_account_by_id,
    get_user_provider_account_credentials,
    insert_delivery_history,
    list_active_users_with_delivery_context,
    list_available_user_library_albums,
    mark_job_failed,
    mark_job_succeeded,
    mark_user_provider_account_status,
    mark_user_provider_sync_failed,
    mark_user_provider_sync_started,
    mark_user_provider_sync_succeeded,
    requeue_stale_running_jobs,
    list_cycle_album_ids,
    list_provider_accounts_due_for_sync,
    upsert_user_library_albums,
)
from src.errors import is_auth_error
from src.logging_utils import configure_logging, log_event
from src.providers import build_provider_client
from src.telegram_delivery import send_album_message


JOB_TYPE_DAILY_DELIVER = "daily_deliver"
JOB_TYPE_DELIVER_NOW = "deliver_now"
JOB_TYPE_NEXT_CYCLE_NOW = "next_cycle_now"
JOB_TYPE_SYNC_LIBRARY = "sync_library"
JOB_TYPE_REVALIDATE_PROVIDER = "revalidate_provider"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerConfig:
    bot_token: str
    library_limit: int
    worker_id: str
    poll_seconds: int
    claim_batch_size: int
    retry_backoff_base_seconds: int
    retry_backoff_max_seconds: int
    due_window_seconds: int
    job_lease_seconds: int
    provider_sync_interval_seconds: int


def _get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    return int(raw)


def _get_env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _local_date_key(user_id: int, local_date_iso: str) -> str:
    return f"daily:{user_id}:{local_date_iso}"


def _compute_backoff_seconds(attempt: int, base: int, max_seconds: int) -> int:
    # attempt is current attempt value before increment in DB.
    factor = max(attempt, 0)
    return min(base * (2 ** factor), max_seconds)


def _is_due_now(
    timezone_name: str,
    daily_time_local,
    now_utc: datetime,
    window_seconds: int,
) -> tuple[bool, str]:
    tz = ZoneInfo(timezone_name)
    local_now = now_utc.astimezone(tz)
    local_date = local_now.date()
    scheduled = datetime.combine(local_date, daily_time_local, tzinfo=tz)
    window_end = local_now + timedelta(seconds=window_seconds)
    # Due window: only check/enqueue when scheduled time is close.
    return local_now <= scheduled <= window_end, local_date.isoformat()


def _load_worker_config() -> WorkerConfig:
    token = _get_env_str("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    return WorkerConfig(
        bot_token=token,
        library_limit=_get_env_int("LIBRARY_LIMIT", 500),
        worker_id=_get_env_str("WORKER_ID", f"worker-{os.getpid()}"),
        poll_seconds=_get_env_int("WORKER_POLL_SECONDS", 15),
        claim_batch_size=_get_env_int("WORKER_CLAIM_BATCH_SIZE", 10),
        retry_backoff_base_seconds=_get_env_int("WORKER_RETRY_BACKOFF_BASE_SECONDS", 30),
        retry_backoff_max_seconds=_get_env_int("WORKER_RETRY_BACKOFF_MAX_SECONDS", 1800),
        due_window_seconds=_get_env_int("WORKER_DUE_WINDOW_SECONDS", 60),
        job_lease_seconds=_get_env_int("WORKER_JOB_LEASE_SECONDS", 300),
        provider_sync_interval_seconds=_get_env_int("PROVIDER_SYNC_INTERVAL_SECONDS", 21600),
    )


def enqueue_due_jobs(cfg: WorkerConfig) -> int:
    now_utc = datetime.now(timezone.utc)
    users = list_active_users_with_delivery_context()
    enqueued_count = 0

    for user in users:
        user_id = int(user["user_id"])
        timezone_name = str(user["timezone"])
        daily_time_local = user["daily_time_local"]
        chat_id = int(user["telegram_chat_id"])

        try:
            due, local_date_iso = _is_due_now(
                timezone_name,
                daily_time_local,
                now_utc,
                cfg.due_window_seconds,
            )
        except ZoneInfoNotFoundError:
            log_event(
                logger,
                logging.WARNING,
                "invalid_user_timezone",
                user_id=user_id,
                telegram_chat_id=chat_id,
                worker_id=cfg.worker_id,
            )
            continue

        if not due:
            continue

        idem_key = _local_date_key(user_id, local_date_iso)
        row = enqueue_job_once(
            idempotency_key=idem_key,
            idempotency_expires_at=now_utc + timedelta(days=7),
            job_id=uuid4(),
            user_id=user_id,
            job_type=JOB_TYPE_DAILY_DELIVER,
            run_at=now_utc,
            payload={
                "idempotency_key": idem_key,
                "local_date": local_date_iso,
                "timezone": timezone_name,
                "telegram_chat_id": chat_id,
            },
        )
        if row is None:
            log_event(
                logger,
                logging.INFO,
                "daily_enqueue_skipped_idempotency",
                job_type=JOB_TYPE_DAILY_DELIVER,
                user_id=user_id,
                telegram_chat_id=chat_id,
                idempotency_key=idem_key,
                worker_id=cfg.worker_id,
            )
            continue

        enqueued_count += 1
        log_event(
            logger,
            logging.INFO,
            "job_enqueued",
            job_id=row.get("id"),
            job_type=JOB_TYPE_DAILY_DELIVER,
            user_id=user_id,
            telegram_chat_id=chat_id,
            attempt=row.get("attempt"),
            idempotency_key=idem_key,
            worker_id=cfg.worker_id,
        )

    return enqueued_count


def _sync_bucket(now_utc: datetime, interval_seconds: int) -> int:
    return int(now_utc.timestamp()) // max(interval_seconds, 1)


def enqueue_due_sync_jobs(cfg: WorkerConfig) -> int:
    now_utc = datetime.now(timezone.utc)
    sync_before = now_utc - timedelta(seconds=cfg.provider_sync_interval_seconds)
    accounts = list_provider_accounts_due_for_sync(sync_before=sync_before)
    enqueued_count = 0

    for account in accounts:
        account_id = int(account["id"])
        user_id = int(account["user_id"])
        provider = str(account["provider"])
        idem_key = f"sync:{account_id}:{_sync_bucket(now_utc, cfg.provider_sync_interval_seconds)}"
        row = enqueue_job_once(
            idempotency_key=idem_key,
            idempotency_expires_at=now_utc + timedelta(seconds=cfg.provider_sync_interval_seconds * 2),
            job_id=uuid4(),
            user_id=user_id,
            job_type=JOB_TYPE_SYNC_LIBRARY,
            run_at=now_utc,
            payload={
                "idempotency_key": idem_key,
                "user_provider_account_id": account_id,
                "provider": provider,
            },
        )
        if row is not None:
            enqueued_count += 1

    return enqueued_count


def _build_provider_client_for_account(account: dict, credentials: dict):
    return build_provider_client(str(account["provider"]), credentials=credentials)


def _sync_provider_account(cfg: WorkerConfig, account: dict) -> list[dict]:
    account_id = int(account["id"])
    credentials = get_user_provider_account_credentials(account_id)
    if not credentials:
        raise RuntimeError("Provider credentials are not configured")

    mark_user_provider_sync_started(account_id)
    provider_client = _build_provider_client_for_account(account, credentials)
    try:
        albums = provider_client.list_saved_albums(limit=cfg.library_limit)
        synced_count = upsert_user_library_albums(account_id, albums)
        result_status = SYNC_RESULT_EMPTY_LIBRARY if synced_count == 0 else SYNC_RESULT_OK
        mark_user_provider_sync_succeeded(account_id, library_item_count=synced_count, result_status=result_status)
        if str(account.get("status") or "") != PROVIDER_ACCOUNT_STATUS_CONNECTED:
            mark_user_provider_account_status(account_id, PROVIDER_ACCOUNT_STATUS_CONNECTED)
        return albums
    except Exception as exc:
        if is_auth_error(exc):
            mark_user_provider_sync_failed(account_id, str(exc), result_status=SYNC_RESULT_AUTH_ERROR)
            mark_user_provider_account_status(account_id, PROVIDER_ACCOUNT_STATUS_NEEDS_REAUTH)
        else:
            mark_user_provider_sync_failed(account_id, str(exc), result_status=SYNC_RESULT_TRANSIENT_ERROR)
        raise


def _get_delivery_albums(cfg: WorkerConfig, user_id: int) -> list[dict]:
    account = get_active_user_provider_account(user_id)
    if account is None:
        raise RuntimeError("No active provider account is configured")
    account_id = int(account["id"])
    cached_albums = list_available_user_library_albums(account_id)
    if cached_albums:
        return cached_albums
    return _sync_provider_account(cfg, account)


async def _execute_delivery_job(bot: Bot, cfg: WorkerConfig, job: dict) -> None:
    payload = job.get("payload") or {}
    user_id = int(job["user_id"])
    chat_id = int(payload["telegram_chat_id"])
    job_type = str(job["job_type"])
    force_next_cycle = bool(payload.get("force_next_cycle")) or job_type == JOB_TYPE_NEXT_CYCLE_NOW

    albums = _get_delivery_albums(cfg, user_id)
    if not albums:
        raise RuntimeError("Library is empty")

    # Cycle semantics:
    # - default: keep current cycle_number until exhausted, then rotate
    # - force_next_cycle: immediately transition to next cycle_number
    latest_cycle_number = get_latest_cycle_number(user_id) or 0
    current_cycle_number = (latest_cycle_number + 1) if force_next_cycle else max(latest_cycle_number, 1)

    eligible = [a for a in albums if a.get("provider_album_id")]
    delivered_ids = set(list_cycle_album_ids(user_id=user_id, cycle_number=current_cycle_number))
    unsent = [a for a in eligible if str(a.get("provider_album_id")) not in delivered_ids]

    if not unsent and not force_next_cycle:
        current_cycle_number += 1
        unsent = eligible

    if not unsent:
        raise RuntimeError("No eligible albums available")

    selected_album = random.choice(unsent)
    selected_album_id = str(selected_album.get("provider_album_id") or "")
    if not selected_album_id:
        raise RuntimeError("Selected album has no provider_album_id")

    # Rare race safety (multiple workers): if the chosen album is inserted by another
    # worker first, retry with remaining unsent candidates.
    reserved = insert_delivery_history(
        user_id=user_id,
        cycle_number=current_cycle_number,
        album_id=selected_album_id,
    )
    if not reserved:
        remaining = [a for a in unsent if str(a.get("provider_album_id")) != selected_album_id]
        random.shuffle(remaining)
        for candidate in remaining:
            candidate_id = str(candidate.get("provider_album_id") or "")
            if not candidate_id:
                continue
            if insert_delivery_history(
                user_id=user_id,
                cycle_number=current_cycle_number,
                album_id=candidate_id,
            ):
                selected_album = candidate
                reserved = True
                break

    if not reserved:
        raise RuntimeError("Could not reserve a unique album in current cycle")

    if job_type == JOB_TYPE_DAILY_DELIVER:
        prefix = "📅 Daily album"
    elif force_next_cycle:
        prefix = "⏭️ New cycle album"
    else:
        prefix = "🎲 Album now"
    await send_album_message(bot, chat_id=chat_id, album=selected_album, prefix=prefix)


def _execute_sync_job(cfg: WorkerConfig, job: dict) -> None:
    payload = job.get("payload") or {}
    account_id = int(payload["user_provider_account_id"])
    account = get_user_provider_account_by_id(account_id)
    if account is None:
        raise RuntimeError("Provider account not found")
    _sync_provider_account(cfg, account)


def _execute_revalidate_provider_job(cfg: WorkerConfig, job: dict) -> None:
    payload = job.get("payload") or {}
    account_id = int(payload["user_provider_account_id"])
    account = get_user_provider_account_by_id(account_id)
    if account is None:
        raise RuntimeError("Provider account not found")

    credentials = get_user_provider_account_credentials(account_id)
    if not credentials:
        raise RuntimeError("Provider credentials are not configured")

    provider_client = _build_provider_client_for_account(account, credentials)
    try:
        provider_client.validate_credentials()
        mark_user_provider_account_status(account_id, PROVIDER_ACCOUNT_STATUS_CONNECTED)
    except Exception as exc:
        if is_auth_error(exc):
            mark_user_provider_account_status(account_id, PROVIDER_ACCOUNT_STATUS_NEEDS_REAUTH)
        raise


async def process_claimed_jobs(bot: Bot, cfg: WorkerConfig) -> int:
    jobs = claim_runnable_jobs(worker_id=cfg.worker_id, batch_size=cfg.claim_batch_size)
    processed = 0

    for job in jobs:
        job_id = UUID(str(job["id"]))
        job_type = str(job["job_type"])
        attempt = int(job.get("attempt") or 0)
        payload = job.get("payload") or {}
        idem_key = payload.get("idempotency_key")
        chat_id = payload.get("telegram_chat_id")

        log_event(
            logger,
            logging.INFO,
            "job_claimed",
            job_id=job_id,
            job_type=job_type,
            user_id=job.get("user_id"),
            telegram_chat_id=chat_id,
            attempt=attempt,
            idempotency_key=idem_key,
            worker_id=cfg.worker_id,
        )

        try:
            if job_type == JOB_TYPE_SYNC_LIBRARY:
                _execute_sync_job(cfg, job)
            elif job_type == JOB_TYPE_REVALIDATE_PROVIDER:
                _execute_revalidate_provider_job(cfg, job)
            elif job_type in {JOB_TYPE_DAILY_DELIVER, JOB_TYPE_DELIVER_NOW, JOB_TYPE_NEXT_CYCLE_NOW}:
                await _execute_delivery_job(bot, cfg, job)
            else:
                raise RuntimeError(f"Unsupported job_type: {job_type}")
            mark_job_succeeded(job_id=job_id, idempotency_key=idem_key)
            log_event(
                logger,
                logging.INFO,
                "job_succeeded",
                job_id=job_id,
                job_type=job_type,
                user_id=job.get("user_id"),
                telegram_chat_id=chat_id,
                attempt=attempt,
                idempotency_key=idem_key,
                worker_id=cfg.worker_id,
            )
        except Exception as exc:
            backoff_seconds = _compute_backoff_seconds(
                attempt=attempt,
                base=cfg.retry_backoff_base_seconds,
                max_seconds=cfg.retry_backoff_max_seconds,
            )
            next_run_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
            state = mark_job_failed(job_id=job_id, error_text=str(exc), next_run_at=next_run_at)
            log_event(
                logger,
                logging.ERROR,
                "job_failed",
                message=f"job_failed next_status={state.get('status')} next_run_at={state.get('run_at')}",
                exc_info=True,
                job_id=job_id,
                job_type=job_type,
                user_id=job.get("user_id"),
                telegram_chat_id=chat_id,
                attempt=state.get("attempt"),
                idempotency_key=idem_key,
                worker_id=cfg.worker_id,
            )
        processed += 1

    return processed


async def run_worker() -> None:
    cfg = _load_worker_config()
    bot = Bot(token=cfg.bot_token)
    log_event(
        logger,
        logging.INFO,
        "worker_started",
        message=f"worker_started poll_seconds={cfg.poll_seconds} claim_batch_size={cfg.claim_batch_size}",
        worker_id=cfg.worker_id,
    )

    while True:
        try:
            enqueued = enqueue_due_jobs(cfg)
            sync_enqueued = enqueue_due_sync_jobs(cfg)
            requeued = requeue_stale_running_jobs(cfg.job_lease_seconds)
            processed = await process_claimed_jobs(bot, cfg)
            if requeued:
                log_event(
                    logger,
                    logging.WARNING,
                    "stale_jobs_requeued",
                    message=f"stale_jobs_requeued count={requeued} lease_seconds={cfg.job_lease_seconds}",
                    worker_id=cfg.worker_id,
                )
            log_event(
                logger,
                logging.DEBUG,
                "worker_loop_completed",
                message=(
                    f"worker_loop_completed enqueued={enqueued} "
                    f"sync_enqueued={sync_enqueued} requeued={requeued} processed={processed}"
                ),
                worker_id=cfg.worker_id,
            )
        except Exception:
            log_event(
                logger,
                logging.ERROR,
                "worker_loop_failed",
                exc_info=True,
                worker_id=cfg.worker_id,
            )
        await asyncio.sleep(cfg.poll_seconds)


def main() -> None:
    load_dotenv()
    log_level_name = _get_env_str("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, None)
    if not isinstance(log_level, int):
        raise RuntimeError(f"Invalid LOG_LEVEL: {log_level_name}")

    configure_logging(log_level)
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
