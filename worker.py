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
    claim_runnable_jobs,
    enqueue_job_once,
    get_latest_cycle_number,
    insert_delivery_history,
    list_cycle_album_ids,
    list_active_users_with_settings,
    mark_job_failed,
    mark_job_succeeded,
    requeue_stale_running_jobs,
)
from src.library import get_albums_with_cache
from src.telegram_delivery import send_album_message


JOB_TYPE_DAILY_DELIVER = "daily_deliver"
JOB_TYPE_DELIVER_NOW = "deliver_now"
JOB_TYPE_NEXT_CYCLE_NOW = "next_cycle_now"


@dataclass(frozen=True)
class WorkerConfig:
    bot_token: str
    auth_path: str
    cache_path: str
    library_limit: int
    worker_id: str
    poll_seconds: int
    claim_batch_size: int
    retry_backoff_base_seconds: int
    retry_backoff_max_seconds: int
    due_window_seconds: int
    job_lease_seconds: int


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
        auth_path=_get_env_str("YTM_AUTH_PATH", "secrets/browser.json"),
        cache_path=_get_env_str("ALBUM_CACHE_PATH", _get_env_str("CACHE_PATH", "data/albums_cache.json")),
        library_limit=_get_env_int("LIBRARY_LIMIT", 500),
        worker_id=_get_env_str("WORKER_ID", f"worker-{os.getpid()}"),
        poll_seconds=_get_env_int("WORKER_POLL_SECONDS", 15),
        claim_batch_size=_get_env_int("WORKER_CLAIM_BATCH_SIZE", 10),
        retry_backoff_base_seconds=_get_env_int("WORKER_RETRY_BACKOFF_BASE_SECONDS", 30),
        retry_backoff_max_seconds=_get_env_int("WORKER_RETRY_BACKOFF_MAX_SECONDS", 1800),
        due_window_seconds=_get_env_int("WORKER_DUE_WINDOW_SECONDS", 60),
        job_lease_seconds=_get_env_int("WORKER_JOB_LEASE_SECONDS", 300),
    )


def enqueue_due_jobs(cfg: WorkerConfig) -> int:
    now_utc = datetime.now(timezone.utc)
    users = list_active_users_with_settings()
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
            logging.warning("Skip user_id=%s due to invalid timezone=%s", user_id, timezone_name)
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
            logging.debug("Skip enqueue: idempotency exists key=%s", idem_key)
            continue

        enqueued_count += 1
        logging.info("Enqueued daily_deliver job user_id=%s key=%s", user_id, idem_key)

    return enqueued_count


async def _execute_delivery_job(bot: Bot, cfg: WorkerConfig, job: dict) -> None:
    payload = job.get("payload") or {}
    user_id = int(job["user_id"])
    chat_id = int(payload["telegram_chat_id"])
    job_type = str(job["job_type"])
    force_next_cycle = bool(payload.get("force_next_cycle")) or job_type == JOB_TYPE_NEXT_CYCLE_NOW

    albums = get_albums_with_cache(
        auth_path=cfg.auth_path,
        cache_path=cfg.cache_path,
        refresh=False,
        limit=cfg.library_limit,
    )
    if not albums:
        raise RuntimeError("Library is empty")

    # Cycle semantics:
    # - default: keep current cycle_number until exhausted, then rotate
    # - force_next_cycle: immediately transition to next cycle_number
    latest_cycle_number = get_latest_cycle_number(user_id) or 0
    current_cycle_number = (latest_cycle_number + 1) if force_next_cycle else max(latest_cycle_number, 1)

    eligible = [a for a in albums if a.get("browseId")]
    delivered_ids = set(list_cycle_album_ids(user_id=user_id, cycle_number=current_cycle_number))
    unsent = [a for a in eligible if str(a.get("browseId")) not in delivered_ids]

    if not unsent and not force_next_cycle:
        current_cycle_number += 1
        unsent = eligible

    if not unsent:
        raise RuntimeError("No eligible albums available")

    selected_album = random.choice(unsent)
    selected_album_id = str(selected_album.get("browseId") or "")
    if not selected_album_id:
        raise RuntimeError("Selected album has no browseId")

    # Rare race safety (multiple workers): if the chosen album is inserted by another
    # worker first, retry with remaining unsent candidates.
    reserved = insert_delivery_history(
        user_id=user_id,
        cycle_number=current_cycle_number,
        album_id=selected_album_id,
    )
    if not reserved:
        remaining = [a for a in unsent if str(a.get("browseId")) != selected_album_id]
        random.shuffle(remaining)
        for candidate in remaining:
            candidate_id = str(candidate.get("browseId") or "")
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


async def process_claimed_jobs(bot: Bot, cfg: WorkerConfig) -> int:
    jobs = claim_runnable_jobs(worker_id=cfg.worker_id, batch_size=cfg.claim_batch_size)
    processed = 0

    for job in jobs:
        job_id = UUID(str(job["id"]))
        job_type = str(job["job_type"])
        attempt = int(job.get("attempt") or 0)
        payload = job.get("payload") or {}
        idem_key = payload.get("idempotency_key")

        try:
            if job_type not in {JOB_TYPE_DAILY_DELIVER, JOB_TYPE_DELIVER_NOW, JOB_TYPE_NEXT_CYCLE_NOW}:
                raise RuntimeError(f"Unsupported job_type: {job_type}")

            await _execute_delivery_job(bot, cfg, job)
            mark_job_succeeded(job_id=job_id, idempotency_key=idem_key)
            logging.info("Job succeeded id=%s type=%s", job_id, job_type)
        except Exception as exc:
            backoff_seconds = _compute_backoff_seconds(
                attempt=attempt,
                base=cfg.retry_backoff_base_seconds,
                max_seconds=cfg.retry_backoff_max_seconds,
            )
            next_run_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
            state = mark_job_failed(job_id=job_id, error_text=str(exc), next_run_at=next_run_at)
            logging.exception(
                "Job failed id=%s type=%s next_status=%s next_run_at=%s",
                job_id,
                job_type,
                state.get("status"),
                state.get("run_at"),
            )
        processed += 1

    return processed


async def run_worker() -> None:
    cfg = _load_worker_config()
    bot = Bot(token=cfg.bot_token)
    logging.info(
        "Worker started worker_id=%s poll_seconds=%s claim_batch_size=%s",
        cfg.worker_id,
        cfg.poll_seconds,
        cfg.claim_batch_size,
    )

    while True:
        try:
            enqueued = enqueue_due_jobs(cfg)
            requeued = requeue_stale_running_jobs(cfg.job_lease_seconds)
            processed = await process_claimed_jobs(bot, cfg)
            if requeued:
                logging.warning("Requeued stale running jobs count=%s lease_seconds=%s", requeued, cfg.job_lease_seconds)
            logging.debug("Worker loop done enqueued=%s requeued=%s processed=%s", enqueued, requeued, processed)
        except Exception:
            logging.exception("Worker loop failed")
        await asyncio.sleep(cfg.poll_seconds)


def main() -> None:
    load_dotenv()
    log_level_name = _get_env_str("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, None)
    if not isinstance(log_level, int):
        raise RuntimeError(f"Invalid LOG_LEVEL: {log_level_name}")

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
