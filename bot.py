import asyncio
import logging
import os
from datetime import datetime, time as dt_time, timedelta, timezone
from time import perf_counter
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
)

from src.library import get_albums_with_cache, load_cache_payload
from src.errors import is_auth_error, format_auth_help
from src.logging_utils import configure_logging, log_event
from src.db import (
    approve_user,
    block_user,
    enqueue_job_once,
    ensure_user_settings,
    get_admin_status_snapshot,
    get_user_timezone_by_chat_id,
    get_user_delivery_stats,
    get_user_settings,
    list_recent_deliveries,
    set_user_daily_time,
    set_user_timezone,
    upsert_user,
)
from src.telegram_delivery import (
    CB_NEXT,
    CB_REFRESH,
    CB_STATUS,
    build_keyboard,
)


Album = Dict[str, Any]
JOB_TYPE_DELIVER_NOW = "deliver_now"
JOB_TYPE_NEXT_CYCLE_NOW = "next_cycle_now"
logger = logging.getLogger(__name__)


def _log_bot_event(
    event: str,
    *,
    level: int = logging.INFO,
    message: str | None = None,
    exc_info: Any = None,
    user_id: Optional[int] = None,
    telegram_chat_id: Optional[int] = None,
    job_id: Optional[str] = None,
    job_type: Optional[str] = None,
    attempt: Optional[int] = None,
    idempotency_key: Optional[str] = None,
) -> None:
    log_event(
        logger,
        level,
        event,
        message=message,
        exc_info=exc_info,
        user_id=user_id,
        telegram_chat_id=telegram_chat_id,
        job_id=job_id,
        job_type=job_type,
        attempt=attempt,
        idempotency_key=idempotency_key,
    )

def is_cooled_down(app: Application, min_seconds: float = 2.0) -> bool:
    # Global callback throttle: one shared timestamp in app.bot_data for the whole bot
    # process (not per user/button). We use event-loop monotonic time to avoid issues
    # from system clock jumps.
    now = asyncio.get_event_loop().time()
    state = app.bot_data["cooldown"]
    last = float(state.get("last_ts", 0.0))
    if now - last < min_seconds:
        return False
    state["last_ts"] = now
    return True


async def enforce_cooldown(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
    min_seconds: float = 2.0,
) -> bool:
    if is_cooled_down(context.application, min_seconds):
        return True

    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None
    _log_bot_event(
        "action_throttled",
        message=f"action_throttled action={action}",
        user_id=user_id,
        telegram_chat_id=chat_id,
    )
    if update.callback_query is not None:
        await update.callback_query.answer("Too fast 🙂", show_alert=False)
    else:
        await reply(update, context, "Too fast 🙂")
    return False

def parse_time_hhmm(value: str) -> dt_time:
    # Accept strict HH:MM 24-hour format only.
    value = value.strip()
    if len(value) != 5 or value[2] != ":":
        raise ValueError("Time must be HH:MM (24h), e.g. 07:30")
    parsed = dt_time.fromisoformat(value)
    if parsed.second != 0 or parsed.microsecond != 0:
        raise ValueError("Time must be HH:MM (24h), e.g. 07:30")
    return parsed


def get_optional_env_int(name: str) -> Optional[int]:
    v = os.getenv(name)
    if v is None or not v.strip():
        return None
    return int(v)


def get_update_chat_id(update: Update) -> Optional[int]:
    return update.effective_chat.id if update.effective_chat is not None else None


def get_request_id(update: Update) -> str:
    # Deterministic request id to dedupe retried Telegram updates/commands.
    env = os.getenv("ENVIRONMENT", "dev")  # or "prod"
    scope = f"env:{env}"

    msg = getattr(update, "effective_message", None)
    if msg is not None and msg.message_id is not None:
        chat_id = getattr(update.effective_chat, "id", None)
        return str(uuid5(NAMESPACE_URL, f"{scope}:telegram-msg:{chat_id}:{msg.message_id}"))

    if update.update_id is not None:
        return str(uuid5(NAMESPACE_URL, f"{scope}:telegram-update:{update.update_id}"))

    # Fallback means: we cannot dedupe this request reliably
    return str(uuid4())


def _is_admin_override_chat(update: Update, admin_chat_id_override: Optional[int]) -> bool:
    chat_id = get_update_chat_id(update)
    return admin_chat_id_override is not None and chat_id == admin_chat_id_override


def register_user_from_update(update: Update) -> Optional[Dict[str, Any]]:
    # Register/update Telegram identity in DB and ensure defaults in app.user_settings.
    if update.effective_user is None or update.effective_chat is None:
        _log_bot_event("user_registration_skipped", level=logging.WARNING)
        return None
    user = upsert_user(
        telegram_user_id=update.effective_user.id,
        telegram_chat_id=update.effective_chat.id,
        username=update.effective_user.username,
    )
    ensure_user_settings(user["id"])
    _log_bot_event(
        "user_registered",
        user_id=user["id"],
        telegram_chat_id=update.effective_chat.id,
    )
    return user


async def require_allowlisted_user(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> Optional[Dict[str, Any]]:
    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None
    admin_chat_id_override = context.application.bot_data["admin_chat_id_override"]

    try:
        user = register_user_from_update(update)
    except Exception as e:
        if chat_id is not None:
            await notify_error(context, chat_id, f"Failed to register user for {action}", e)
        _log_bot_event(
            "user_registration_failed",
            level=logging.ERROR,
            message=f"user_registration_failed action={action}",
            exc_info=True,
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        return None

    if user is None:
        return None

    if _is_admin_override_chat(update, admin_chat_id_override):
        _log_bot_event(
            "access_granted_admin_override",
            message=f"access_granted_admin_override action={action}",
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        return user

    if not bool(user.get("allowlisted")):
        _log_bot_event(
            "access_denied_not_allowlisted",
            message=f"access_denied_not_allowlisted action={action} status={user.get('status')}",
            user_id=user.get("id"),
            telegram_chat_id=chat_id,
        )
        await reply(update, context, "Registered. Waiting for approval.")
        return None

    _log_bot_event(
        "access_granted",
        message=f"access_granted action={action} status={user.get('status')}",
        user_id=user.get("id"),
        telegram_chat_id=chat_id,
    )
    return user


async def require_admin_override(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> bool:
    admin_chat_id_override = context.application.bot_data["admin_chat_id_override"]
    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None

    if admin_chat_id_override is None:
        _log_bot_event(
            "admin_command_denied_unconfigured",
            level=logging.WARNING,
            message=f"admin_command_denied_unconfigured action={action}",
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        await reply(update, context, "Admin commands are disabled: ALLOWED_CHAT_ID is not configured.")
        return False

    if not _is_admin_override_chat(update, admin_chat_id_override):
        _log_bot_event(
            "admin_command_denied_unauthorized",
            level=logging.WARNING,
            message=f"admin_command_denied_unauthorized action={action}",
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        await reply(update, context, "This command is admin-only.")
        return False

    _log_bot_event(
        "admin_command_allowed",
        message=f"admin_command_allowed action={action}",
        user_id=user_id,
        telegram_chat_id=chat_id,
    )
    return True

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Log exceptions from handlers/jobs.
    _log_bot_event("handler_error", level=logging.ERROR, exc_info=context.error)

async def notify_error(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    title: str,
    exc: Exception,
) -> None:
    """
    Sends a user-friendly error message to Telegram and logs details.
    """
    error_id = uuid4().hex[:8]
    _log_bot_event(
        "notify_error",
        level=logging.ERROR,
        message=f"{title} [error_id={error_id}]",
        exc_info=exc,
        telegram_chat_id=chat_id,
    )

    # Do not expose raw exception details in user-visible messages.
    msg = (
        f"❌ {title}\n\n"
        "Internal error occurred. Check service logs for details.\n"
        f"error_id: {error_id}"
    )

    if is_auth_error(exc):
        msg = f"{msg}\n\n{format_auth_help()}"

    await context.bot.send_message(chat_id=chat_id, text=msg)

async def reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None) -> None:
    # Reply either to a normal message (/command) or to a callback query (button press).
    if update.message is not None:
        await update.message.reply_text(text=text, reply_markup=reply_markup)
        return

    if update.callback_query is not None and update.callback_query.message is not None:
        await update.callback_query.message.reply_text(text=text, reply_markup=reply_markup)
        return

    # Fallback: no route to reply.
    _log_bot_event("reply_skipped", level=logging.WARNING)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None
    if chat_id is None:
        _log_bot_event("start_missing_chat", level=logging.WARNING, user_id=user_id)
        return

    _log_bot_event("command_start", user_id=user_id, telegram_chat_id=chat_id)
    try:
        user = register_user_from_update(update)
    except Exception as e:
        await notify_error(context, chat_id, "Failed to register user", e)
        return
    if user is None:
        return

    if not bool(user.get("allowlisted")):
        await reply(update, context, "Registered. Waiting for approval.")
        return

    await reply(
        update,
        context,
        "Welcome, you're active. Use /settime and /settz.",
        reply_markup=build_keyboard(None),
    )


async def cmd_approve(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_override(update, context, "approve"):
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not context.args:
        await reply(update, context, "Usage: /approve <telegram_user_id>")
        return
    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await reply(update, context, "Invalid user id. Usage: /approve <telegram_user_id>")
        return

    try:
        row = approve_user(target_user_id)
    except Exception as e:
        await notify_error(context, chat_id, f"Failed to approve user {target_user_id}", e)
        return
    if row is None:
        await reply(update, context, f"User not found: telegram_user_id={target_user_id}")
        return

    _log_bot_event(
        "user_approved",
        user_id=row["id"],
        telegram_chat_id=row["telegram_chat_id"],
    )

    await reply(
        update,
        context,
        (
            f"✅ Approved telegram_user_id={row['telegram_user_id']}\n"
            f"status={row['status']} allowlisted={row['allowlisted']}"
        ),
    )


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_override(update, context, "block"):
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not context.args:
        await reply(update, context, "Usage: /block <telegram_user_id>")
        return
    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await reply(update, context, "Invalid user id. Usage: /block <telegram_user_id>")
        return

    try:
        row = block_user(target_user_id)
    except Exception as e:
        await notify_error(context, chat_id, f"Failed to block user {target_user_id}", e)
        return
    if row is None:
        await reply(update, context, f"User not found: telegram_user_id={target_user_id}")
        return

    _log_bot_event(
        "user_blocked",
        user_id=row["id"],
        telegram_chat_id=row["telegram_chat_id"],
    )

    await reply(
        update,
        context,
        (
            f"⛔ Blocked telegram_user_id={row['telegram_user_id']}\n"
            f"status={row['status']} allowlisted={row['allowlisted']}"
        ),
    )


async def cmd_admin_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_override(update, context, "admin_status"):
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return

    try:
        snapshot = get_admin_status_snapshot(pending_limit=20)
    except Exception as e:
        await notify_error(context, chat_id, "Failed to load admin status", e)
        return

    _log_bot_event("admin_status_requested", telegram_chat_id=chat_id)

    pending_users = snapshot["pending_users"]
    lines = ["Pending users:"]
    if pending_users:
        for row in pending_users:
            lines.append(
                f"- tg_user={row['telegram_user_id']} chat={row['telegram_chat_id']} created={row['created_at']} username={row['username']}"
            )
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            f"Queued jobs count: {snapshot['queued_jobs_count']}",
            f"Running jobs count: {snapshot['running_jobs_count']}",
            f"Failed/dead jobs count: {snapshot['failed_dead_jobs_count']}",
            "",
            "Last delivery per user:",
        ]
    )

    last_deliveries = snapshot["last_delivery_per_user"]
    if last_deliveries:
        for row in last_deliveries:
            delivered_at = row.get("delivered_at")
            delivered_text = _fmt_ts(delivered_at, context.application.bot_data["tz"]) if delivered_at else "n/a"
            lines.append(
                f"- user={row['user_id']} tg_user={row['telegram_user_id']} chat={row['telegram_chat_id']} "
                f"last={delivered_text} album={row.get('album_id') or 'n/a'} cycle={row.get('cycle_number') or 'n/a'}"
            )
    else:
        lines.append("- none")

    await reply(update, context, "\n".join(lines))


async def cmd_settz(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await require_allowlisted_user(update, context, "settz")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not context.args:
        await reply(update, context, "Usage: /settz <IANA_TZ>\nExample: /settz Europe/Riga")
        return

    tz_name = context.args[0].strip()
    try:
        ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        await reply(
            update,
            context,
            "Invalid timezone. Use IANA format, for example: Europe/Riga",
        )
        return

    try:
        settings = set_user_timezone(user["id"], tz_name)
    except Exception as e:
        await notify_error(context, chat_id, "Failed to save timezone", e)
        return

    _log_bot_event(
        "schedule_updated",
        message=f"schedule_updated field=timezone timezone={settings['timezone']}",
        user_id=user["id"],
        telegram_chat_id=chat_id,
    )

    await reply(
        update,
        context,
        f"✅ Timezone saved: {settings['timezone']}\nDaily time: {settings['daily_time_local']}",
    )


async def cmd_settime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await require_allowlisted_user(update, context, "settime")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not context.args:
        await reply(update, context, "Usage: /settime <HH:MM>\nExample: /settime 07:30")
        return

    raw_time = context.args[0].strip()
    try:
        parsed_time = parse_time_hhmm(raw_time)
    except ValueError:
        await reply(update, context, "Invalid time. Use HH:MM (24h), for example: 07:30")
        return

    try:
        settings = set_user_daily_time(user["id"], parsed_time)
    except Exception as e:
        await notify_error(context, chat_id, "Failed to save daily time", e)
        return

    _log_bot_event(
        "schedule_updated",
        message=f"schedule_updated field=daily_time_local daily_time_local={settings['daily_time_local']}",
        user_id=user["id"],
        telegram_chat_id=chat_id,
    )

    await reply(
        update,
        context,
        f"✅ Daily time saved: {settings['daily_time_local']}\nTimezone: {settings['timezone']}",
    )


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await require_allowlisted_user(update, context, "now")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not await enforce_cooldown(update, context, "now"):
        return

    _log_bot_event("command_now", user_id=user["id"], telegram_chat_id=chat_id)
    request_id = get_request_id(update)
    idem_key = f"manual:{user['id']}:{request_id}"

    try:
        now_utc = datetime.now(timezone.utc)
        row = enqueue_job_once(
            idempotency_key=idem_key,
            idempotency_expires_at=now_utc + timedelta(days=2),
            job_id=uuid4(),
            user_id=user["id"],
            job_type=JOB_TYPE_DELIVER_NOW,
            run_at=now_utc,
            payload={
                "telegram_chat_id": chat_id,
                "idempotency_key": idem_key,
            },
        )
        if row is not None:
            _log_bot_event(
                "manual_delivery_requested",
                user_id=user["id"],
                telegram_chat_id=chat_id,
                job_id=row.get("id"),
                job_type=JOB_TYPE_DELIVER_NOW,
                attempt=row.get("attempt"),
                idempotency_key=idem_key,
            )
            await reply(update, context, "Queued ✅")
        else:
            _log_bot_event(
                "manual_delivery_requested",
                message="manual_delivery_requested duplicate=true",
                user_id=user["id"],
                telegram_chat_id=chat_id,
                job_type=JOB_TYPE_DELIVER_NOW,
                idempotency_key=idem_key,
            )
            await reply(update, context, "Expect the previous album to arrive soon 🙂")
    except Exception as e:
        await notify_error(context, chat_id, "Failed to queue /now delivery", e)
        return


async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_chat_id_override = context.application.bot_data["admin_chat_id_override"]
    if not _is_admin_override_chat(update, admin_chat_id_override):
        chat_id = get_update_chat_id(update)
        user_id = update.effective_user.id if update.effective_user else None
        _log_bot_event("refresh_denied", user_id=user_id, telegram_chat_id=chat_id)
        await reply(update, context, "Not allowed.")
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not await enforce_cooldown(update, context, "refresh"):
        return

    user_id = update.effective_user.id if update.effective_user else None
    _log_bot_event("command_refresh", user_id=user_id, telegram_chat_id=chat_id)

    auth_path = context.application.bot_data["auth_path"]
    cache_path = context.application.bot_data["cache_path"]
    limit = context.application.bot_data["library_limit"]

    # Force sync of the cached album list.
    started_at = perf_counter()
    try:
        albums = get_albums_with_cache(
            auth_path=auth_path,
            cache_path=cache_path,
            refresh=True,
            limit=limit,
        )
    except Exception as e:
        await notify_error(context, chat_id, "Failed to refresh library cache", e)
        return

    elapsed_ms = int((perf_counter() - started_at) * 1000)
    _log_bot_event(
        "library_refreshed",
        message=f"library_refreshed source=command albums={len(albums)} elapsed_ms={elapsed_ms}",
        user_id=user_id,
        telegram_chat_id=chat_id,
    )
    if not albums:
        _log_bot_event(
            "library_refresh_empty",
            level=logging.WARNING,
            message="library_refresh_empty source=command",
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
    await reply(update, context, f"✅ Refreshed. Cached albums: {len(albums)}", reply_markup=build_keyboard(None))


async def cmd_nextcycle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await require_allowlisted_user(update, context, "nextcycle")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not await enforce_cooldown(update, context, "nextcycle"):
        return

    _log_bot_event("command_nextcycle", user_id=user["id"], telegram_chat_id=chat_id)
    request_id = get_request_id(update)
    idem_key = f"nextcycle:{user['id']}:{request_id}"
    try:
        now_utc = datetime.now(timezone.utc)
        row = enqueue_job_once(
            idempotency_key=idem_key,
            idempotency_expires_at=now_utc + timedelta(days=2),
            job_id=uuid4(),
            user_id=user["id"],
            job_type=JOB_TYPE_NEXT_CYCLE_NOW,
            run_at=now_utc,
            payload={
                "telegram_chat_id": chat_id,
                "idempotency_key": idem_key,
                "force_next_cycle": True,
            },
        )
        if row is not None:
            _log_bot_event(
                "manual_delivery_requested",
                message="manual_delivery_requested force_next_cycle=true",
                user_id=user["id"],
                telegram_chat_id=chat_id,
                job_id=row.get("id"),
                job_type=JOB_TYPE_NEXT_CYCLE_NOW,
                attempt=row.get("attempt"),
                idempotency_key=idem_key,
            )
            await reply(update, context, "Queued ✅ New cycle album will arrive soon")
        else:
            _log_bot_event(
                "manual_delivery_requested",
                message="manual_delivery_requested duplicate=true force_next_cycle=true",
                user_id=user["id"],
                telegram_chat_id=chat_id,
                job_type=JOB_TYPE_NEXT_CYCLE_NOW,
                idempotency_key=idem_key,
            )
            await reply(update, context, "Expect the previous new-cycle album to arrive soon 🙂")
    except Exception as e:
        await notify_error(context, chat_id, "Failed to queue /nextcycle delivery", e)
        return

def _fmt_ts(ts: Optional[Any], tz: ZoneInfo) -> str:
    if not ts:
        return "n/a"
    dt = ts.astimezone(tz)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def resolve_app_timezone(
    admin_chat_id_override: Optional[int],
    default_timezone_name: str = "UTC",
) -> ZoneInfo:
    timezone_name = default_timezone_name

    if admin_chat_id_override is not None:
        db_timezone_name = get_user_timezone_by_chat_id(admin_chat_id_override)
        if db_timezone_name:
            timezone_name = db_timezone_name

    return ZoneInfo(timezone_name)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await require_allowlisted_user(update, context, "status")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return

    _log_bot_event("command_status", user_id=user["id"], telegram_chat_id=chat_id)

    cache_path = context.application.bot_data["cache_path"]
    tz = context.application.bot_data["tz"]
    try:
        settings = get_user_settings(user["id"])
        db_stats = get_user_delivery_stats(user["id"])
        recent_deliveries = list_recent_deliveries(user["id"], limit=5)
    except Exception as e:
        await notify_error(context, chat_id, "Failed to load DB status", e)
        return

    cache_payload = load_cache_payload(cache_path) or {}
    albums = cache_payload.get("albums") or []
    cache_updated = cache_payload.get("updated_at")

    total = len(albums)
    _log_bot_event(
        "status_snapshot",
        message=f"status_snapshot cached_albums={total}",
        user_id=user["id"],
        telegram_chat_id=chat_id,
    )
    if total == 0:
        _log_bot_event(
            "status_empty_cache",
            level=logging.WARNING,
            user_id=user["id"],
            telegram_chat_id=chat_id,
        )

    msg_lines = [
        "DB user:",
        f"Access: allowlisted={user.get('allowlisted')} status={user.get('status')}",
        f"Timezone: {settings.get('timezone')}",
        f"Daily time: {settings.get('daily_time_local')}",
        f"DB deliveries total: {db_stats.get('total_deliveries')}",
        f"DB latest cycle: {db_stats.get('latest_cycle_number') or 'n/a'}",
        f"DB sent in latest cycle: {db_stats.get('latest_cycle_count')}",
        f"DB last delivered: {_fmt_ts(db_stats.get('last_delivered_at'), tz)}",
        "",
        "Recent DB deliveries:",
    ]
    if recent_deliveries:
        for row in recent_deliveries:
            msg_lines.append(
                f"- {_fmt_ts(row.get('delivered_at'), tz)} album={row.get('album_id')} cycle={row.get('cycle_number')}"
            )
    else:
        msg_lines.append("- none")

    msg_lines.extend(["", "Local cache:", f"Cached albums: {total}", f"Cache updated: {_fmt_ts(cache_updated, tz)}"])

    await reply(update, context, "\n".join(msg_lines), reply_markup=build_keyboard(None))


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    # Acknowledge callback immediately so Telegram stops showing the loading spinner.
    # In throttled branches we answer again with a short toast for user feedback.
    await query.answer()

    data = query.data
    user = await require_allowlisted_user(update, context, f"callback:{data or 'unknown'}")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    _log_bot_event(
        "callback_received",
        message=f"callback_received action={data}",
        user_id=user.get("id"),
        telegram_chat_id=chat_id,
    )
    if data == CB_NEXT:
        # Reuse the /now logic.
        fake_update = update
        await cmd_now(fake_update, context)
        return

    if data == CB_REFRESH:
        await cmd_refresh(update, context)
        return

    if data == CB_STATUS:
        await cmd_status(update, context)
        return


def main() -> None:
    load_dotenv()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    admin_chat_id_override = get_optional_env_int("ALLOWED_CHAT_ID")
    default_timezone_name = os.getenv("DEFAULT_TIMEZONE", "UTC").strip() or "UTC"
    tz = resolve_app_timezone(admin_chat_id_override, default_timezone_name)

    auth_path = os.getenv("YTM_AUTH_PATH", "secrets/browser.json")
    cache_path = os.getenv("ALBUM_CACHE_PATH", "data/albums_cache.json")
    library_limit = int(os.getenv("LIBRARY_LIMIT", "500"))
    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    log_level = getattr(logging, log_level_name, None)
    if not isinstance(log_level, int):
        raise RuntimeError(f"Invalid LOG_LEVEL: {log_level_name}")

    configure_logging(log_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    _log_bot_event(
        "bot_started",
        message=(
            f"bot_started tz={tz.key} "
            f"library_limit={library_limit} allowed_chat_id={admin_chat_id_override}"
        ),
    )

    app = Application.builder().token(token).defaults(Defaults(tzinfo=tz)).build()

    # Store config in bot_data so handlers/jobs can access it.
    app.bot_data["admin_chat_id_override"] = admin_chat_id_override
    app.bot_data["auth_path"] = auth_path
    app.bot_data["cache_path"] = cache_path
    app.bot_data["library_limit"] = library_limit
    app.bot_data["tz"] = tz
    app.bot_data["cooldown"] = {"last_ts": 0.0}

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("approve", cmd_approve))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("admin_status", cmd_admin_status))
    app.add_handler(CommandHandler("settz", cmd_settz))
    app.add_handler(CommandHandler("settime", cmd_settime))
    app.add_handler(CommandHandler("now", cmd_now))
    app.add_handler(CommandHandler("nextcycle", cmd_nextcycle))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_error_handler(on_error)

    # Buttons (callback queries)
    app.add_handler(CallbackQueryHandler(on_callback))

    # Start polling (no public IP required)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
