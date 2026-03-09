import asyncio
import logging
import os
from datetime import datetime, time as dt_time, timedelta, timezone
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
from src.db import (
    approve_user,
    block_user,
    enqueue_job,
    ensure_user_settings,
    get_user_delivery_stats,
    get_user_settings,
    list_recent_deliveries,
    list_pending_users,
    set_user_daily_time,
    set_user_timezone,
    try_insert_idempotency_key,
    upsert_user,
)
from src.telegram_delivery import (
    CB_NEXT,
    CB_REFRESH,
    CB_STATUS,
    build_keyboard,
    send_album_message,
)


Album = Dict[str, Any]
JOB_TYPE_DELIVER_NOW = "deliver_now"
JOB_TYPE_NEXT_CYCLE_NOW = "next_cycle_now"

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
    logging.info("Action throttled action=%s chat_id=%s user_id=%s", action, chat_id, user_id)
    if update.callback_query is not None:
        await update.callback_query.answer("Too fast 🙂", show_alert=False)
    else:
        await reply(update, context, "Too fast 🙂")
    return False

def parse_daily_time(value: str) -> dt_time:
    # Parse HH:MM into datetime.time.
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError("DAILY_TIME must be in HH:MM format, e.g. 09:30")
    hh = int(parts[0])
    mm = int(parts[1])
    return dt_time(hour=hh, minute=mm)


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
        logging.warning("Cannot register user: missing effective_user or effective_chat")
        return None
    user = upsert_user(
        telegram_user_id=update.effective_user.id,
        telegram_chat_id=update.effective_chat.id,
        username=update.effective_user.username,
    )
    ensure_user_settings(user["id"])
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
        logging.exception("DB registration failed action=%s chat_id=%s user_id=%s", action, chat_id, user_id)
        return None

    if user is None:
        return None

    if _is_admin_override_chat(update, admin_chat_id_override):
        logging.info(
            "Access granted via admin override action=%s chat_id=%s user_id=%s",
            action,
            chat_id,
            user_id,
        )
        return user

    if not bool(user.get("allowlisted")):
        logging.info(
            "Access denied (not allowlisted) action=%s chat_id=%s user_id=%s status=%s",
            action,
            chat_id,
            user_id,
            user.get("status"),
        )
        await reply(update, context, "Registered. Waiting for approval.")
        return None

    logging.info(
        "Access granted action=%s chat_id=%s user_id=%s status=%s",
        action,
        chat_id,
        user_id,
        user.get("status"),
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
        logging.warning(
            "Admin command denied (override not configured) action=%s chat_id=%s user_id=%s",
            action,
            chat_id,
            user_id,
        )
        await reply(update, context, "Admin commands are disabled: ALLOWED_CHAT_ID is not configured.")
        return False

    if not _is_admin_override_chat(update, admin_chat_id_override):
        logging.warning(
            "Admin command denied (unauthorized chat) action=%s chat_id=%s user_id=%s",
            action,
            chat_id,
            user_id,
        )
        await reply(update, context, "This command is admin-only.")
        return False

    logging.info("Admin command allowed action=%s chat_id=%s user_id=%s", action, chat_id, user_id)
    return True

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Log exceptions from handlers/jobs.
    logging.exception("Unhandled exception in handler/job", exc_info=context.error)

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
    logging.exception("%s [error_id=%s]: %s", title, error_id, exc)

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
    logging.warning("Reply skipped: no message or callback message in update")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None
    if chat_id is None:
        logging.warning("Command /start without effective_chat user_id=%s", user_id)
        return

    logging.info("Command /start chat_id=%s user_id=%s", chat_id, user_id)
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

    await reply(
        update,
        context,
        (
            f"⛔ Blocked telegram_user_id={row['telegram_user_id']}\n"
            f"status={row['status']} allowlisted={row['allowlisted']}"
        ),
    )


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_override(update, context, "pending"):
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return

    try:
        rows = list_pending_users(limit=20)
    except Exception as e:
        await notify_error(context, chat_id, "Failed to list pending users", e)
        return

    if not rows:
        await reply(update, context, "No pending users.")
        return

    lines = ["Pending users (latest 20):"]
    for row in rows:
        lines.append(
            f"- {row['telegram_user_id']} chat={row['telegram_chat_id']} created={row['created_at']} username={row['username']}"
        )
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

    user_id = update.effective_user.id if update.effective_user else None
    logging.info("Command /now chat_id=%s user_id=%s", chat_id, user_id)
    request_id = get_request_id(update)
    idem_key = f"manual:{user['id']}:{request_id}"

    try:
        created = try_insert_idempotency_key(
            key=idem_key,
            user_id=user["id"],
            job_type=JOB_TYPE_DELIVER_NOW,
            expires_at=datetime.now(timezone.utc) + timedelta(days=2),
        )
        if created:
            enqueue_job(
                job_id=uuid4(),
                user_id=user["id"],
                job_type=JOB_TYPE_DELIVER_NOW,
                run_at=datetime.now(timezone.utc),
                payload={
                    "telegram_chat_id": chat_id,
                    "idempotency_key": idem_key,
                },
            )
            logging.info(
                "Queued deliver_now user_id=%s telegram_user_id=%s request_id=%s",
                user["id"],
                user_id,
                request_id,
            )
            await reply(update, context, "Queued ✅")
        else:
            logging.info(
                "Skipped duplicate deliver_now enqueue user_id=%s telegram_user_id=%s request_id=%s",
                user["id"],
                user_id,
                request_id,
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
        logging.info("Refresh denied chat_id=%s user_id=%s", chat_id, user_id)
        await reply(update, context, "Not allowed.")
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    if not await enforce_cooldown(update, context, "refresh"):
        return

    user_id = update.effective_user.id if update.effective_user else None
    logging.info("Command /refresh chat_id=%s user_id=%s", chat_id, user_id)

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
    logging.info("Library refreshed source=command albums=%s elapsed_ms=%s", len(albums), elapsed_ms)
    if not albums:
        logging.warning("Library refresh returned empty album list source=command")
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

    user_id = update.effective_user.id if update.effective_user else None
    logging.info("Command /nextcycle chat_id=%s user_id=%s", chat_id, user_id)
    request_id = get_request_id(update)
    idem_key = f"nextcycle:{user['id']}:{request_id}"
    try:
        created = try_insert_idempotency_key(
            key=idem_key,
            user_id=user["id"],
            job_type=JOB_TYPE_NEXT_CYCLE_NOW,
            expires_at=datetime.now(timezone.utc) + timedelta(days=2),
        )
        if created:
            enqueue_job(
                job_id=uuid4(),
                user_id=user["id"],
                job_type=JOB_TYPE_NEXT_CYCLE_NOW,
                run_at=datetime.now(timezone.utc),
                payload={
                    "telegram_chat_id": chat_id,
                    "idempotency_key": idem_key,
                    "force_next_cycle": True,
                },
            )
            logging.info(
                "Queued next_cycle_now user_id=%s telegram_user_id=%s request_id=%s",
                user["id"],
                user_id,
                request_id,
            )
            await reply(update, context, "Queued ✅ New cycle album will arrive soon")
        else:
            logging.info(
                "Skipped duplicate next_cycle_now enqueue user_id=%s telegram_user_id=%s request_id=%s",
                user["id"],
                user_id,
                request_id,
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

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await require_allowlisted_user(update, context, "status")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return

    user_id = update.effective_user.id if update.effective_user else None
    logging.info("Command /status chat_id=%s user_id=%s", chat_id, user_id)

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
    logging.info("Status snapshot cached_albums=%s", total)
    if total == 0:
        logging.warning("Status shows empty cached library")

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
    user_id = update.effective_user.id if update.effective_user else None
    logging.info("Callback action=%s chat_id=%s user_id=%s", data, chat_id, user_id)
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

    tz_name = os.getenv("TZ", "Europe/Riga")
    tz = ZoneInfo(tz_name)

    daily_time_str = os.getenv("DAILY_TIME", "09:30")
    parse_daily_time(daily_time_str)

    auth_path = os.getenv("YTM_AUTH_PATH", "secrets/browser.json")
    cache_path = os.getenv("ALBUM_CACHE_PATH", "data/albums_cache.json")
    library_limit = int(os.getenv("LIBRARY_LIMIT", "500"))
    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    log_level = getattr(logging, log_level_name, None)
    if not isinstance(log_level, int):
        raise RuntimeError(f"Invalid LOG_LEVEL: {log_level_name}")

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.info(
        "Bot startup TZ=%s DAILY_TIME=%s LIBRARY_LIMIT=%s ALLOWED_CHAT_ID=%s",
        tz_name,
        daily_time_str,
        library_limit,
        admin_chat_id_override,
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
    app.add_handler(CommandHandler("pending", cmd_pending))
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
