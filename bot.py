import asyncio
import logging
import os
from datetime import datetime, time as dt_time
from uuid import uuid4
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
)

from src.history import load_history, history_count
from src.formatting import album_message, album_url
from src.library import get_albums_with_cache, load_cache_payload
from src.picker import pick_random_album_no_repeat
from src.errors import is_auth_error, format_auth_help


Album = Dict[str, Any]

CB_NEXT = "NEXT_ALBUM"
CB_REFRESH = "REFRESH_LIBRARY"
CB_STATUS = "STATUS"


def build_keyboard(open_url: Optional[str]) -> InlineKeyboardMarkup:
    # Buttons trigger callback queries handled by CallbackQueryHandler.
    row1 = [
        InlineKeyboardButton("🎲 Another album", callback_data=CB_NEXT),
        InlineKeyboardButton("🔄 Refresh library", callback_data=CB_REFRESH),
    ]
    row2 = [InlineKeyboardButton("📊 Status", callback_data=CB_STATUS)]

    rows = [row1, row2]

    if open_url:
        rows.insert(0, [InlineKeyboardButton("🔗 Open album", url=open_url)])

    return InlineKeyboardMarkup(rows)

def is_cooled_down(app: Application, min_seconds: float = 2.0) -> bool:
    now = asyncio.get_event_loop().time()
    state = app.bot_data["cooldown"]
    last = float(state.get("last_ts", 0.0))
    if now - last < min_seconds:
        return False
    state["last_ts"] = now
    return True

async def send_album(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    album: Album,
    prefix: Optional[str] = None,
) -> None:
    # Send album message and attach action buttons.
    text = album_message(album)
    if prefix:
        text = f"{prefix}\n\n{text}"

    url = album_url(album)
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=build_keyboard(url),
        disable_web_page_preview=False,
    )


def parse_daily_time(value: str) -> dt_time:
    # Parse HH:MM into datetime.time.
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError("DAILY_TIME must be in HH:MM format, e.g. 09:30")
    hh = int(parts[0])
    mm = int(parts[1])
    return dt_time(hour=hh, minute=mm)


def get_env_int(name: str) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        raise RuntimeError(f"{name} is not set")
    return int(v)


def is_allowed_chat(update: Update, allowed_chat_id: int) -> bool:
    # Only allow actions from a single chat_id (single-user bot).
    if update.effective_chat is None:
        return False
    return update.effective_chat.id == allowed_chat_id

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

    # Fallback: send directly to the allowed chat (should be rare).
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    await context.bot.send_message(chat_id=allowed_chat_id, text=text, reply_markup=reply_markup)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    await reply(update, context, "Ready. Use /now to get an album, or wait for the daily post.", reply_markup=build_keyboard(None))


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    auth_path = context.application.bot_data["auth_path"]
    cache_path = context.application.bot_data["cache_path"]
    history_path = context.application.bot_data["history_path"]
    limit = context.application.bot_data["library_limit"]

    try:
        album, refreshed = pick_random_album_no_repeat(
            auth_path=auth_path,
            cache_path=cache_path,
            history_path=history_path,
            library_limit=limit,
        )
    except Exception as e:
        await notify_error(context, allowed_chat_id, "Failed to pick an album", e)
        return

    prefix = "🔄 Library refreshed (cycle restarted)" if refreshed else None
    await send_album(context, allowed_chat_id, album, prefix=prefix)


async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    auth_path = context.application.bot_data["auth_path"]
    cache_path = context.application.bot_data["cache_path"]
    limit = context.application.bot_data["library_limit"]

    # Force sync of the cached album list.
    try:
        albums = get_albums_with_cache(
            auth_path=auth_path,
            cache_path=cache_path,
            refresh=True,
            limit=limit,
        )
    except Exception as e:
        await notify_error(context, allowed_chat_id, "Failed to refresh library cache", e)
        return

    await reply(update, context, f"✅ Refreshed. Cached albums: {len(albums)}", reply_markup=build_keyboard(None))

def _fmt_ts(ts: Optional[int], tz: ZoneInfo) -> str:
    if not ts:
        return "n/a"
    dt = datetime.fromtimestamp(int(ts), tz=tz)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    cache_path = context.application.bot_data["cache_path"]
    history_path = context.application.bot_data["history_path"]
    tz = context.application.bot_data["tz"]

    cache_payload = load_cache_payload(cache_path) or {}
    albums = cache_payload.get("albums") or []
    cache_updated = cache_payload.get("updated_at")

    history = load_history(history_path)
    sent_n = history_count(history)
    hist_updated = history.get("updated_at")

    total = len(albums)
    remaining = max(total - sent_n, 0)

    msg_lines = [
        f"Cached albums: {total}",
        f"Sent in current cycle: {sent_n}",
        f"Remaining: {remaining}",
        f"Cache updated: {_fmt_ts(cache_updated, tz)}",
        f"History updated: {_fmt_ts(hist_updated, tz)}",
    ]

    await reply(update, context, "\n".join(msg_lines), reply_markup=build_keyboard(None))


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    query = update.callback_query
    if query is None:
        return

    await query.answer()

    data = query.data
    if data == CB_NEXT:
        if not is_cooled_down(context.application, 2.0):
            await query.answer("Too fast 🙂", show_alert=False)
            return
        # Reuse the /now logic.
        fake_update = update
        await cmd_now(fake_update, context)
        return

    if data == CB_REFRESH:
        if not is_cooled_down(context.application, 2.0):
            await query.answer("Too fast 🙂", show_alert=False)
            return
        auth_path = context.application.bot_data["auth_path"]
        cache_path = context.application.bot_data["cache_path"]
        limit = context.application.bot_data["library_limit"]
        
        try:
            albums = get_albums_with_cache(
                auth_path=auth_path,
                cache_path=cache_path,
                refresh=True,
                limit=limit,
            )
        except Exception as e:
            await notify_error(context, allowed_chat_id, "Failed to refresh library cache", e)
            return

        await query.message.reply_text(
            f"✅ Refreshed. Cached albums: {len(albums)}",
            reply_markup=build_keyboard(None),
        )
        return

    if data == CB_STATUS:
        await cmd_status(update, context)
        return


async def daily_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    # Daily scheduled job: pick and send an album to the allowed chat.
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]

    auth_path = context.application.bot_data["auth_path"]
    cache_path = context.application.bot_data["cache_path"]
    history_path = context.application.bot_data["history_path"]
    limit = context.application.bot_data["library_limit"]

    try:
        album, refreshed = pick_random_album_no_repeat(
            auth_path=auth_path,
            cache_path=cache_path,
            history_path=history_path,
            library_limit=limit,
        )
    except Exception as e:
        await notify_error(context, allowed_chat_id, "Daily job failed (cannot pick album)", e)
        return

    prefix = "🔄 Library refreshed (cycle restarted)" if refreshed else "📅 Daily album"
    await send_album(context, allowed_chat_id, album, prefix=prefix)


def main() -> None:
    load_dotenv()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    allowed_chat_id = get_env_int("ALLOWED_CHAT_ID")

    tz_name = os.getenv("TZ", "Europe/Riga")
    tz = ZoneInfo(tz_name)

    daily_time_str = os.getenv("DAILY_TIME", "09:30")
    daily_time = parse_daily_time(daily_time_str)
    daily_time = daily_time.replace(tzinfo=tz)

    # Paths & limits (tweakable without code changes if you want later)
    auth_path = os.getenv("YTM_AUTH_PATH", "secrets/browser.json")
    cache_path = os.getenv("ALBUM_CACHE_PATH", "data/albums_cache.json")
    history_path = os.getenv("HISTORY_PATH", "data/sent_history.json")
    library_limit = int(os.getenv("LIBRARY_LIMIT", "500"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = Application.builder().token(token).defaults(Defaults(tzinfo=tz)).build()

    # Store config in bot_data so handlers/jobs can access it.
    app.bot_data["allowed_chat_id"] = allowed_chat_id
    app.bot_data["auth_path"] = auth_path
    app.bot_data["cache_path"] = cache_path
    app.bot_data["history_path"] = history_path
    app.bot_data["library_limit"] = library_limit
    app.bot_data["tz"] = tz
    app.bot_data["cooldown"] = {"last_ts": 0.0}

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("now", cmd_now))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_error_handler(on_error)

    # Buttons (callback queries)
    app.add_handler(CallbackQueryHandler(on_callback))

    # Daily scheduled job in local timezone
    app.job_queue.run_daily(
        daily_job,
        time=daily_time,
        days=(0, 1, 2, 3, 4, 5, 6),
        name="daily_album_job",
        job_kwargs={"misfire_grace_time": 120, "coalesce": True, "max_instances": 1},
    )

    # Start polling (no public IP required)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
