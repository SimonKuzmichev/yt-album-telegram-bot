import asyncio
import logging
import os
from datetime import time as dt_time
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

from src.formatting import album_message, album_url
from src.library import get_albums_with_cache
from src.picker import pick_random_album_no_repeat


Album = Dict[str, Any]

CB_NEXT = "NEXT_ALBUM"
CB_REFRESH = "REFRESH_LIBRARY"
CB_STATUS = "STATUS"


def build_keyboard() -> InlineKeyboardMarkup:
    # Buttons trigger callback queries handled by CallbackQueryHandler.
    keyboard = [
        [
            InlineKeyboardButton("🎲 Another album", callback_data=CB_NEXT),
            InlineKeyboardButton("🔄 Refresh library", callback_data=CB_REFRESH),
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data=CB_STATUS),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


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

    # If we have a URL, Telegram will auto-link it; message also includes inline buttons.
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=build_keyboard(),
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


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    await update.message.reply_text(
        "Ready. Use /now to get an album, or wait for the daily post.",
        reply_markup=build_keyboard(),
    )


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    auth_path = context.application.bot_data["auth_path"]
    cache_path = context.application.bot_data["cache_path"]
    history_path = context.application.bot_data["history_path"]
    limit = context.application.bot_data["library_limit"]

    album, refreshed = pick_random_album_no_repeat(
        auth_path=auth_path,
        cache_path=cache_path,
        history_path=history_path,
        library_limit=limit,
    )

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
    albums = get_albums_with_cache(
        auth_path=auth_path,
        cache_path=cache_path,
        refresh=True,
        limit=limit,
    )

    await update.message.reply_text(
        f"✅ Refreshed. Cached albums: {len(albums)}",
        reply_markup=build_keyboard(),
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed_chat_id = context.application.bot_data["allowed_chat_id"]
    if not is_allowed_chat(update, allowed_chat_id):
        return

    cache_path = context.application.bot_data["cache_path"]
    history_path = context.application.bot_data["history_path"]

    # Keep it simple: report cache/history file presence and sizes.
    cache_exists = os.path.exists(cache_path)
    hist_exists = os.path.exists(history_path)

    msg_lines = [
        f"Cache file: {'yes' if cache_exists else 'no'} ({cache_path})",
        f"History file: {'yes' if hist_exists else 'no'} ({history_path})",
    ]

    await update.message.reply_text(
        "\n".join(msg_lines),
        reply_markup=build_keyboard(),
    )


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
        # Reuse the /now logic.
        fake_update = update
        await cmd_now(fake_update, context)
        return

    if data == CB_REFRESH:
        auth_path = context.application.bot_data["auth_path"]
        cache_path = context.application.bot_data["cache_path"]
        limit = context.application.bot_data["library_limit"]

        albums = get_albums_with_cache(
            auth_path=auth_path,
            cache_path=cache_path,
            refresh=True,
            limit=limit,
        )

        await query.message.reply_text(
            f"✅ Refreshed. Cached albums: {len(albums)}",
            reply_markup=build_keyboard(),
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

    album, refreshed = pick_random_album_no_repeat(
        auth_path=auth_path,
        cache_path=cache_path,
        history_path=history_path,
        library_limit=limit,
    )

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

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("now", cmd_now))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("status", cmd_status))

    # Buttons (callback queries)
    app.add_handler(CallbackQueryHandler(on_callback))

    # Daily scheduled job in local timezone
    app.job_queue.run_daily(
        daily_job,
        time=daily_time,
        days=(0, 1, 2, 3, 4, 5, 6),
        name="daily_album_job",
    )

    # Start polling (no public IP required)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()