import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application

from src.formatting import album_message, album_url

Album = Dict[str, Any]


def _build_keyboard(album: Album) -> Optional[InlineKeyboardMarkup]:
    # Build an inline keyboard with a single "Open album" button, if URL is available.
    url = album_url(album)
    if not url:
        return None

    keyboard = [[InlineKeyboardButton(text="Open album", url=url)]]
    return InlineKeyboardMarkup(keyboard)


async def send_album_message(album: Album) -> None:
    """
    Sends a single album message to the configured CHAT_ID using Telegram Bot API.

    Required environment variables:
      - BOT_TOKEN
      - CHAT_ID
    """
    load_dotenv()

    token = os.getenv("BOT_TOKEN")
    chat_id = os.getenv("CHAT_ID")

    if not token:
        raise RuntimeError("BOT_TOKEN is not set (check your .env or environment variables).")
    if not chat_id:
        raise RuntimeError("CHAT_ID is not set (check your .env or environment variables).")

    text = album_message(album)
    keyboard = _build_keyboard(album)

    # Create an application instance just to send a message (no polling/webhook needed).
    app = Application.builder().token(token).build()

    # Using HTML parse mode is safe here because we are not injecting untrusted HTML.
    await app.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=False,
        reply_markup=keyboard,
    )