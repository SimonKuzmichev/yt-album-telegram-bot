from typing import Any, Dict, Optional

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from src.formatting import album_message, album_url


Album = Dict[str, Any]

CB_NEXT = "NEXT_ALBUM"
CB_REFRESH = "REFRESH_LIBRARY"
CB_STATUS = "STATUS"


def build_keyboard(open_url: Optional[str]) -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton("🎲 Another album", callback_data=CB_NEXT),
        InlineKeyboardButton("🔄 Refresh library", callback_data=CB_REFRESH),
    ]
    row2 = [InlineKeyboardButton("📊 Status", callback_data=CB_STATUS)]
    rows = [row1, row2]

    if open_url:
        rows.insert(0, [InlineKeyboardButton("🔗 Open album", url=open_url)])

    return InlineKeyboardMarkup(rows)


def build_album_text(album: Album, prefix: Optional[str] = None) -> str:
    text = album_message(album)
    if prefix:
        return f"{prefix}\n\n{text}"
    return text


async def send_album_message(
    bot: Bot,
    chat_id: int,
    album: Album,
    prefix: Optional[str] = None,
) -> None:
    url = album_url(album)
    await bot.send_message(
        chat_id=chat_id,
        text=build_album_text(album, prefix=prefix),
        reply_markup=build_keyboard(url),
        disable_web_page_preview=False,
    )
