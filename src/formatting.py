from typing import Any, Dict, Optional
from urllib.parse import quote

Album = Dict[str, Any]


def album_url(album: Album) -> Optional[str]:
    """
    Пытаемся построить URL на альбом в YouTube Music.
    Обычно browseId выглядит как 'MPREb_...' или похожий.
    """
    bid = album.get("browseId")
    if not bid:
        return None
    return f"https://music.youtube.com/browse/{quote(str(bid))}"

def album_message(album: Album) -> str:
    title = (album.get("title") or "").strip()
    artist = (album.get("artist") or "").strip()
    year = album.get("year")

    lines = []
    if title and artist:
        lines.append(f"🎧 {artist} — {title}")
    elif title:
        lines.append(f"🎧 {title}")
    else:
        lines.append("🎧 Альбом дня")

    if year:
        lines.append(f"📅 {year}")

    url = album_url(album)
    if url:
        lines.append(url)

    return "\n".join(lines)


def album_button_data(album: Album) -> Optional[dict]:
    url = album_url(album)
    if not url:
        return None
    return {"text": "Открыть альбом", "url": url}