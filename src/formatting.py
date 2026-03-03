from typing import Any, Dict, Optional
from urllib.parse import quote

Album = Dict[str, Any]


def _safe_text(value: Any) -> str:
    # Normalize potentially untrusted text from external metadata.
    text = str(value or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
    return text


def album_url(album: Album) -> Optional[str]:
    """
    We’re trying to construct a URL for an album in YouTube Music.
    Typically, the browseId looks like MPREb_... or something similar.
    """
    bid = album.get("browseId")
    if not bid:
        return None
    return f"https://music.youtube.com/browse/{quote(str(bid))}"

def album_message(album: Album) -> str:
    title = _safe_text(album.get("title"))
    artist = _safe_text(album.get("artist"))
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
