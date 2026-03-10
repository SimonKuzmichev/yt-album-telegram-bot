from typing import Any, Dict, Optional

Album = Dict[str, Any]


def _safe_text(value: Any) -> str:
    # Normalize potentially untrusted text from external metadata.
    text = str(value or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()
    return text


def album_url(album: Album) -> Optional[str]:
    url = album.get("url")
    if not url:
        return None
    return str(url)

def album_message(album: Album) -> str:
    title = _safe_text(album.get("title"))
    artist = _safe_text(album.get("artist"))
    year = album.get("release_year")

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
