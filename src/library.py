import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ytmusicapi import YTMusic


Album = Dict[str, Any]


def _normalize_album(raw: Dict[str, Any]) -> Album:
    """
    Normalize the ytmusicapi response into a stable format for downstream use.
    """
    artists = raw.get("artists") or []
    artist = ", ".join(a.get("name", "") for a in artists if a.get("name")) or raw.get("artist", "")

    return {
        "title": raw.get("title", "").strip(),
        "artist": artist.strip(),
        "year": raw.get("year"),
        "browseId": raw.get("browseId") or raw.get("audioPlaylistId") or raw.get("playlistId"),
        "thumbnails": raw.get("thumbnails") or [],
    }


def fetch_library_albums(
    auth_path: str,
    limit: Optional[int] = None,
) -> List[Album]:
    """
    Fetch albums directly from the YT Music library.
    limit=None returns as many items as the method provides (pagination may be needed later).
    """
    yt = YTMusic(auth_path)
    # ytmusicapi supports limit; None or a large value usually returns more results
    raw_albums = yt.get_library_albums(limit=limit)
    albums = [_normalize_album(a) for a in raw_albums]

    # Remove invalid entries without ID/title
    albums = [a for a in albums if a.get("browseId") and a.get("title")]
    return albums


def load_cached_albums(cache_path: str) -> Optional[List[Album]]:
    p = Path(cache_path)
    if not p.exists():
        return None
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("albums")


def save_cached_albums(cache_path: str, albums: List[Album]) -> None:
    p = Path(cache_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": int(time.time()), "albums": albums}
    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def get_albums_with_cache(
    auth_path: str,
    cache_path: str = "data/albums_cache.json",
    refresh: bool = False,
    limit: Optional[int] = None,
) -> List[Album]:
    """
    Unified entry point:
    - if cache exists and refresh=False, read from cache
    - otherwise fetch from YT Music and update cache
    """
    if not refresh:
        cached = load_cached_albums(cache_path)
        if cached:
            return cached

    albums = fetch_library_albums(auth_path=auth_path, limit=limit)
    save_cached_albums(cache_path, albums)
    return albums

def load_cache_payload(cache_path: str) -> Optional[Dict[str, Any]]:
    p = Path(cache_path)
    if not p.exists():
        return None
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)