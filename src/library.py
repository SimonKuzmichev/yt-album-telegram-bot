import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.providers import NormalizedAlbum, ProviderClient


def _coerce_legacy_album(album: Dict[str, Any], default_provider: str) -> Optional[NormalizedAlbum]:
    provider_album_id = str(
        album.get("provider_album_id")
        or album.get("browseId")
        or album.get("audioPlaylistId")
        or album.get("playlistId")
        or ""
    ).strip()
    title = str(album.get("title") or "").strip()
    if not provider_album_id or not title:
        return None

    artist = str(album.get("artist") or "").strip()
    url = album.get("url")
    if not url and album.get("browseId"):
        url = f"https://music.youtube.com/browse/{provider_album_id}"

    release_year = album.get("release_year", album.get("year"))
    if release_year in ("", None):
        release_year = None
    elif not isinstance(release_year, int):
        try:
            release_year = int(release_year)
        except (TypeError, ValueError):
            release_year = None

    raw_payload = album.get("raw_payload_json")
    if not isinstance(raw_payload, dict):
        raw_payload = dict(album)

    return {
        "provider": str(album.get("provider") or default_provider),
        "provider_album_id": provider_album_id,
        "title": title,
        "artist": artist,
        "url": str(url).strip() if url else None,
        "release_year": release_year,
        "raw_payload_json": raw_payload,
    }


def _normalize_cached_albums(payload: Dict[str, Any], default_provider: str) -> List[NormalizedAlbum]:
    raw_albums = payload.get("albums") or []
    albums: List[NormalizedAlbum] = []
    for raw_album in raw_albums:
        if not isinstance(raw_album, dict):
            continue
        normalized = _coerce_legacy_album(raw_album, default_provider=default_provider)
        if normalized is not None:
            albums.append(normalized)
    return albums


def load_cached_albums(cache_path: str, *, default_provider: str) -> Optional[List[NormalizedAlbum]]:
    p = Path(cache_path)
    if not p.exists():
        return None
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return _normalize_cached_albums(payload, default_provider=default_provider)


def save_cached_albums(cache_path: str, provider_name: str, albums: List[NormalizedAlbum]) -> None:
    p = Path(cache_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "provider": provider_name,
        "updated_at": int(time.time()),
        "albums": albums,
    }
    with p.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def get_albums_with_cache(
    provider_client: ProviderClient,
    cache_path: str = "data/albums_cache.json",
    refresh: bool = False,
    limit: Optional[int] = None,
) -> List[NormalizedAlbum]:
    if not refresh:
        cached = load_cached_albums(cache_path, default_provider=provider_client.provider_name)
        if cached:
            return cached

    albums = provider_client.list_saved_albums(limit=limit)
    save_cached_albums(cache_path, provider_client.provider_name, albums)
    return albums


def load_cache_payload(cache_path: str) -> Optional[Dict[str, Any]]:
    p = Path(cache_path)
    if not p.exists():
        return None
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)
