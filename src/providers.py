from __future__ import annotations

import json
import tempfile
from typing import Any, Dict, List, Mapping, Optional, Protocol, TypedDict
from urllib.parse import quote


RawPayload = Dict[str, Any]


class NormalizedAlbum(TypedDict):
    provider: str
    provider_album_id: str
    title: str
    artist: str
    url: Optional[str]
    release_year: Optional[int]
    raw_payload_json: RawPayload


class ProviderClient(Protocol):
    provider_name: str

    def validate_credentials(self) -> None:
        ...

    def list_saved_albums(self, limit: Optional[int] = None) -> List[NormalizedAlbum]:
        ...

    def normalize_album(self, raw_album: Mapping[str, Any]) -> Optional[NormalizedAlbum]:
        ...


def _coerce_release_year(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


class YTMusicProviderClient:
    provider_name = "ytmusic"

    def __init__(self, auth_path: Optional[str] = None, credentials: Optional[Any] = None) -> None:
        self.auth_path = auth_path
        self.credentials = credentials
        self._materialized_auth_path: Optional[str] = None

    def _resolve_auth_value(self) -> Any:
        if self.credentials is None:
            if not self.auth_path:
                raise RuntimeError("YT Music auth is not configured")
            return self.auth_path

        if isinstance(self.credentials, str):
            return self.credentials

        if self._materialized_auth_path is None:
            # ytmusicapi expects a file-like auth input in some setups, so for
            # DB-backed credential blobs we materialize one private temp file
            # per worker process and reuse its path for subsequent client builds.
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                prefix="ytmusic-auth-",
                delete=False,
            ) as tmp:
                json.dump(self.credentials, tmp, ensure_ascii=False)
                self._materialized_auth_path = tmp.name
        return self._materialized_auth_path

    def _create_client(self):
        from ytmusicapi import YTMusic

        return YTMusic(self._resolve_auth_value())

    def validate_credentials(self) -> None:
        self._create_client().get_library_albums(limit=1)

    def list_saved_albums(self, limit: Optional[int] = None) -> List[NormalizedAlbum]:
        raw_albums = self._create_client().get_library_albums(limit=limit)
        albums: List[NormalizedAlbum] = []
        for raw_album in raw_albums:
            normalized = self.normalize_album(raw_album)
            if normalized is not None:
                albums.append(normalized)
        return albums

    def normalize_album(self, raw_album: Mapping[str, Any]) -> Optional[NormalizedAlbum]:
        artists = raw_album.get("artists") or []
        artist = ", ".join(
            _normalize_text(artist_row.get("name"))
            for artist_row in artists
            if _normalize_text(artist_row.get("name"))
        )
        if not artist:
            artist = _normalize_text(raw_album.get("artist"))

        provider_album_id = _normalize_text(
            raw_album.get("browseId") or raw_album.get("audioPlaylistId") or raw_album.get("playlistId")
        )
        title = _normalize_text(raw_album.get("title"))
        if not provider_album_id or not title:
            return None

        return {
            "provider": self.provider_name,
            "provider_album_id": provider_album_id,
            "title": title,
            "artist": artist,
            "url": f"https://music.youtube.com/browse/{quote(provider_album_id)}",
            "release_year": _coerce_release_year(raw_album.get("year")),
            "raw_payload_json": dict(raw_album),
        }


class SpotifyProviderClient:
    provider_name = "spotify"

    def validate_credentials(self) -> None:
        raise NotImplementedError("Spotify provider is not implemented yet")

    def list_saved_albums(self, limit: Optional[int] = None) -> List[NormalizedAlbum]:
        raise NotImplementedError("Spotify provider is not implemented yet")

    def normalize_album(self, raw_album: Mapping[str, Any]) -> Optional[NormalizedAlbum]:
        raise NotImplementedError("Spotify provider is not implemented yet")


def build_provider_client(
    provider_name: str,
    *,
    auth_path: Optional[str] = None,
    credentials: Optional[Any] = None,
) -> ProviderClient:
    normalized_name = provider_name.strip().lower()
    if normalized_name == "ytmusic":
        resolved_auth_path = auth_path.strip() if isinstance(auth_path, str) and auth_path.strip() else None
        return YTMusicProviderClient(auth_path=resolved_auth_path, credentials=credentials)
    if normalized_name == "spotify":
        return SpotifyProviderClient()
    raise RuntimeError(f"Unsupported provider: {provider_name}")
