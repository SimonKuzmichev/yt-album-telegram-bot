from __future__ import annotations

import logging
import os
import json
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Mapping, Optional, Protocol, TypedDict
from urllib.parse import quote

import requests

from src.logging_utils import log_event
from src.metrics import record_oauth_refresh, record_token_refresh_failure


RawPayload = Dict[str, Any]
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_TOKEN_REFRESH_GRACE_SECONDS = 600
logger = logging.getLogger(__name__)


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

    def get_updated_credentials(self) -> Optional[Dict[str, Any]]:
        ...

    def get_account_metadata_updates(self) -> Dict[str, Any]:
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

    def _resolve_auth_value(self) -> Any:
        if self.credentials is None:
            if not self.auth_path:
                raise RuntimeError("YT Music auth is not configured")
            return self.auth_path

        if isinstance(self.credentials, str):
            return self.credentials

        return self._materialize_credentials_file()

    def _materialize_credentials_file(self) -> str:
        fd, tmp_path = tempfile.mkstemp(suffix=".json", prefix="ytmusic-auth-")
        fd_open = True
        try:
            os.chmod(tmp_path, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                fd_open = False
                json.dump(self.credentials, tmp, ensure_ascii=False)
        except Exception:
            if fd_open:
                os.close(fd)
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise
        return tmp_path

    @contextmanager
    def _client_session(self) -> Iterator[Any]:
        auth_value = self._resolve_auth_value()
        materialized_auth_path = auth_value if self.credentials is not None and not isinstance(self.credentials, str) else None

        from ytmusicapi import YTMusic

        try:
            yield YTMusic(auth_value)
        finally:
            if materialized_auth_path:
                try:
                    os.unlink(materialized_auth_path)
                except FileNotFoundError:
                    pass

    def validate_credentials(self) -> None:
        with self._client_session() as client:
            client.get_library_albums(limit=1)

    def list_saved_albums(self, limit: Optional[int] = None) -> List[NormalizedAlbum]:
        with self._client_session() as client:
            raw_albums = client.get_library_albums(limit=limit)
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

    def get_updated_credentials(self) -> Optional[Dict[str, Any]]:
        return None

    def get_account_metadata_updates(self) -> Dict[str, Any]:
        return {}


class SpotifyProviderClient:
    provider_name = "spotify"

    def __init__(self, credentials: Optional[Any] = None) -> None:
        if not isinstance(credentials, dict):
            raise RuntimeError("Spotify auth is not configured")
        self.credentials: Dict[str, Any] = dict(credentials)
        self._account_metadata_updates: Dict[str, Any] = {}

    def _get_client_credentials(self) -> tuple[str, str]:
        client_id = str(os.getenv("SPOTIFY_CLIENT_ID", "")).strip()
        client_secret = str(os.getenv("SPOTIFY_CLIENT_SECRET", "")).strip()
        if not client_id or not client_secret:
            raise RuntimeError("Spotify client credentials are not configured")
        return client_id, client_secret

    def _parse_expiry(self) -> Optional[datetime]:
        raw_value = self.credentials.get("token_expires_at")
        if isinstance(raw_value, datetime):
            return raw_value if raw_value.tzinfo is not None else raw_value.replace(tzinfo=timezone.utc)
        if isinstance(raw_value, str) and raw_value.strip():
            parsed = datetime.fromisoformat(raw_value.strip())
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        return None

    def _access_token_is_fresh(self) -> bool:
        access_token = _normalize_text(self.credentials.get("access_token"))
        expires_at = self._parse_expiry()
        if not access_token or expires_at is None:
            return False
        return expires_at > datetime.now(timezone.utc) + timedelta(seconds=SPOTIFY_TOKEN_REFRESH_GRACE_SECONDS)

    def _refresh_access_token(self) -> None:
        refresh_token = _normalize_text(self.credentials.get("refresh_token"))
        if not refresh_token:
            raise RuntimeError("Spotify refresh token is not configured")

        try:
            client_id, client_secret = self._get_client_credentials()
            response = requests.post(
                SPOTIFY_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                auth=(client_id, client_secret),
                timeout=15,
            )
            try:
                payload = response.json()
            except ValueError as exc:
                raise RuntimeError(f"Spotify token refresh returned invalid JSON (status={response.status_code})") from exc

            if response.status_code >= 400:
                error_code = payload.get("error") or "unknown_error"
                error_description = payload.get("error_description") or "token refresh failed"
                raise RuntimeError(f"Spotify token refresh failed: {error_code}: {error_description}")

            access_token = _normalize_text(payload.get("access_token"))
            token_type = _normalize_text(payload.get("token_type")) or "Bearer"
            scope = _normalize_text(payload.get("scope")) or _normalize_text(self.credentials.get("granted_scope"))
            returned_refresh_token = _normalize_text(payload.get("refresh_token"))
            expires_in = payload.get("expires_in")
            if not access_token or expires_in is None:
                raise RuntimeError("Spotify token refresh response was missing required fields")
            try:
                expires_in_seconds = int(expires_in)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Spotify token refresh returned an invalid expires_in value") from exc

            refreshed_at = datetime.now(timezone.utc)
            expires_at = refreshed_at + timedelta(seconds=expires_in_seconds)
            self.credentials["access_token"] = access_token
            self.credentials["token_type"] = token_type
            self.credentials["granted_scope"] = scope
            self.credentials["token_expires_at"] = expires_at.isoformat()
            if returned_refresh_token:
                self.credentials["refresh_token"] = returned_refresh_token
            self._account_metadata_updates = {
                "token_expires_at": expires_at,
                "granted_scope": scope or None,
                "last_refresh_at": refreshed_at,
            }
            record_oauth_refresh(self.provider_name, "success")
            log_event(
                logger,
                logging.INFO,
                "oauth_refresh_succeeded",
                message=f"oauth_refresh_succeeded provider=spotify token_expires_at={expires_at.isoformat()}",
                provider=self.provider_name,
            )
        except Exception:
            record_oauth_refresh(self.provider_name, "failed")
            record_token_refresh_failure(self.provider_name)
            log_event(
                logger,
                logging.ERROR,
                "oauth_refresh_failed",
                exc_info=True,
                message="oauth_refresh_failed provider=spotify",
                provider=self.provider_name,
            )
            raise

    def _get_access_token(self) -> str:
        if not self._access_token_is_fresh():
            self._refresh_access_token()
        access_token = _normalize_text(self.credentials.get("access_token"))
        if not access_token:
            raise RuntimeError("Spotify access token is not configured")
        return access_token

    def _spotify_get(self, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        access_token = self._get_access_token()
        response = requests.get(
            f"{SPOTIFY_API_BASE_URL}{path}",
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Spotify API returned invalid JSON (status={response.status_code})") from exc
        if response.status_code >= 400:
            error_payload = payload.get("error") if isinstance(payload, dict) else None
            if isinstance(error_payload, dict):
                status = error_payload.get("status") or response.status_code
                message = error_payload.get("message") or "Spotify API request failed"
                raise RuntimeError(f"Spotify API request failed: {status}: {message}")
            raise RuntimeError(f"Spotify API request failed with status {response.status_code}")
        if not isinstance(payload, dict):
            raise RuntimeError("Spotify API returned an unexpected payload")
        return payload

    def validate_credentials(self) -> None:
        self._spotify_get("/me")

    def list_saved_albums(self, limit: Optional[int] = None) -> List[NormalizedAlbum]:
        albums: List[NormalizedAlbum] = []
        offset = 0
        remaining = limit if limit is not None else None
        while True:
            page_limit = 50 if remaining is None else max(min(remaining, 50), 1)
            payload = self._spotify_get("/me/albums", params={"limit": page_limit, "offset": offset})
            items = payload.get("items") or []
            for item in items:
                normalized = self.normalize_album(item)
                if normalized is not None:
                    albums.append(normalized)
                    if remaining is not None and len(albums) >= limit:
                        return albums
            if not payload.get("next") or not items:
                return albums
            offset += len(items)
            if remaining is not None:
                remaining = limit - len(albums)
                if remaining <= 0:
                    return albums

    def normalize_album(self, raw_album: Mapping[str, Any]) -> Optional[NormalizedAlbum]:
        album = raw_album.get("album") if isinstance(raw_album.get("album"), Mapping) else raw_album
        if not isinstance(album, Mapping):
            return None

        provider_album_id = _normalize_text(album.get("id"))
        title = _normalize_text(album.get("name"))
        if not provider_album_id or not title:
            return None

        artists = album.get("artists") or []
        artist = ", ".join(
            _normalize_text(artist_row.get("name"))
            for artist_row in artists
            if isinstance(artist_row, Mapping) and _normalize_text(artist_row.get("name"))
        )
        images = album.get("images") or []
        external_urls = album.get("external_urls") if isinstance(album.get("external_urls"), Mapping) else {}
        release_date = _normalize_text(album.get("release_date"))

        return {
            "provider": self.provider_name,
            "provider_album_id": provider_album_id,
            "title": title,
            "artist": artist,
            "url": _normalize_text(external_urls.get("spotify")) or None,
            "release_year": _coerce_release_year(release_date[:4] if release_date else None),
            "raw_payload_json": {
                "added_at": raw_album.get("added_at"),
                "album": dict(album),
                "images_count": len(images),
            },
        }

    def get_updated_credentials(self) -> Optional[Dict[str, Any]]:
        if not self._account_metadata_updates:
            return None
        return dict(self.credentials)

    def get_account_metadata_updates(self) -> Dict[str, Any]:
        return dict(self._account_metadata_updates)


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
        return SpotifyProviderClient(credentials=credentials)
    raise RuntimeError(f"Unsupported provider: {provider_name}")
