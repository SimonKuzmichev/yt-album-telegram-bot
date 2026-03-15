from __future__ import annotations

import json
import os
import stat
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet

from tests.support import install_module_stubs

install_module_stubs()

import worker  # noqa: E402
from src.credentials_encryption import (  # noqa: E402
    ENVELOPE_PREFIX,
    MASTER_KEY_ENV_VAR,
    decrypt_for_runtime,
    encrypt_for_storage,
    redact_sensitive_mapping,
)
from src.metrics import classify_error  # noqa: E402
from src.providers import SpotifyProviderClient, YTMusicProviderClient, build_provider_client  # noqa: E402


class _MetricProbe:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []

    def labels(self, **kwargs):
        self.calls.append(("labels", kwargs))
        return self

    def inc(self) -> None:
        self.calls.append(("inc", None))

    def observe(self, value) -> None:
        self.calls.append(("observe", value))


def test_credentials_encrypt_and_decrypt_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "access_token": "secret-access-token",
        "refresh_token": "secret-refresh-token",
        "nested": {"cookie_blob": "value", "display_name": "friend"},
    }
    monkeypatch.setenv(MASTER_KEY_ENV_VAR, Fernet.generate_key().decode("utf-8"))

    encrypted = encrypt_for_storage(payload)
    decrypted = decrypt_for_runtime(encrypted)

    assert encrypted.startswith(ENVELOPE_PREFIX)
    assert "secret-access-token" not in encrypted
    assert decrypted == payload


def test_credentials_decrypt_rejects_wrong_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(MASTER_KEY_ENV_VAR, Fernet.generate_key().decode("utf-8"))
    encrypted = encrypt_for_storage({"token": "abc"})

    monkeypatch.setenv(MASTER_KEY_ENV_VAR, Fernet.generate_key().decode("utf-8"))

    with pytest.raises(RuntimeError, match="could not be decrypted"):
        decrypt_for_runtime(encrypted)


def test_redact_sensitive_mapping_masks_nested_values() -> None:
    redacted = redact_sensitive_mapping(
        {
            "access_token": "secret",
            "display_name": "friend",
            "nested": {
                "authorization_header": "Bearer token",
                "country": "US",
            },
        }
    )

    assert redacted == {
        "access_token": "[REDACTED]",
        "display_name": "friend",
        "nested": {
            "authorization_header": "[REDACTED]",
            "country": "US",
        },
    }


def test_build_provider_client_normalizes_provider_name() -> None:
    client = build_provider_client("  yTmUsIc  ", credentials={"cookie_blob": "secret"})

    assert isinstance(client, YTMusicProviderClient)


def test_build_provider_client_supports_spotify() -> None:
    client = build_provider_client(
        "spotify",
        credentials={
            "access_token": "secret-access-token",
            "refresh_token": "secret-refresh-token",
            "token_expires_at": "2026-03-15T13:00:00+00:00",
        },
    )

    assert isinstance(client, SpotifyProviderClient)


def test_ytmusic_provider_materialized_credentials_file_is_private_and_removed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class FakeYTMusic:
        def __init__(self, auth_value: str) -> None:
            observed["path"] = auth_value
            observed["exists_during_init"] = os.path.exists(auth_value)
            observed["mode"] = stat.S_IMODE(os.stat(auth_value).st_mode)
            with open(auth_value, encoding="utf-8") as fh:
                observed["payload"] = json.load(fh)

        def get_library_albums(self, *, limit: int | None = None):
            assert limit == 5
            return [{"browseId": "album-1", "title": "Discovery", "artists": [{"name": "Daft Punk"}]}]

    fake_module = ModuleType("ytmusicapi")
    fake_module.YTMusic = FakeYTMusic
    monkeypatch.setitem(sys.modules, "ytmusicapi", fake_module)

    client = YTMusicProviderClient(credentials={"cookie_blob": "secret"})

    albums = client.list_saved_albums(limit=5)

    assert [album["provider_album_id"] for album in albums] == ["album-1"]
    assert observed["exists_during_init"] is True
    assert observed["mode"] == 0o600
    assert observed["payload"] == {"cookie_blob": "secret"}
    assert not os.path.exists(str(observed["path"]))


def test_ytmusic_provider_removes_materialized_credentials_file_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class FakeYTMusic:
        def __init__(self, auth_value: str) -> None:
            observed["path"] = auth_value

        def get_library_albums(self, *, limit: int | None = None):
            raise RuntimeError("boom")

    fake_module = ModuleType("ytmusicapi")
    fake_module.YTMusic = FakeYTMusic
    monkeypatch.setitem(sys.modules, "ytmusicapi", fake_module)

    client = YTMusicProviderClient(credentials={"cookie_blob": "secret"})

    with pytest.raises(RuntimeError, match="boom"):
        client.validate_credentials()

    assert not os.path.exists(str(observed["path"]))


def test_spotify_provider_lists_saved_albums(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, object] = {}

    class FakeResponse:
        def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code

        def json(self) -> dict[str, object]:
            return self._payload

    def fake_get(url: str, *, params=None, headers=None, timeout=None):
        observed["url"] = url
        observed["params"] = params
        observed["headers"] = headers
        observed["timeout"] = timeout
        return FakeResponse(
            {
                "items": [
                    {
                        "added_at": "2026-03-15T12:00:00Z",
                        "album": {
                            "id": "album-1",
                            "name": "Discovery",
                            "artists": [{"name": "Daft Punk"}],
                            "release_date": "2001-03-12",
                            "external_urls": {"spotify": "https://open.spotify.com/album/album-1"},
                            "images": [{"url": "https://image.test/1.jpg"}],
                        },
                    }
                ],
                "next": None,
            }
        )

    monkeypatch.setattr("src.providers.requests.get", fake_get)
    client = SpotifyProviderClient(
        credentials={
            "access_token": "secret-access-token",
            "refresh_token": "secret-refresh-token",
            "token_expires_at": "2099-03-15T13:00:00+00:00",
        }
    )

    albums = client.list_saved_albums(limit=5)

    assert [album["provider_album_id"] for album in albums] == ["album-1"]
    assert albums[0]["artist"] == "Daft Punk"
    assert observed["params"] == {"limit": 5, "offset": 0}
    assert observed["headers"] == {"Authorization": "Bearer secret-access-token"}


def test_spotify_provider_refreshes_expired_access_token(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, object] = {}

    class FakeResponse:
        def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code

        def json(self) -> dict[str, object]:
            return self._payload

    def fake_post(url: str, *, data=None, auth=None, timeout=None):
        observed["refresh_url"] = url
        observed["refresh_data"] = data
        observed["refresh_auth"] = auth
        return FakeResponse(
            {
                "access_token": "new-access-token",
                "token_type": "Bearer",
                "scope": "user-library-read",
                "expires_in": 3600,
            }
        )

    def fake_get(url: str, *, params=None, headers=None, timeout=None):
        observed["headers"] = headers
        return FakeResponse({"id": "spotify-user"})

    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "client-123")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "secret-456")
    monkeypatch.setattr("src.providers.requests.post", fake_post)
    monkeypatch.setattr("src.providers.requests.get", fake_get)
    client = SpotifyProviderClient(
        credentials={
            "access_token": "old-access-token",
            "refresh_token": "secret-refresh-token",
            "token_expires_at": "2020-03-15T13:00:00+00:00",
        }
    )

    client.validate_credentials()
    updated_credentials = client.get_updated_credentials()
    metadata_updates = client.get_account_metadata_updates()

    assert observed["refresh_auth"] == ("client-123", "secret-456")
    assert observed["refresh_data"] == {"grant_type": "refresh_token", "refresh_token": "secret-refresh-token"}
    assert observed["headers"] == {"Authorization": "Bearer new-access-token"}
    assert updated_credentials is not None
    assert updated_credentials["access_token"] == "new-access-token"
    assert updated_credentials["refresh_token"] == "secret-refresh-token"
    assert metadata_updates["granted_scope"] == "user-library-read"
    assert metadata_updates["token_expires_at"].tzinfo is not None


def test_spotify_provider_records_refresh_failure_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status_code = 400

        def json(self) -> dict[str, object]:
            return {"error": "invalid_grant", "error_description": "refresh token revoked"}

    recorded: list[str] = []

    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "client-123")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "secret-456")
    monkeypatch.setattr("src.providers.requests.post", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr("src.providers.record_token_refresh_failure", lambda provider: recorded.append(provider))
    client = SpotifyProviderClient(
        credentials={
            "access_token": "old-access-token",
            "refresh_token": "secret-refresh-token",
            "token_expires_at": "2020-03-15T13:00:00+00:00",
        }
    )

    with pytest.raises(RuntimeError, match="invalid_grant"):
        client.validate_credentials()

    assert recorded == ["spotify"]


def test_sync_provider_account_recovers_needs_reauth_to_connected() -> None:
    cfg = SimpleNamespace(library_limit=10)
    account = {"id": 55, "provider": "ytmusic", "status": "needs_reauth"}
    provider_client = SimpleNamespace(list_saved_albums=lambda limit=None: [{"provider_album_id": "a-1"}])

    with patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
         patch.object(worker, "mark_user_provider_sync_started"), \
         patch.object(worker, "build_provider_client", return_value=provider_client), \
         patch.object(worker, "upsert_user_library_albums", return_value=1), \
         patch.object(worker, "mark_user_provider_sync_succeeded"), \
         patch.object(worker, "mark_user_provider_account_status") as mark_status, \
         patch.object(worker, "provider_sync_total", _MetricProbe()), \
         patch.object(worker, "provider_sync_failures_total", _MetricProbe()), \
         patch.object(worker, "provider_sync_duration_seconds", _MetricProbe()):
        albums = worker._sync_provider_account(cfg, account)

    assert albums == [{"provider_album_id": "a-1"}]
    mark_status.assert_called_once_with(55, "connected")


def test_sync_provider_account_classifies_rate_limit_failures() -> None:
    cfg = SimpleNamespace(library_limit=10)
    account = {"id": 55, "provider": "ytmusic", "status": "connected"}
    provider_client = SimpleNamespace(
        list_saved_albums=lambda limit=None: (_ for _ in ()).throw(RuntimeError("429 too many requests"))
    )
    sync_total = _MetricProbe()
    sync_failures = _MetricProbe()

    with patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
         patch.object(worker, "mark_user_provider_sync_started"), \
         patch.object(worker, "build_provider_client", return_value=provider_client), \
         patch.object(worker, "is_auth_error", return_value=False), \
         patch.object(worker, "is_rate_limited", return_value=True), \
         patch.object(worker, "mark_user_provider_sync_failed"), \
         patch.object(worker, "provider_sync_total", sync_total), \
         patch.object(worker, "provider_sync_failures_total", sync_failures), \
         patch.object(worker, "provider_sync_duration_seconds", _MetricProbe()):
        with pytest.raises(RuntimeError, match="429 too many requests"):
            worker._sync_provider_account(cfg, account, sync_job_id=uuid4())

    assert ("labels", {"provider": "ytmusic", "status": "transient_error"}) in sync_total.calls
    assert ("labels", {"provider": "ytmusic", "error_type": "rate_limited"}) in sync_failures.calls


def test_revalidate_provider_job_marks_account_needs_reauth_on_auth_error() -> None:
    cfg = SimpleNamespace()
    job = {"id": uuid4(), "payload": {"user_provider_account_id": 55}}
    provider_client = SimpleNamespace(
        validate_credentials=lambda: (_ for _ in ()).throw(RuntimeError("401 unauthorized"))
    )

    with patch.object(worker, "get_user_provider_account_by_id", return_value={"id": 55, "provider": "ytmusic", "status": "connected"}), \
         patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
         patch.object(worker, "build_provider_client", return_value=provider_client), \
         patch.object(worker, "is_auth_error", return_value=True), \
         patch.object(worker, "is_rate_limited", return_value=False), \
         patch.object(worker, "mark_user_provider_account_status") as mark_status:
        with pytest.raises(RuntimeError, match="401 unauthorized"):
            worker._execute_revalidate_provider_job(cfg, job)

    mark_status.assert_called_once_with(55, "needs_reauth")


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (RuntimeError("boom"), "runtime_error"),
        (ValueError("bad value"), "value_error"),
        (OSError("disk full"), "o_s_error"),
    ],
)
def test_classify_error_normalizes_exception_names(exc: Exception, expected: str) -> None:
    assert classify_error(exc) == expected
