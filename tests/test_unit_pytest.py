from __future__ import annotations

from types import SimpleNamespace
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
from src.providers import YTMusicProviderClient, build_provider_client  # noqa: E402


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
