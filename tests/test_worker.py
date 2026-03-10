import os
import unittest
from datetime import datetime, time as dt_time, timezone
from types import SimpleNamespace
from unittest.mock import patch

from tests.support import install_module_stubs

install_module_stubs()

import worker  # noqa: E402
from worker import (  # noqa: E402
    _compute_backoff_seconds,
    _get_delivery_albums,
    _get_env_int,
    _get_env_str,
    _is_due_now,
    _local_date_key,
    _sync_provider_account,
)


class EnvHelperTests(unittest.TestCase):
    def test_get_env_int_uses_default_when_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_get_env_int("WORKER_POLL_SECONDS", 15), 15)

    def test_get_env_int_strips_whitespace(self) -> None:
        with patch.dict(os.environ, {"WORKER_POLL_SECONDS": " 30 "}, clear=True):
            self.assertEqual(_get_env_int("WORKER_POLL_SECONDS", 15), 30)

    def test_get_env_str_uses_default_when_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_get_env_str("WORKER_ID", "worker-default"), "worker-default")

    def test_get_env_str_strips_whitespace(self) -> None:
        with patch.dict(os.environ, {"WORKER_ID": " worker-123 "}, clear=True):
            self.assertEqual(_get_env_str("WORKER_ID", "worker-default"), "worker-123")


class LocalDateKeyTests(unittest.TestCase):
    def test_builds_expected_idempotency_key(self) -> None:
        self.assertEqual(_local_date_key(7, "2026-03-09"), "daily:7:2026-03-09")


class ComputeBackoffSecondsTests(unittest.TestCase):
    def test_negative_attempt_behaves_like_zero(self) -> None:
        self.assertEqual(_compute_backoff_seconds(attempt=-1, base=30, max_seconds=300), 30)

    def test_grows_exponentially(self) -> None:
        self.assertEqual(_compute_backoff_seconds(attempt=0, base=30, max_seconds=300), 30)
        self.assertEqual(_compute_backoff_seconds(attempt=1, base=30, max_seconds=300), 60)
        self.assertEqual(_compute_backoff_seconds(attempt=3, base=30, max_seconds=300), 240)

    def test_caps_at_maximum(self) -> None:
        self.assertEqual(_compute_backoff_seconds(attempt=10, base=30, max_seconds=300), 300)


class IsDueNowTests(unittest.TestCase):
    def test_due_when_scheduled_time_is_inside_window(self) -> None:
        due, local_date = _is_due_now(
            "Europe/Riga",
            dt_time(9, 0),
            datetime(2026, 3, 9, 6, 55, tzinfo=timezone.utc),
            window_seconds=300,
        )

        self.assertTrue(due)
        self.assertEqual(local_date, "2026-03-09")

    def test_not_due_when_scheduled_time_is_outside_window(self) -> None:
        due, _ = _is_due_now(
            "Europe/Riga",
            dt_time(9, 0),
            datetime(2026, 3, 9, 6, 54, 59, tzinfo=timezone.utc),
            window_seconds=300,
        )

        self.assertFalse(due)

    def test_due_window_is_inclusive_at_end_boundary(self) -> None:
        due, _ = _is_due_now(
            "Europe/Riga",
            dt_time(9, 0),
            datetime(2026, 3, 9, 6, 55, tzinfo=timezone.utc),
            window_seconds=300,
        )

        self.assertTrue(due)

    def test_not_due_after_scheduled_time_has_passed(self) -> None:
        due, _ = _is_due_now(
            "Europe/Riga",
            dt_time(9, 0),
            datetime(2026, 3, 9, 7, 0, 1, tzinfo=timezone.utc),
            window_seconds=300,
        )

        self.assertFalse(due)

    def test_returns_local_date_for_user_timezone_not_utc_date(self) -> None:
        due, local_date = _is_due_now(
            "Asia/Tokyo",
            dt_time(9, 0),
            datetime(2026, 3, 9, 23, 55, tzinfo=timezone.utc),
            window_seconds=300,
        )

        self.assertTrue(due)
        self.assertEqual(local_date, "2026-03-10")


class SyncProviderAccountTests(unittest.TestCase):
    def test_marks_account_needs_reauth_on_auth_error(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        account = {"id": 55, "provider": "ytmusic", "status": "active"}
        provider_client = SimpleNamespace(
            list_saved_albums=lambda limit=None: (_ for _ in ()).throw(RuntimeError("401 unauthorized"))
        )

        with patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
             patch.object(worker, "mark_user_provider_sync_started"), \
             patch.object(worker, "build_provider_client", return_value=provider_client), \
             patch.object(worker, "is_auth_error", return_value=True), \
             patch.object(worker, "mark_user_provider_sync_failed") as mark_failed, \
             patch.object(worker, "mark_user_provider_account_status") as mark_status:
            with self.assertRaises(RuntimeError):
                _sync_provider_account(cfg, account)

        mark_failed.assert_called_once()
        mark_status.assert_called_once_with(55, "needs_reauth")


class DeliveryAlbumSelectionTests(unittest.TestCase):
    def test_uses_cached_provider_albums_when_available(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        cached_albums = [{"provider_album_id": "album-1", "title": "Dummy"}]

        with patch.object(worker, "get_active_user_provider_account", return_value={"id": 99, "provider": "ytmusic"}), \
             patch.object(worker, "list_available_user_library_albums", return_value=cached_albums), \
             patch.object(worker, "_sync_provider_account") as sync_provider_account:
            albums = _get_delivery_albums(cfg, user_id=7)

        self.assertEqual(albums, cached_albums)
        sync_provider_account.assert_not_called()

    def test_raises_when_no_provider_account_exists(self) -> None:
        cfg = SimpleNamespace(library_limit=25)

        with patch.object(worker, "get_active_user_provider_account", return_value=None):
            with self.assertRaises(RuntimeError):
                _get_delivery_albums(cfg, user_id=8)


if __name__ == "__main__":
    unittest.main()
