import asyncio
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
    _sync_log_fields,
    enqueue_due_jobs,
    enqueue_due_token_refresh_jobs,
    _sync_provider_account,
)


class _MetricProbe:
    def __init__(self) -> None:
        self.calls = []

    def labels(self, **kwargs):
        self.calls.append(("labels", kwargs))
        return self

    def inc(self) -> None:
        self.calls.append(("inc", None))

    def observe(self, value) -> None:
        self.calls.append(("observe", value))


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
    def test_records_sync_success_metrics(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        account = {"id": 55, "provider": "ytmusic", "status": "connected"}
        provider_client = SimpleNamespace(list_saved_albums=lambda limit=None: [{"provider_album_id": "a-1"}])
        sync_total = _MetricProbe()
        sync_failures = _MetricProbe()
        sync_duration = _MetricProbe()

        with patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
             patch.object(worker, "mark_user_provider_sync_started"), \
             patch.object(worker, "build_provider_client", return_value=provider_client), \
             patch.object(worker, "upsert_user_library_albums", return_value=1), \
             patch.object(worker, "mark_user_provider_sync_succeeded"), \
             patch.object(worker, "provider_sync_total", sync_total), \
             patch.object(worker, "provider_sync_failures_total", sync_failures), \
             patch.object(worker, "provider_sync_duration_seconds", sync_duration):
            _sync_provider_account(cfg, account)

        self.assertIn(("labels", {"provider": "ytmusic", "status": "ok"}), sync_total.calls)
        self.assertIn(("inc", None), sync_total.calls)
        self.assertFalse(any(call == ("inc", None) for call in sync_failures.calls))
        self.assertEqual(sync_duration.calls[0][0], "observe")

    def test_marks_account_needs_reauth_on_auth_error(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        account = {"id": 55, "provider": "ytmusic", "status": "connected"}
        provider_client = SimpleNamespace(
            list_saved_albums=lambda limit=None: (_ for _ in ()).throw(RuntimeError("401 unauthorized"))
        )

        with patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
             patch.object(worker, "mark_user_provider_sync_started"), \
             patch.object(worker, "build_provider_client", return_value=provider_client), \
             patch.object(worker, "is_auth_error", return_value=True), \
             patch.object(worker, "mark_user_provider_sync_failed") as mark_failed, \
             patch.object(worker, "mark_user_provider_account_status") as mark_status, \
             patch.object(worker, "provider_sync_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_failures_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_duration_seconds", _MetricProbe()):
            with self.assertRaises(RuntimeError):
                _sync_provider_account(cfg, account)

        mark_failed.assert_called_once_with(55, "401 unauthorized", result_status="auth_error")
        mark_status.assert_called_once_with(55, "needs_reauth")

    def test_marks_empty_library_sync_result_when_provider_returns_no_albums(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        account = {"id": 55, "provider": "ytmusic", "status": "connected"}
        provider_client = SimpleNamespace(list_saved_albums=lambda limit=None: [])

        with patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
             patch.object(worker, "mark_user_provider_sync_started"), \
             patch.object(worker, "build_provider_client", return_value=provider_client), \
             patch.object(worker, "upsert_user_library_albums", return_value=0), \
             patch.object(worker, "mark_user_provider_sync_succeeded") as mark_succeeded, \
             patch.object(worker, "mark_user_provider_account_status") as mark_status, \
             patch.object(worker, "provider_sync_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_failures_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_duration_seconds", _MetricProbe()):
            albums = _sync_provider_account(cfg, account)

        self.assertEqual(albums, [])
        mark_succeeded.assert_called_once_with(55, library_item_count=0, result_status="empty_library")
        mark_status.assert_not_called()

    def test_persists_provider_credentials_after_runtime_refresh(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        account = {
            "id": 55,
            "user_id": 88,
            "provider": "spotify",
            "status": "connected",
            "is_active": True,
        }
        provider_client = SimpleNamespace(
            list_saved_albums=lambda limit=None: [{"provider_album_id": "a-1"}],
            get_updated_credentials=lambda: {
                "access_token": "new-access-token",
                "refresh_token": "secret-refresh-token",
                "token_expires_at": "2026-03-15T13:00:00+00:00",
            },
            get_account_metadata_updates=lambda: {
                "token_expires_at": datetime(2026, 3, 15, 13, 0, tzinfo=timezone.utc),
                "granted_scope": "user-library-read",
                "last_refresh_at": datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
            },
        )

        with patch.object(worker, "get_user_provider_account_credentials", return_value={"refresh_token": "secret"}), \
             patch.object(worker, "mark_user_provider_sync_started"), \
             patch.object(worker, "build_provider_client", return_value=provider_client), \
             patch.object(worker, "upsert_user_provider_account_credentials") as upsert_credentials, \
             patch.object(worker, "upsert_user_library_albums", return_value=1), \
             patch.object(worker, "mark_user_provider_sync_succeeded"), \
             patch.object(worker, "provider_sync_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_failures_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_duration_seconds", _MetricProbe()):
            _sync_provider_account(cfg, account)

        upsert_credentials.assert_called_once()
        self.assertEqual(upsert_credentials.call_args.kwargs["user_id"], 88)
        self.assertEqual(upsert_credentials.call_args.kwargs["provider"], "spotify")
        self.assertEqual(upsert_credentials.call_args.kwargs["granted_scope"], "user-library-read")


class TokenRefreshEnqueueTests(unittest.TestCase):
    def test_enqueues_refresh_jobs_for_accounts_nearing_expiry(self) -> None:
        cfg = SimpleNamespace(worker_id="worker-1")
        account = {
            "id": 55,
            "user_id": 88,
            "provider": "spotify",
            "token_expires_at": datetime(2026, 3, 15, 12, 5, tzinfo=timezone.utc),
        }

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                current = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
                if tz is None:
                    return current.replace(tzinfo=None)
                return current.astimezone(tz)

        with patch.object(worker, "datetime", FixedDateTime), \
             patch.object(worker, "list_provider_accounts_needing_token_refresh", return_value=[account]) as list_accounts, \
             patch.object(worker, "enqueue_job_once", return_value={"id": "job-1", "attempt": 0}) as enqueue_job_once, \
             patch.object(worker, "log_event"):
            enqueued = enqueue_due_token_refresh_jobs(cfg)

        self.assertEqual(enqueued, 1)
        list_accounts.assert_called_once()
        self.assertEqual(
            enqueue_job_once.call_args.kwargs["job_type"],
            worker.JOB_TYPE_REFRESH_PROVIDER_TOKEN,
        )
        self.assertEqual(
            enqueue_job_once.call_args.kwargs["idempotency_key"],
            "refresh-token:55:1773576300",
        )

    def test_duplicate_refresh_enqueue_logs_at_debug(self) -> None:
        cfg = SimpleNamespace(worker_id="worker-1")
        account = {
            "id": 55,
            "user_id": 88,
            "provider": "spotify",
            "token_expires_at": datetime(2026, 3, 15, 12, 5, tzinfo=timezone.utc),
        }

        with patch.object(worker, "list_provider_accounts_needing_token_refresh", return_value=[account]), \
             patch.object(worker, "enqueue_job_once", return_value=None), \
             patch.object(worker, "log_event") as log_event:
            enqueued = enqueue_due_token_refresh_jobs(cfg)

        self.assertEqual(enqueued, 0)
        _, level, event = log_event.call_args.args[:3]
        self.assertEqual(level, worker.logging.DEBUG)
        self.assertEqual(event, "provider_token_refresh_enqueue_skipped_idempotency")

    def test_daily_enqueue_skips_users_without_connected_provider(self) -> None:
        cfg = SimpleNamespace(worker_id="worker-1", due_window_seconds=60)
        users = [
            {
                "user_id": 88,
                "telegram_chat_id": 99,
                "timezone": "UTC",
                "daily_time_local": dt_time(9, 0),
                "active_provider": None,
                "provider_status": None,
            }
        ]

        with patch.object(worker, "list_active_users_with_delivery_context", return_value=users), \
             patch.object(worker, "enqueue_job_once") as enqueue_job_once, \
             patch.object(worker, "log_event"):
            enqueued = enqueue_due_jobs(cfg)

        self.assertEqual(enqueued, 0)
        enqueue_job_once.assert_not_called()

    def test_logs_structured_sync_failure_fields(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        account = {"id": 55, "provider": "ytmusic", "status": "connected"}
        sync_job_id = "8eaf4f97-3292-4dde-b79b-cf9493feebce"
        provider_client = SimpleNamespace(
            list_saved_albums=lambda limit=None: (_ for _ in ()).throw(RuntimeError("429 too many requests"))
        )

        with patch.object(worker, "get_user_provider_account_credentials", return_value={"cookie_blob": "secret"}), \
             patch.object(worker, "mark_user_provider_sync_started"), \
             patch.object(worker, "build_provider_client", return_value=provider_client), \
             patch.object(worker, "is_auth_error", return_value=False), \
             patch.object(worker, "is_rate_limited", return_value=True), \
             patch.object(worker, "mark_user_provider_sync_failed"), \
             patch.object(worker, "log_event") as log_event, \
             patch.object(worker, "provider_sync_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_failures_total", _MetricProbe()), \
             patch.object(worker, "provider_sync_duration_seconds", _MetricProbe()):
            with self.assertRaises(RuntimeError):
                _sync_provider_account(cfg, account, sync_job_id=sync_job_id)

        _, level, event = log_event.call_args_list[-1].args[:3]
        fields = log_event.call_args_list[-1].kwargs
        self.assertEqual(level, worker.logging.ERROR)
        self.assertEqual(event, "provider_sync_failed")
        self.assertEqual(fields["provider"], "ytmusic")
        self.assertEqual(fields["user_provider_account_id"], 55)
        self.assertEqual(fields["sync_job_id"], sync_job_id)
        self.assertEqual(fields["provider_status"], "connected")
        self.assertEqual(fields["sync_result"], "transient_error")
        self.assertTrue(fields["rate_limited"])


class SyncLogFieldsTests(unittest.TestCase):
    def test_defaults_to_account_context(self) -> None:
        fields = _sync_log_fields(account={"id": 3, "provider": "ytmusic", "status": "connected"})

        self.assertEqual(
            fields,
            {
                "provider": "ytmusic",
                "user_provider_account_id": 3,
                "sync_job_id": None,
                "provider_status": "connected",
                "sync_result": None,
                "rate_limited": None,
            },
        )


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

    def test_delivery_failure_releases_reserved_album(self) -> None:
        cfg = SimpleNamespace(library_limit=10)
        job = {
            "id": "job-1",
            "user_id": 7,
            "job_type": worker.JOB_TYPE_DELIVER_NOW,
            "payload": {"telegram_chat_id": 99},
        }
        album = {"provider_album_id": "album-1", "title": "Discovery"}

        async def failing_send(*args, **kwargs):
            raise RuntimeError("telegram send failed")

        with patch.object(worker, "get_active_user_provider_account", return_value={"id": 55, "provider": "ytmusic"}), \
             patch.object(worker, "_get_delivery_albums", return_value=[album]), \
             patch.object(worker, "get_latest_cycle_number", return_value=1), \
             patch.object(worker, "list_cycle_album_ids", return_value=[]), \
             patch.object(worker, "insert_delivery_history", return_value=True), \
             patch.object(worker, "delete_delivery_history") as delete_delivery_history, \
             patch.object(worker.random, "choice", return_value=album), \
             patch.object(worker, "send_album_message", new=failing_send), \
             patch.object(worker, "album_deliveries_total", _MetricProbe()), \
             patch.object(worker, "delivery_attempts_total", _MetricProbe()), \
             patch.object(worker, "delivery_failures_total", _MetricProbe()), \
             patch.object(worker, "delivery_duration_seconds", _MetricProbe()):
            with self.assertRaisesRegex(RuntimeError, "telegram send failed"):
                asyncio.run(worker._execute_delivery_job(bot=SimpleNamespace(), cfg=cfg, job=job))

        delete_delivery_history.assert_called_once_with(user_id=7, cycle_number=1, album_id="album-1")


class JobFailureHandlingTests(unittest.TestCase):
    def test_auth_failures_are_marked_non_retryable(self) -> None:
        cfg = SimpleNamespace(
            worker_id="worker-1",
            claim_batch_size=10,
            retry_backoff_base_seconds=30,
            retry_backoff_max_seconds=1800,
        )
        job = {
            "id": "8eaf4f97-3292-4dde-b79b-cf9493feebce",
            "user_id": 7,
            "job_type": worker.JOB_TYPE_SYNC_LIBRARY,
            "attempt": 0,
            "payload": {"user_provider_account_id": 55, "provider": "spotify"},
        }

        with patch.object(worker, "claim_runnable_jobs", return_value=[job]), \
             patch.object(worker, "_execute_sync_job", side_effect=RuntimeError("401 unauthorized")), \
             patch.object(worker, "is_auth_error", return_value=True), \
             patch.object(worker, "mark_job_failed", return_value={"status": "dead", "attempt": 1, "run_at": None}) as mark_job_failed, \
             patch.object(worker, "log_event"):
            processed = asyncio.run(worker.process_claimed_jobs(bot=SimpleNamespace(), cfg=cfg))

        self.assertEqual(processed, 1)
        self.assertFalse(mark_job_failed.call_args.kwargs["retryable"])


if __name__ == "__main__":
    unittest.main()
