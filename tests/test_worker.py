import os
import unittest
from datetime import datetime, time as dt_time, timezone
from unittest.mock import patch

from tests.support import install_module_stubs

install_module_stubs()

from worker import (
    _compute_backoff_seconds,
    _get_env_int,
    _get_env_str,
    _is_due_now,
    _local_date_key,
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


if __name__ == "__main__":
    unittest.main()
