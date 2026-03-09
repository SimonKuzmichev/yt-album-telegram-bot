import os
import unittest
from datetime import datetime, time as dt_time, timezone
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID, NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from tests.support import install_module_stubs

install_module_stubs()

from bot import (
    _fmt_ts,
    _is_admin_override_chat,
    get_optional_env_int,
    get_request_id,
    parse_daily_time,
    parse_time_hhmm,
)


class ParseDailyTimeTests(unittest.TestCase):
    def test_parses_hhmm(self) -> None:
        self.assertEqual(parse_daily_time("09:30"), dt_time(9, 30))

    def test_strips_whitespace(self) -> None:
        self.assertEqual(parse_daily_time(" 18:05 "), dt_time(18, 5))

    def test_rejects_missing_colon(self) -> None:
        with self.assertRaises(ValueError):
            parse_daily_time("0930")


class ParseTimeHhmmTests(unittest.TestCase):
    def test_parses_strict_hhmm(self) -> None:
        self.assertEqual(parse_time_hhmm("07:30"), dt_time(7, 30))

    def test_strips_surrounding_whitespace(self) -> None:
        self.assertEqual(parse_time_hhmm(" 23:05 "), dt_time(23, 5))

    def test_rejects_non_zero_padded_time(self) -> None:
        with self.assertRaises(ValueError):
            parse_time_hhmm("7:30")

    def test_rejects_seconds(self) -> None:
        with self.assertRaises(ValueError):
            parse_time_hhmm("07:30:00")


class GetOptionalEnvIntTests(unittest.TestCase):
    def test_returns_none_when_variable_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(get_optional_env_int("MISSING"))

    def test_returns_none_for_blank_string(self) -> None:
        with patch.dict(os.environ, {"VALUE": "   "}, clear=True):
            self.assertIsNone(get_optional_env_int("VALUE"))

    def test_parses_integer_value(self) -> None:
        with patch.dict(os.environ, {"VALUE": "42"}, clear=True):
            self.assertEqual(get_optional_env_int("VALUE"), 42)


class GetRequestIdTests(unittest.TestCase):
    def test_uses_message_identity_when_available(self) -> None:
        update = SimpleNamespace(
            effective_message=SimpleNamespace(message_id=42),
            effective_chat=SimpleNamespace(id=9001),
            update_id=777,
        )

        with patch.dict(os.environ, {"ENVIRONMENT": "test"}, clear=False):
            request_id = get_request_id(update)

        expected = str(uuid5(NAMESPACE_URL, "env:test:telegram-msg:9001:42"))
        self.assertEqual(request_id, expected)

    def test_falls_back_to_update_id_when_message_missing(self) -> None:
        update = SimpleNamespace(
            effective_message=None,
            effective_chat=None,
            update_id=777,
        )

        with patch.dict(os.environ, {"ENVIRONMENT": "test"}, clear=False):
            request_id = get_request_id(update)

        expected = str(uuid5(NAMESPACE_URL, "env:test:telegram-update:777"))
        self.assertEqual(request_id, expected)

    def test_returns_random_uuid_when_no_stable_identity_exists(self) -> None:
        update = SimpleNamespace(
            effective_message=None,
            effective_chat=None,
            update_id=None,
        )

        first = get_request_id(update)
        second = get_request_id(update)

        self.assertNotEqual(first, second)
        self.assertEqual(UUID(first).version, 4)
        self.assertEqual(UUID(second).version, 4)


class IsAdminOverrideChatTests(unittest.TestCase):
    def test_true_when_chat_matches_override(self) -> None:
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=123))
        self.assertTrue(_is_admin_override_chat(update, 123))

    def test_false_when_override_not_configured(self) -> None:
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=123))
        self.assertFalse(_is_admin_override_chat(update, None))

    def test_false_when_chat_differs(self) -> None:
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=123))
        self.assertFalse(_is_admin_override_chat(update, 456))


class FmtTsTests(unittest.TestCase):
    def test_returns_na_for_missing_timestamp(self) -> None:
        self.assertEqual(_fmt_ts(None, ZoneInfo("UTC")), "n/a")

    def test_formats_timestamp_in_target_timezone(self) -> None:
        ts = datetime(2026, 3, 9, 6, 0, tzinfo=timezone.utc)
        formatted = _fmt_ts(ts, ZoneInfo("Europe/Riga"))
        self.assertEqual(formatted, "2026-03-09 08:00:00 EET")


if __name__ == "__main__":
    unittest.main()
