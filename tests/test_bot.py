import os
import unittest
from datetime import datetime, time as dt_time, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID, NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from tests.support import install_module_stubs

install_module_stubs()

from bot import (  # noqa: E402
    COMMAND_LOCK_TTLS_SECONDS,
    _fmt_ts,
    _is_admin_override_chat,
    acquire_command_lock,
    enforce_command_lock,
    get_command_lock_key,
    get_env_str,
    get_optional_env_int,
    get_request_id,
    parse_time_hhmm,
    resolve_app_timezone,
)


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


class GetEnvStrTests(unittest.TestCase):
    def test_returns_default_when_variable_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_env_str("MISSING", "fallback"), "fallback")

    def test_returns_default_for_blank_string(self) -> None:
        with patch.dict(os.environ, {"VALUE": "   "}, clear=True):
            self.assertEqual(get_env_str("VALUE", "fallback"), "fallback")

    def test_returns_stripped_value(self) -> None:
        with patch.dict(os.environ, {"VALUE": " redis://localhost:6379/0 "}, clear=True):
            self.assertEqual(get_env_str("VALUE", "fallback"), "redis://localhost:6379/0")


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


class CommandLockTests(unittest.IsolatedAsyncioTestCase):
    def test_get_command_lock_key_uses_action_and_user_id(self) -> None:
        self.assertEqual(get_command_lock_key("refresh", 42), "command-lock:refresh:42")

    async def test_acquire_command_lock_sets_expected_ttl(self) -> None:
        redis_client = SimpleNamespace(set=AsyncMock(return_value=True))
        context = SimpleNamespace(application=SimpleNamespace(bot_data={"redis": redis_client}))

        acquired = await acquire_command_lock(context, "refresh", 42)

        self.assertTrue(acquired)
        redis_client.set.assert_awaited_once_with(
            "command-lock:refresh:42",
            "1",
            ex=COMMAND_LOCK_TTLS_SECONDS["refresh"],
            nx=True,
        )

    async def test_enforce_command_lock_replies_when_lock_exists(self) -> None:
        redis_client = SimpleNamespace(set=AsyncMock(return_value=False))
        context = SimpleNamespace(application=SimpleNamespace(bot_data={"redis": redis_client}))
        update = SimpleNamespace(
            effective_chat=SimpleNamespace(id=99),
            callback_query=None,
            message=SimpleNamespace(reply_text=AsyncMock()),
            effective_user=SimpleNamespace(id=42),
        )

        allowed = await enforce_command_lock(update, context, "now", 42)

        self.assertFalse(allowed)
        update.message.reply_text.assert_awaited_once()


class FmtTsTests(unittest.TestCase):
    def test_returns_na_for_missing_timestamp(self) -> None:
        self.assertEqual(_fmt_ts(None, ZoneInfo("UTC")), "n/a")

    def test_formats_unix_epoch_timestamp_in_target_timezone(self) -> None:
        formatted = _fmt_ts(1773036000, ZoneInfo("Europe/Riga"))
        self.assertEqual(formatted, "2026-03-09 08:00:00 EET")

    def test_formats_timestamp_in_target_timezone(self) -> None:
        ts = datetime(2026, 3, 9, 6, 0, tzinfo=timezone.utc)
        formatted = _fmt_ts(ts, ZoneInfo("Europe/Riga"))
        self.assertEqual(formatted, "2026-03-09 08:00:00 EET")


class ResolveAppTimezoneTests(unittest.TestCase):
    def test_uses_admin_timezone_from_db_when_present(self) -> None:
        with patch("bot.get_user_timezone_by_chat_id", return_value="Europe/Riga"):
            resolved = resolve_app_timezone(123, "UTC")

        self.assertEqual(resolved.key, "Europe/Riga")

    def test_falls_back_to_default_when_admin_not_in_db(self) -> None:
        with patch("bot.get_user_timezone_by_chat_id", return_value=None):
            resolved = resolve_app_timezone(123, "UTC")

        self.assertEqual(resolved.key, "UTC")

    def test_falls_back_to_default_when_admin_override_missing(self) -> None:
        with patch("bot.get_user_timezone_by_chat_id") as get_timezone:
            resolved = resolve_app_timezone(None, "Europe/Riga")

        get_timezone.assert_not_called()
        self.assertEqual(resolved.key, "Europe/Riga")


if __name__ == "__main__":
    unittest.main()
