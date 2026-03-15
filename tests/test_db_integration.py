import os
import sys
import unittest
import importlib
from datetime import datetime, time as dt_time, timedelta, timezone
from types import ModuleType
from unittest.mock import patch
from uuid import uuid4

from cryptography.fernet import Fernet

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:
    psycopg = None
    dict_row = None


def _install_worker_import_stubs() -> None:
    if "dotenv" not in sys.modules:
        dotenv = ModuleType("dotenv")
        dotenv.load_dotenv = lambda: None
        sys.modules["dotenv"] = dotenv

    if "telegram" not in sys.modules:
        telegram = ModuleType("telegram")
        telegram.Bot = type("Bot", (), {})
        sys.modules["telegram"] = telegram


db = None
worker = None
IMPORT_ERROR = None
if psycopg is not None:
    try:
        _install_worker_import_stubs()
        # Unit tests install lightweight stubs into sys.modules. Remove those so
        # integration tests always import the real database and worker modules.
        sys.modules.pop("src.db", None)
        sys.modules.pop("worker", None)
        db = importlib.import_module("src.db")
        worker = importlib.import_module("worker")
    except ModuleNotFoundError as exc:
        IMPORT_ERROR = exc


class PostgresIntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if psycopg is None:
            raise unittest.SkipTest("psycopg is not installed")
        if IMPORT_ERROR is not None:
            raise unittest.SkipTest(f"missing runtime dependency for integration tests: {IMPORT_ERROR.name}")
        if db is None or worker is None:
            raise unittest.SkipTest("integration modules could not be imported")

        cls.test_database_url = os.getenv("TEST_DATABASE_URL", "").strip()
        if not cls.test_database_url:
            raise unittest.SkipTest("TEST_DATABASE_URL is not set")

        with psycopg.connect(cls.test_database_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::INT AS cnt
                    FROM information_schema.tables
                    WHERE table_schema = 'app'
                      AND table_name = 'users'
                    """
                )
                row = cur.fetchone()
        if not row or row.get("cnt") != 1:
            raise unittest.SkipTest("app schema is not present in TEST_DATABASE_URL")

    def setUp(self) -> None:
        self.env_patch = patch.dict(
            os.environ,
            {
                "DATABASE_URL": self.test_database_url,
                "CREDENTIALS_MASTER_KEY": Fernet.generate_key().decode("utf-8"),
            },
            clear=False,
        )
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        self.reset_database()

    def reset_database(self) -> None:
        with psycopg.connect(self.test_database_url, row_factory=dict_row) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        TRUNCATE TABLE
                            app.oauth_sessions,
                            app.user_library_albums,
                            app.user_provider_sync_state,
                            app.user_provider_accounts,
                            app.delivery_history,
                            app.idempotency_keys,
                            app.jobs,
                            app.user_settings,
                            app.users
                        RESTART IDENTITY CASCADE
                        """
                    )

    def query_one(self, sql: str, params=()):
        with psycopg.connect(self.test_database_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()

    def create_user(self, telegram_user_id: int, telegram_chat_id: int, username: str) -> int:
        row = db.upsert_user(
            telegram_user_id=telegram_user_id,
            telegram_chat_id=telegram_chat_id,
            username=username,
        )
        db.ensure_user_settings(row["id"])
        return int(row["id"])


class UserIntegrationTests(PostgresIntegrationTestCase):
    def test_user_registration_creates_and_updates_same_user(self) -> None:
        first = db.upsert_user(telegram_user_id=1001, telegram_chat_id=2001, username="first_name")
        second = db.upsert_user(telegram_user_id=1001, telegram_chat_id=2002, username="renamed")

        row = self.query_one(
            """
            SELECT id, telegram_user_id, telegram_chat_id, username, allowlisted, status
            FROM app.users
            WHERE telegram_user_id = %s
            """,
            (1001,),
        )
        count_row = self.query_one(
            "SELECT COUNT(*)::INT AS cnt FROM app.users WHERE telegram_user_id = %s",
            (1001,),
        )

        self.assertEqual(first["id"], second["id"])
        self.assertEqual(count_row["cnt"], 1)
        self.assertEqual(row["telegram_chat_id"], 2002)
        self.assertEqual(row["username"], "renamed")
        self.assertFalse(row["allowlisted"])
        self.assertEqual(row["status"], "pending")

    def test_schedule_storage_is_per_user(self) -> None:
        user_a = self.create_user(telegram_user_id=1010, telegram_chat_id=2010, username="alpha")
        user_b = self.create_user(telegram_user_id=1020, telegram_chat_id=2020, username="beta")

        db.set_user_timezone(user_a, "Europe/Riga")
        db.set_user_daily_time(user_a, dt_time(7, 30))
        db.set_user_timezone(user_b, "Asia/Tokyo")
        db.set_user_daily_time(user_b, dt_time(21, 45))

        settings_a = db.get_user_settings(user_a)
        settings_b = db.get_user_settings(user_b)

        self.assertEqual(settings_a["timezone"], "Europe/Riga")
        self.assertEqual(settings_a["daily_time_local"], dt_time(7, 30))
        self.assertEqual(settings_b["timezone"], "Asia/Tokyo")
        self.assertEqual(settings_b["daily_time_local"], dt_time(21, 45))


class JobIntegrationTests(PostgresIntegrationTestCase):
    def test_enqueue_daily_job_creates_only_one_job_per_user_per_local_date(self) -> None:
        user_id = self.create_user(telegram_user_id=1100, telegram_chat_id=2100, username="daily")
        db.approve_user(1100)
        db.set_user_timezone(user_id, "Asia/Tokyo")
        db.set_user_daily_time(user_id, dt_time(9, 0))

        fixed_now = datetime(2026, 3, 9, 23, 59, tzinfo=timezone.utc)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return fixed_now.replace(tzinfo=None)
                return fixed_now.astimezone(tz)

        cfg = worker.WorkerConfig(
            bot_token="test-token",
            library_limit=1,
            worker_id="test-worker",
            poll_seconds=1,
            claim_batch_size=10,
            retry_backoff_base_seconds=30,
            retry_backoff_max_seconds=1800,
            due_window_seconds=60,
            job_lease_seconds=300,
            provider_sync_interval_seconds=21600,
        )

        with patch.object(worker, "datetime", FixedDateTime):
            first_count = worker.enqueue_due_jobs(cfg)
            second_count = worker.enqueue_due_jobs(cfg)

        jobs_row = self.query_one("SELECT COUNT(*)::INT AS cnt FROM app.jobs WHERE user_id = %s", (user_id,))
        idem_row = self.query_one(
            "SELECT COUNT(*)::INT AS cnt FROM app.idempotency_keys WHERE user_id = %s AND key = %s",
            (user_id, f"daily:{user_id}:2026-03-10"),
        )

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        self.assertEqual(jobs_row["cnt"], 1)
        self.assertEqual(idem_row["cnt"], 1)

    def test_enqueue_job_once_rolls_back_idempotency_key_when_job_insert_fails(self) -> None:
        user_id = self.create_user(telegram_user_id=1150, telegram_chat_id=2150, username="idem")
        existing_job_id = uuid4()
        idempotency_key = f"manual:{user_id}:rollback-check"

        db.enqueue_job(
            job_id=existing_job_id,
            user_id=user_id,
            job_type="deliver_now",
            run_at=datetime.now(timezone.utc),
            payload={"telegram_chat_id": 2150},
        )

        with self.assertRaises(Exception):
            db.enqueue_job_once(
                idempotency_key=idempotency_key,
                idempotency_expires_at=datetime.now(timezone.utc) + timedelta(days=1),
                job_id=existing_job_id,
                user_id=user_id,
                job_type="deliver_now",
                run_at=datetime.now(timezone.utc),
                payload={"telegram_chat_id": 2150},
            )

        idem_row = self.query_one(
            "SELECT COUNT(*)::INT AS cnt FROM app.idempotency_keys WHERE key = %s",
            (idempotency_key,),
        )
        jobs_row = self.query_one(
            "SELECT COUNT(*)::INT AS cnt FROM app.jobs WHERE user_id = %s",
            (user_id,),
        )

        retry_row = db.enqueue_job_once(
            idempotency_key=idempotency_key,
            idempotency_expires_at=datetime.now(timezone.utc) + timedelta(days=1),
            job_id=uuid4(),
            user_id=user_id,
            job_type="deliver_now",
            run_at=datetime.now(timezone.utc),
            payload={"telegram_chat_id": 2150},
        )

        idem_row_after_retry = self.query_one(
            "SELECT COUNT(*)::INT AS cnt FROM app.idempotency_keys WHERE key = %s",
            (idempotency_key,),
        )
        jobs_row_after_retry = self.query_one(
            "SELECT COUNT(*)::INT AS cnt FROM app.jobs WHERE user_id = %s",
            (user_id,),
        )

        self.assertEqual(idem_row["cnt"], 0)
        self.assertEqual(jobs_row["cnt"], 1)
        self.assertIsNotNone(retry_row)
        self.assertEqual(idem_row_after_retry["cnt"], 1)
        self.assertEqual(jobs_row_after_retry["cnt"], 2)

    def test_claiming_runnable_jobs_does_not_return_same_job_twice(self) -> None:
        user_id = self.create_user(telegram_user_id=1200, telegram_chat_id=2200, username="claim")
        job = db.enqueue_job(
            job_id=uuid4(),
            user_id=user_id,
            job_type="deliver_now",
            run_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            payload={"telegram_chat_id": 2200},
        )

        first_claim = db.claim_runnable_jobs(worker_id="worker-a", batch_size=10)
        second_claim = db.claim_runnable_jobs(worker_id="worker-b", batch_size=10)

        row = self.query_one("SELECT status, locked_by FROM app.jobs WHERE id = %s", (job["id"],))

        self.assertEqual(len(first_claim), 1)
        self.assertEqual(first_claim[0]["id"], job["id"])
        self.assertEqual(len(second_claim), 0)
        self.assertEqual(row["status"], "running")
        self.assertEqual(row["locked_by"], "worker-a")

    def test_retry_path_increments_attempt_and_reschedules(self) -> None:
        user_id = self.create_user(telegram_user_id=1300, telegram_chat_id=2300, username="retry")
        job = db.enqueue_job(
            job_id=uuid4(),
            user_id=user_id,
            job_type="deliver_now",
            run_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            payload={"telegram_chat_id": 2300},
        )
        claimed = db.claim_runnable_jobs(worker_id="worker-a", batch_size=10)
        next_run_at = datetime.now(timezone.utc) + timedelta(minutes=5)

        state = db.mark_job_failed(job_id=job["id"], error_text="boom", next_run_at=next_run_at)
        row = self.query_one(
            "SELECT attempt, status, run_at, last_error, locked_by, locked_at FROM app.jobs WHERE id = %s",
            (job["id"],),
        )

        self.assertEqual(len(claimed), 1)
        self.assertEqual(state["attempt"], 1)
        self.assertEqual(state["status"], "queued")
        self.assertEqual(state["run_at"], next_run_at)
        self.assertEqual(row["attempt"], 1)
        self.assertEqual(row["status"], "queued")
        self.assertEqual(row["run_at"], next_run_at)
        self.assertEqual(row["last_error"], "boom")
        self.assertIsNone(row["locked_by"])
        self.assertIsNone(row["locked_at"])


class DeliveryHistoryIntegrationTests(PostgresIntegrationTestCase):
    def test_inserting_delivery_history_prevents_duplicate_album_in_same_cycle(self) -> None:
        user_id = self.create_user(telegram_user_id=1400, telegram_chat_id=2400, username="history")

        first_insert = db.insert_delivery_history(user_id=user_id, cycle_number=1, album_id="album-1")
        second_insert = db.insert_delivery_history(user_id=user_id, cycle_number=1, album_id="album-1")
        third_insert = db.insert_delivery_history(user_id=user_id, cycle_number=2, album_id="album-1")

        self.assertTrue(first_insert)
        self.assertFalse(second_insert)
        self.assertTrue(third_insert)


class ProviderAccountIntegrationTests(PostgresIntegrationTestCase):
    def test_upsert_user_provider_account_credentials_encrypts_and_loads_credentials(self) -> None:
        user_id = self.create_user(telegram_user_id=1500, telegram_chat_id=2500, username="provider_user")

        row = db.upsert_user_provider_account_credentials(
            user_id=user_id,
            provider="ytmusic",
            credentials={"cookie_blob": "secret-cookie", "visitor_data": "abc123"},
            status="connected",
            is_active=True,
        )

        db_row = self.query_one(
            """
            SELECT id, provider, credentials_encrypted, is_active
            FROM app.user_provider_accounts
            WHERE id = %s
            """,
            (row["id"],),
        )

        self.assertEqual(db_row["provider"], "ytmusic")
        self.assertTrue(db_row["is_active"])
        self.assertTrue(str(db_row["credentials_encrypted"]).startswith("fernet:v1:"))
        self.assertNotIn("secret-cookie", str(db_row["credentials_encrypted"]))

        credentials = db.get_user_provider_account_credentials(int(row["id"]))
        self.assertEqual(
            credentials,
            {"cookie_blob": "secret-cookie", "visitor_data": "abc123"},
        )

    def test_activating_new_provider_deactivates_previous_active_account(self) -> None:
        user_id = self.create_user(telegram_user_id=1600, telegram_chat_id=2600, username="switcher")

        first = db.upsert_user_provider_account_credentials(
            user_id=user_id,
            provider="ytmusic",
            credentials={"cookie_blob": "yt"},
            is_active=True,
        )
        second = db.upsert_user_provider_account_credentials(
            user_id=user_id,
            provider="spotify",
            credentials={"refresh_token": "sp"},
            is_active=True,
        )

        active = db.get_active_user_provider_account(user_id)
        inactive_row = self.query_one(
            """
            SELECT is_active
            FROM app.user_provider_accounts
            WHERE id = %s
            """,
            (first["id"],),
        )
        count_row = self.query_one(
            """
            SELECT COUNT(*)::INT AS cnt
            FROM app.user_provider_accounts
            WHERE user_id = %s
            """,
            (user_id,),
        )

        self.assertEqual(active["id"], second["id"])
        self.assertEqual(active["provider"], "spotify")
        self.assertFalse(inactive_row["is_active"])
        self.assertEqual(count_row["cnt"], 2)

    def test_sync_state_tracks_explicit_result_statuses(self) -> None:
        user_id = self.create_user(telegram_user_id=1700, telegram_chat_id=2700, username="sync_state")
        account = db.upsert_user_provider_account_credentials(
            user_id=user_id,
            provider="ytmusic",
            credentials={"cookie_blob": "secret-cookie"},
            status="connected",
            is_active=True,
        )

        db.mark_user_provider_sync_started(int(account["id"]))
        db.mark_user_provider_sync_failed(int(account["id"]), "temporary outage", result_status="transient_error")
        failed_state = db.get_user_provider_sync_state(int(account["id"]))
        self.assertEqual(failed_state["last_sync_result"], "transient_error")
        self.assertEqual(failed_state["last_error"], "temporary outage")

        db.mark_user_provider_sync_succeeded(int(account["id"]), library_item_count=0, result_status="empty_library")
        empty_state = db.get_user_provider_sync_state(int(account["id"]))
        self.assertEqual(empty_state["last_sync_result"], "empty_library")
        self.assertEqual(empty_state["library_item_count"], 0)
        self.assertIsNone(empty_state["last_error"])

    def test_due_sync_query_only_returns_connected_accounts(self) -> None:
        sync_before = datetime.now(timezone.utc) - timedelta(hours=6)
        connected_user_id = self.create_user(telegram_user_id=1800, telegram_chat_id=2800, username="connected")
        blocked_user_id = self.create_user(telegram_user_id=1801, telegram_chat_id=2801, username="reauth")
        db.approve_user(1800)
        db.approve_user(1801)

        connected = db.upsert_user_provider_account_credentials(
            user_id=connected_user_id,
            provider="ytmusic",
            credentials={"cookie_blob": "yt-connected"},
            status="connected",
            is_active=True,
        )
        db.upsert_user_provider_account_credentials(
            user_id=blocked_user_id,
            provider="ytmusic",
            credentials={"cookie_blob": "yt-reauth"},
            status="needs_reauth",
            is_active=True,
        )

        rows = db.list_provider_accounts_due_for_sync(sync_before=sync_before)

        self.assertEqual([int(row["id"]) for row in rows], [int(connected["id"])])


class OAuthSessionIntegrationTests(PostgresIntegrationTestCase):
    def test_create_oauth_session_persists_user_binding_and_expiry(self) -> None:
        user_id = self.create_user(telegram_user_id=1900, telegram_chat_id=2900, username="oauth_user")
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)

        session = db.create_oauth_session(
            user_id=user_id,
            provider="spotify",
            state="state-abc",
            requested_chat_id=2900,
            expires_at=expires_at,
        )

        row = self.query_one(
            """
            SELECT user_id, provider, state, status, requested_chat_id, expires_at, consumed_at
            FROM app.oauth_sessions
            WHERE id = %s
            """,
            (session["id"],),
        )

        self.assertEqual(row["user_id"], user_id)
        self.assertEqual(row["provider"], "spotify")
        self.assertEqual(row["state"], "state-abc")
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["requested_chat_id"], 2900)
        self.assertEqual(row["expires_at"], expires_at)
        self.assertIsNone(row["consumed_at"])

    def test_update_oauth_session_status_enforces_expected_current_status(self) -> None:
        user_id = self.create_user(telegram_user_id=1901, telegram_chat_id=2901, username="oauth_status")
        session = db.create_oauth_session(
            user_id=user_id,
            provider="spotify",
            state="state-def",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        )

        consumed = db.update_oauth_session_status(
            int(session["id"]),
            db.OAUTH_SESSION_STATUS_CONSUMED,
            expected_current_status=db.OAUTH_SESSION_STATUS_PENDING,
        )
        second_update = db.update_oauth_session_status(
            int(session["id"]),
            db.OAUTH_SESSION_STATUS_FAILED,
            expected_current_status=db.OAUTH_SESSION_STATUS_PENDING,
        )
        reloaded = db.get_oauth_session_by_state("spotify", "state-def")

        self.assertIsNotNone(consumed)
        self.assertEqual(consumed["status"], "consumed")
        self.assertIsNone(second_update)
        self.assertEqual(reloaded["status"], "consumed")
        self.assertIsNotNone(reloaded["consumed_at"])


if __name__ == "__main__":
    unittest.main()
