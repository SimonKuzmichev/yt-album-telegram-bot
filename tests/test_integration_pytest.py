from __future__ import annotations

import asyncio
import importlib
import os
import sys
from datetime import datetime, time as dt_time, timedelta, timezone
from types import ModuleType, SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover - optional dependency guard
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


@pytest.fixture(scope="session")
def integration_modules():
    if psycopg is None:
        pytest.skip("psycopg is not installed")

    _install_worker_import_stubs()
    sys.modules.pop("src.db", None)
    sys.modules.pop("worker", None)

    try:
        db = importlib.import_module("src.db")
        worker = importlib.import_module("worker")
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on env
        pytest.skip(f"missing runtime dependency for integration tests: {exc.name}")

    return db, worker


@pytest.fixture(scope="session")
def test_database_url(integration_modules):
    database_url = os.getenv("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is not set")

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
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
        pytest.skip("app schema is not present in TEST_DATABASE_URL")

    return database_url


@pytest.fixture(autouse=True)
def integration_env(monkeypatch: pytest.MonkeyPatch, test_database_url: str):
    monkeypatch.setenv("DATABASE_URL", test_database_url)
    monkeypatch.setenv("CREDENTIALS_MASTER_KEY", Fernet.generate_key().decode("utf-8"))
    monkeypatch.setenv("REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))


@pytest.fixture(autouse=True)
def reset_database(test_database_url: str):
    with psycopg.connect(test_database_url, row_factory=dict_row) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE TABLE
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


def _query_one(database_url: str, sql: str, params=()):
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def _create_user(db, *, telegram_user_id: int, telegram_chat_id: int, username: str) -> int:
    row = db.upsert_user(
        telegram_user_id=telegram_user_id,
        telegram_chat_id=telegram_chat_id,
        username=username,
    )
    db.ensure_user_settings(row["id"])
    return int(row["id"])


def _worker_config(worker_module):
    return worker_module.WorkerConfig(
        bot_token="test-token",
        library_limit=10,
        worker_id="pytest-worker",
        poll_seconds=1,
        claim_batch_size=10,
        retry_backoff_base_seconds=30,
        retry_backoff_max_seconds=1800,
        due_window_seconds=60,
        job_lease_seconds=300,
        provider_sync_interval_seconds=21600,
    )


@pytest.mark.integration
def test_provider_selection_rules_and_status_transitions(integration_modules, test_database_url: str) -> None:
    db, _worker = integration_modules
    user_id = _create_user(db, telegram_user_id=3001, telegram_chat_id=4001, username="phase2-user")
    db.approve_user(3001)

    ytmusic_account = db.upsert_user_provider_account_credentials(
        user_id,
        "ytmusic",
        {"cookie_blob": "ytmusic-secret"},
        is_active=True,
    )
    spotify_account = db.upsert_user_provider_account_credentials(
        user_id,
        "spotify",
        {"access_token": "spotify-secret"},
        status="needs_reauth",
        is_active=False,
    )

    active = db.get_active_user_provider_account(user_id)
    assert active is not None
    assert active["provider"] == "ytmusic"

    switched = db.set_active_user_provider_account(user_id, "spotify")
    active = db.get_active_user_provider_account(user_id)
    assert switched is None
    assert active is not None
    assert active["provider"] == "ytmusic"

    db.mark_user_provider_account_status(int(spotify_account["id"]), "connected")
    switched = db.set_active_user_provider_account(user_id, "spotify")
    active = db.get_active_user_provider_account(user_id)
    assert switched is not None
    assert active is not None
    assert active["provider"] == "spotify"
    db.mark_user_provider_sync_started(int(spotify_account["id"]))
    db.mark_user_provider_sync_failed(int(spotify_account["id"]), "401 unauthorized", result_status="auth_error")

    failed_state = db.get_user_provider_sync_state(int(spotify_account["id"]))
    assert failed_state is not None
    assert failed_state["last_sync_result"] == "auth_error"
    assert failed_state["last_error"] == "401 unauthorized"

    db.mark_user_provider_sync_succeeded(int(spotify_account["id"]), library_item_count=2, result_status="ok")

    recovered_state = db.get_user_provider_sync_state(int(spotify_account["id"]))
    assert recovered_state is not None
    assert recovered_state["last_sync_result"] == "ok"
    assert recovered_state["library_item_count"] == 2
    assert recovered_state["last_error"] is None

    encrypted_row = _query_one(
        test_database_url,
        "SELECT credentials_encrypted FROM app.user_provider_accounts WHERE id = %s",
        (int(ytmusic_account["id"]),),
    )
    assert encrypted_row is not None
    assert encrypted_row["credentials_encrypted"].startswith("fernet:v1:")
    assert "ytmusic-secret" not in encrypted_row["credentials_encrypted"]


@pytest.mark.integration
def test_sync_job_populates_cache_and_delivery_uses_cached_albums(integration_modules, test_database_url: str) -> None:
    db, worker = integration_modules
    cfg = _worker_config(worker)
    user_id = _create_user(db, telegram_user_id=3101, telegram_chat_id=4101, username="delivery-user")
    db.approve_user(3101)
    db.set_user_timezone(user_id, "UTC")
    db.set_user_daily_time(user_id, dt_time(9, 0))

    account = db.upsert_user_provider_account_credentials(
        user_id,
        "ytmusic",
        {"cookie_blob": "super-secret-cookie"},
        is_active=True,
    )
    account_id = int(account["id"])

    sync_job = db.enqueue_job_once(
        idempotency_key=f"sync:{account_id}:phase2",
        idempotency_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        job_id=uuid4(),
        user_id=user_id,
        job_type=worker.JOB_TYPE_SYNC_LIBRARY,
        run_at=datetime.now(timezone.utc),
        payload={
            "idempotency_key": f"sync:{account_id}:phase2",
            "user_provider_account_id": account_id,
            "provider": "ytmusic",
        },
    )
    assert sync_job is not None

    albums_from_provider = [
        {
            "provider": "ytmusic",
            "provider_album_id": "album-1",
            "title": "Discovery",
            "artist": "Daft Punk",
            "url": "https://music.youtube.com/browse/album-1",
            "release_year": 2001,
            "raw_payload_json": {"browseId": "album-1"},
        },
        {
            "provider": "ytmusic",
            "provider_album_id": "album-2",
            "title": "Homework",
            "artist": "Daft Punk",
            "url": "https://music.youtube.com/browse/album-2",
            "release_year": 1997,
            "raw_payload_json": {"browseId": "album-2"},
        },
    ]
    provider_client = SimpleNamespace(list_saved_albums=lambda limit=None: albums_from_provider)

    with patch.object(worker, "_build_provider_client_for_account", return_value=provider_client):
        processed = asyncio.run(worker.process_claimed_jobs(bot=SimpleNamespace(), cfg=cfg))

    assert processed == 1

    encrypted_row = _query_one(
        test_database_url,
        "SELECT credentials_encrypted FROM app.user_provider_accounts WHERE id = %s",
        (account_id,),
    )
    assert encrypted_row is not None
    assert encrypted_row["credentials_encrypted"].startswith("fernet:v1:")
    assert "super-secret-cookie" not in encrypted_row["credentials_encrypted"]

    cached_rows = db.list_available_user_library_albums(account_id)
    assert [row["provider_album_id"] for row in cached_rows] == ["album-1", "album-2"]

    delivery_job = db.enqueue_job(
        job_id=uuid4(),
        user_id=user_id,
        job_type=worker.JOB_TYPE_DELIVER_NOW,
        run_at=datetime.now(timezone.utc),
        payload={"telegram_chat_id": 4101},
    )
    assert delivery_job["job_type"] == worker.JOB_TYPE_DELIVER_NOW

    delivered: dict[str, object] = {}

    async def fake_send_album_message(bot, chat_id: int, album: dict, prefix: str) -> None:
        delivered["chat_id"] = chat_id
        delivered["album"] = album
        delivered["prefix"] = prefix

    with patch.object(worker, "_sync_provider_account", side_effect=AssertionError("delivery should use cached albums")), \
         patch.object(worker, "send_album_message", new=fake_send_album_message), \
         patch.object(worker.random, "choice", side_effect=lambda seq: seq[0]):
        processed = asyncio.run(worker.process_claimed_jobs(bot=SimpleNamespace(), cfg=cfg))

    assert processed == 1
    assert delivered["chat_id"] == 4101
    assert delivered["prefix"] == "🎲 Album now"
    assert delivered["album"]["provider_album_id"] == "album-1"

    history_row = _query_one(
        test_database_url,
        "SELECT COUNT(*)::INT AS cnt FROM app.delivery_history WHERE user_id = %s",
        (user_id,),
    )
    assert history_row is not None
    assert history_row["cnt"] == 1
