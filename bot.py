import logging
import os
import secrets
import threading
from datetime import datetime, time as dt_time, timedelta, timezone
from html import escape
from urllib.parse import urlencode
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
import requests
try:
    import redis.asyncio as redis
except ModuleNotFoundError:  # pragma: no cover - exercised only when dependency is absent
    redis = None
from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
)
import uvicorn

from src.errors import is_auth_error, format_auth_help
from src.logging_utils import configure_logging, log_event
from src.metrics import record_command, record_rate_limit_hit, start_metrics_server
from src.db import (
    OAUTH_SESSION_STATUS_CONSUMED,
    OAUTH_SESSION_STATUS_EXPIRED,
    OAUTH_SESSION_STATUS_FAILED,
    OAUTH_SESSION_STATUS_PENDING,
    PROVIDER_ACCOUNT_STATUS_CONNECTED,
    approve_user,
    block_user,
    create_oauth_session,
    enqueue_job_once,
    ensure_user_settings,
    get_active_user_provider_account,
    get_admin_status_snapshot,
    get_oauth_session_by_state,
    get_user_provider_sync_state,
    get_user_timezone_by_chat_id,
    get_user_delivery_stats,
    get_user_settings,
    list_user_provider_accounts,
    list_recent_deliveries,
    mark_user_provider_account_status,
    set_active_user_provider_account,
    set_user_daily_time,
    set_user_timezone,
    update_oauth_session_status,
    upsert_user_provider_account_credentials,
    upsert_user,
)
from src.telegram_delivery import (
    CB_NEXT,
    CB_REFRESH,
    CB_STATUS,
    build_keyboard,
)


Album = Dict[str, Any]
JOB_TYPE_DELIVER_NOW = "deliver_now"
JOB_TYPE_NEXT_CYCLE_NOW = "next_cycle_now"
JOB_TYPE_SYNC_LIBRARY = "sync_library"
COMMAND_LOCK_TTLS_SECONDS = {
    "refresh": 60,
    "now": 5,
    "nextcycle": 60,
}
RATE_LIMIT_WINDOWS = {
    "now": (
        ("hour", 3600, "NOW_RATE_LIMIT_HOURLY", 6),
        ("day", 86400, "NOW_RATE_LIMIT_DAILY", 20),
    ),
    "nextcycle": (
        ("hour", 3600, "NEXTCYCLE_RATE_LIMIT_HOURLY", 6),
        ("day", 86400, "NEXTCYCLE_RATE_LIMIT_DAILY", 20),
    ),
    "refresh": (
        ("hour", 3600, "REFRESH_RATE_LIMIT_HOURLY", 2),
        ("day", 86400, "REFRESH_RATE_LIMIT_DAILY", 6),
    ),
}
logger = logging.getLogger(__name__)
DEFAULT_SPOTIFY_SCOPE = "user-library-read"
DEFAULT_OAUTH_STATE_TTL_SECONDS = 600
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"


class HTTPServerHandle:
    def __init__(self, server: uvicorn.Server, thread: threading.Thread) -> None:
        self.server = server
        self.thread = thread

    def shutdown(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def create_http_app() -> FastAPI:
    app = FastAPI()

    @app.get("/healthz", response_class=PlainTextResponse)
    async def healthz() -> str:
        return "ok"

    @app.get("/oauth/spotify/callback", response_class=HTMLResponse)
    async def spotify_callback(
        state: str | None = None,
        code: str | None = None,
        error: str | None = None,
    ) -> str:
        return handle_spotify_callback(
            state=state,
            code=code,
            error=error,
        )

    return app


def start_http_server(host: str, port: int) -> HTTPServerHandle:
    config = uvicorn.Config(
        create_http_app(),
        host=host,
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="healthcheck-http", daemon=True)
    thread.start()
    return HTTPServerHandle(server, thread)


def build_spotify_callback_html(
    *,
    title: str,
    message: str,
    state_present: bool,
    code_present: bool,
) -> str:
    state_value = "yes" if state_present else "no"
    code_value = "yes" if code_present else "no"
    return (
        "<!doctype html>"
        "<html lang=\"en\">"
        "<head><meta charset=\"utf-8\"><title>Spotify Callback</title></head>"
        "<body>"
        f"<h1>{escape(title)}</h1>"
        f"<p>{escape(message)}</p>"
        f"<p>state present: {state_value}</p>"
        f"<p>code present: {code_value}</p>"
        "</body>"
        "</html>\n"
    )


def _query_param_present(value: str | None) -> bool:
    return value is not None and value != ""


def generate_oauth_state() -> str:
    return secrets.token_urlsafe(24)


def get_spotify_oauth_state_ttl_seconds() -> int:
    return get_env_int("SPOTIFY_OAUTH_STATE_TTL_SECONDS", DEFAULT_OAUTH_STATE_TTL_SECONDS)


def build_spotify_authorize_url(*, client_id: str, redirect_uri: str, state: str) -> str:
    query = urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": DEFAULT_SPOTIFY_SCOPE,
            "state": state,
        }
    )
    return f"https://accounts.spotify.com/authorize?{query}"


def _get_spotify_token_exchange_credentials() -> tuple[str, str, str]:
    client_id = os.getenv("SPOTIFY_CLIENT_ID", "").strip()
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET", "").strip()
    redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "").strip()
    if not client_id or not client_secret or not redirect_uri:
        raise RuntimeError(
            "Spotify OAuth is not configured. Set SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, and SPOTIFY_REDIRECT_URI."
        )
    return client_id, client_secret, redirect_uri


def exchange_spotify_code_for_tokens(*, code: str) -> Dict[str, Any]:
    client_id, client_secret, redirect_uri = _get_spotify_token_exchange_credentials()
    response = requests.post(
        SPOTIFY_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
        auth=(client_id, client_secret),
        timeout=15,
    )

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Spotify token exchange returned invalid JSON (status={response.status_code})") from exc

    if response.status_code >= 400:
        error_code = payload.get("error") or "unknown_error"
        error_description = payload.get("error_description") or "token exchange failed"
        raise RuntimeError(f"Spotify token exchange failed: {error_code}: {error_description}")

    access_token = str(payload.get("access_token") or "").strip()
    token_type = str(payload.get("token_type") or "").strip()
    expires_in = payload.get("expires_in")
    if not access_token or not token_type or expires_in is None:
        raise RuntimeError("Spotify token exchange response was missing required fields")

    try:
        expires_in_seconds = int(expires_in)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Spotify token exchange returned an invalid expires_in value") from exc

    refresh_token = str(payload.get("refresh_token") or "").strip()
    granted_scope = str(payload.get("scope") or "").strip()

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_type,
        "granted_scope": granted_scope,
        "expires_in_seconds": expires_in_seconds,
    }


def _ensure_spotify_failure_account_state(user_id: int) -> None:
    accounts = list_user_provider_accounts(user_id)
    spotify_account = next((row for row in accounts if str(row.get("provider") or "") == "spotify"), None)
    if spotify_account is not None:
        mark_user_provider_account_status(int(spotify_account["id"]), "needs_reauth")
        return

    upsert_user_provider_account_credentials(
        user_id=user_id,
        provider="spotify",
        credentials={},
        status="pending",
        is_active=False,
    )


def _mark_spotify_oauth_failed(*, session_id: int, user_id: int) -> None:
    try:
        update_oauth_session_status(
            session_id,
            OAUTH_SESSION_STATUS_FAILED,
            expected_current_status=OAUTH_SESSION_STATUS_PENDING,
        )
        _ensure_spotify_failure_account_state(user_id)
    except Exception:
        logger.exception("Failed to persist Spotify OAuth failure state session_id=%s user_id=%s", session_id, user_id)


def _queue_spotify_initial_sync(*, user_id: int, provider_account_id: int, telegram_chat_id: Optional[int]) -> None:
    if telegram_chat_id is None:
        return

    now_utc = datetime.now(timezone.utc)
    idempotency_key = f"spotify-oauth-sync:{provider_account_id}"
    enqueue_job_once(
        idempotency_key=idempotency_key,
        idempotency_expires_at=now_utc + timedelta(minutes=30),
        job_id=uuid4(),
        user_id=user_id,
        job_type=JOB_TYPE_SYNC_LIBRARY,
        run_at=now_utc,
        payload={
            "telegram_chat_id": telegram_chat_id,
            "idempotency_key": idempotency_key,
            "user_provider_account_id": provider_account_id,
            "provider": "spotify",
        },
    )


def handle_spotify_callback(
    *,
    state: str | None,
    code: str | None,
    error: str | None = None,
    now_utc: Optional[datetime] = None,
) -> str:
    state_present = _query_param_present(state)
    code_present = _query_param_present(code)
    current_time = now_utc or datetime.now(timezone.utc)

    if not state_present:
        return build_spotify_callback_html(
            title="Invalid Spotify callback",
            message="Missing state parameter.",
            state_present=False,
            code_present=code_present,
        )

    try:
        session = get_oauth_session_by_state("spotify", str(state))
    except Exception:
        logger.exception("Failed to load OAuth session for Spotify callback")
        return build_spotify_callback_html(
            title="Spotify callback error",
            message="Could not validate the connection request.",
            state_present=True,
            code_present=code_present,
        )

    if session is None:
        return build_spotify_callback_html(
            title="Unknown Spotify state",
            message="This connection request is invalid or has already been cleared.",
            state_present=True,
            code_present=code_present,
        )

    if str(session.get("status") or "") != OAUTH_SESSION_STATUS_PENDING:
        return build_spotify_callback_html(
            title="Spotify callback already processed",
            message="This connection request was already handled. You can return to Telegram.",
            state_present=True,
            code_present=code_present,
        )

    expires_at = session.get("expires_at")
    if isinstance(expires_at, datetime) and expires_at <= current_time:
        update_oauth_session_status(
            int(session["id"]),
            OAUTH_SESSION_STATUS_EXPIRED,
            expected_current_status=OAUTH_SESSION_STATUS_PENDING,
        )
        return build_spotify_callback_html(
            title="Spotify state expired",
            message="This connection request expired. Start /connect_spotify again in Telegram.",
            state_present=True,
            code_present=code_present,
        )

    if _query_param_present(error):
        _mark_spotify_oauth_failed(session_id=int(session["id"]), user_id=int(session["user_id"]))
        return build_spotify_callback_html(
            title="Spotify authorization failed",
            message="Spotify returned an authorization error. Start /connect_spotify again in Telegram.",
            state_present=True,
            code_present=code_present,
        )

    if not code_present:
        _mark_spotify_oauth_failed(session_id=int(session["id"]), user_id=int(session["user_id"]))
        return build_spotify_callback_html(
            title="Spotify callback incomplete",
            message="Missing authorization code. Start /connect_spotify again in Telegram.",
            state_present=True,
            code_present=False,
        )

    try:
        token_payload = exchange_spotify_code_for_tokens(code=str(code))
    except Exception:
        logger.exception("Spotify token exchange failed for oauth_session_id=%s", session.get("id"))
        _mark_spotify_oauth_failed(session_id=int(session["id"]), user_id=int(session["user_id"]))
        return build_spotify_callback_html(
            title="Spotify connection failed",
            message="Spotify returned, but token exchange failed. Start /connect_spotify again in Telegram.",
            state_present=True,
            code_present=True,
        )

    try:
        token_expires_at = current_time + timedelta(seconds=int(token_payload["expires_in_seconds"]))
        provider_account = upsert_user_provider_account_credentials(
            user_id=int(session["user_id"]),
            provider="spotify",
            credentials={
                "access_token": token_payload["access_token"],
                "refresh_token": token_payload["refresh_token"],
                "token_type": token_payload["token_type"],
                "granted_scope": token_payload["granted_scope"],
            },
            status=PROVIDER_ACCOUNT_STATUS_CONNECTED,
            is_active=True,
            token_expires_at=token_expires_at,
        )
    except Exception:
        logger.exception("Failed to persist Spotify credentials for oauth_session_id=%s", session.get("id"))
        _mark_spotify_oauth_failed(session_id=int(session["id"]), user_id=int(session["user_id"]))
        return build_spotify_callback_html(
            title="Spotify connection failed",
            message="Spotify returned successfully, but we could not finish saving the connection. Start /connect_spotify again in Telegram.",
            state_present=True,
            code_present=True,
        )

    updated_session = update_oauth_session_status(
        int(session["id"]),
        OAUTH_SESSION_STATUS_CONSUMED,
        expected_current_status=OAUTH_SESSION_STATUS_PENDING,
    )
    if updated_session is None:
        return build_spotify_callback_html(
            title="Spotify callback already processed",
            message="This connection request was already handled. You can return to Telegram.",
            state_present=True,
            code_present=True,
        )

    _queue_spotify_initial_sync(
        user_id=int(session["user_id"]),
        provider_account_id=int(provider_account["id"]),
        telegram_chat_id=session.get("requested_chat_id"),
    )

    return build_spotify_callback_html(
        title="Spotify authorization received",
        message="Spotify returned successfully. You can go back to Telegram.",
        state_present=True,
        code_present=True,
    )


def _log_bot_event(
    event: str,
    *,
    level: int = logging.INFO,
    message: str | None = None,
    exc_info: Any = None,
    user_id: Optional[int] = None,
    telegram_chat_id: Optional[int] = None,
    job_id: Optional[str] = None,
    job_type: Optional[str] = None,
    attempt: Optional[int] = None,
    idempotency_key: Optional[str] = None,
) -> None:
    log_event(
        logger,
        level,
        event,
        message=message,
        exc_info=exc_info,
        user_id=user_id,
        telegram_chat_id=telegram_chat_id,
        job_id=job_id,
        job_type=job_type,
        attempt=attempt,
        idempotency_key=idempotency_key,
    )


def get_env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped or default


def get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def get_command_lock_key(action: str, user_id: int) -> str:
    return f"command-lock:{action}:{user_id}"


def get_request_dedupe_key(action: str, request_id: str) -> str:
    return f"request-dedupe:{action}:{request_id}"


def get_rate_limit_key(
    action: str,
    user_id: int,
    window_name: str,
    bucket: int,
) -> str:
    return f"rate-limit:{action}:{user_id}:{window_name}:{bucket}"


def _get_rate_limit_rules(action: str) -> tuple[tuple[str, int, int], ...]:
    window_specs = RATE_LIMIT_WINDOWS.get(action)
    if window_specs is None:
        raise ValueError(f"Unsupported rate limit action: {action}")

    return tuple(
        (window_name, window_seconds, get_env_int(env_name, default_limit))
        for window_name, window_seconds, env_name, default_limit in window_specs
    )


def _get_rate_limit_bucket(now_ts: int, window_seconds: int) -> int:
    return now_ts // window_seconds


def _format_retry_after(retry_after_seconds: int) -> str:
    if retry_after_seconds <= 60:
        return f"{retry_after_seconds}s"
    minutes, seconds = divmod(retry_after_seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s" if seconds else f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m" if minutes else f"{hours}h"


async def acquire_command_lock(
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
    user_id: int,
) -> bool:
    ttl_seconds = COMMAND_LOCK_TTLS_SECONDS.get(action)
    if ttl_seconds is None:
        raise ValueError(f"Unsupported command lock action: {action}")

    redis_client = context.application.bot_data.get("redis")
    if redis_client is None:
        raise RuntimeError("Redis client is not configured")

    result = await redis_client.set(
        get_command_lock_key(action, user_id),
        "1",
        ex=ttl_seconds,
        nx=True,
    )
    return bool(result)


async def acquire_request_dedupe(
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
    request_id: str,
) -> bool:
    ttl_seconds = COMMAND_LOCK_TTLS_SECONDS.get(action)
    if ttl_seconds is None:
        raise ValueError(f"Unsupported request dedupe action: {action}")

    redis_client = context.application.bot_data.get("redis")
    if redis_client is None:
        raise RuntimeError("Redis client is not configured")

    result = await redis_client.set(
        get_request_dedupe_key(action, request_id),
        "1",
        ex=ttl_seconds,
        nx=True,
    )
    return bool(result)


async def check_rate_limit(
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
    user_id: int,
    now_utc: Optional[datetime] = None,
) -> Optional[Dict[str, int | str]]:
    redis_client = context.application.bot_data.get("redis")
    if redis_client is None:
        raise RuntimeError("Redis client is not configured")

    current_time = now_utc or datetime.now(timezone.utc)
    now_ts = int(current_time.timestamp())

    for window_name, window_seconds, limit in _get_rate_limit_rules(action):
        bucket = _get_rate_limit_bucket(now_ts, window_seconds)
        key = get_rate_limit_key(action, user_id, window_name, bucket)
        count = int(await redis_client.incr(key))
        if count == 1:
            await redis_client.expire(key, window_seconds)
        if count <= limit:
            continue

        retry_after = int(await redis_client.ttl(key))
        if retry_after < 0:
            retry_after = window_seconds
        return {
            "action": action,
            "window_name": window_name,
            "limit": limit,
            "count": count,
            "retry_after_seconds": retry_after,
        }

    return None


async def enforce_command_lock(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
    user_id: int,
) -> bool:
    chat_id = get_update_chat_id(update)
    try:
        acquired = await acquire_command_lock(context, action, user_id)
    except Exception as e:
        if chat_id is not None:
            await notify_error(context, chat_id, f"Failed to acquire /{action} lock", e)
        _log_bot_event(
            "command_lock_failed",
            level=logging.ERROR,
            message=f"command_lock_failed action={action}",
            exc_info=True,
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        return False

    if acquired:
        return True

    _log_bot_event(
        "command_locked",
        message=f"command_locked action={action}",
        user_id=user_id,
        telegram_chat_id=chat_id,
    )
    await reply(update, context, "This command is already in progress. Try again in a few seconds 🙂")
    return False


async def enforce_request_dedupe(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> bool:
    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None
    request_id = get_request_id(update)

    try:
        accepted = await acquire_request_dedupe(context, action, request_id)
    except Exception as e:
        if chat_id is not None:
            await notify_error(context, chat_id, f"Failed to dedupe /{action} request", e)
        _log_bot_event(
            "request_dedupe_failed",
            level=logging.ERROR,
            message=f"request_dedupe_failed action={action}",
            exc_info=True,
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        return False

    if accepted:
        return True

    _log_bot_event(
        "request_deduped",
        message=f"request_deduped action={action}",
        user_id=user_id,
        telegram_chat_id=chat_id,
    )
    if update.callback_query is not None:
        await update.callback_query.answer("Already processing 🙂", show_alert=False)
    else:
        await reply(update, context, "Already processing 🙂")
    return False


async def enforce_rate_limit(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
    user_id: int,
) -> bool:
    chat_id = get_update_chat_id(update)
    try:
        breach = await check_rate_limit(context, action, user_id)
    except Exception as e:
        if chat_id is not None:
            await notify_error(context, chat_id, f"Failed to apply /{action} rate limit", e)
        _log_bot_event(
            "rate_limit_failed",
            level=logging.ERROR,
            message=f"rate_limit_failed action={action}",
            exc_info=True,
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        return False

    if breach is None:
        return True

    retry_after_seconds = int(breach["retry_after_seconds"])
    record_rate_limit_hit(action)
    _log_bot_event(
        "rate_limited",
        message=(
            f"rate_limited action={action} window={breach['window_name']} "
            f"limit={breach['limit']} count={breach['count']}"
        ),
        user_id=user_id,
        telegram_chat_id=chat_id,
    )
    await reply(
        update,
        context,
        (
            f"Rate limit reached for /{action}. "
            f"Try again in {_format_retry_after(retry_after_seconds)}."
        ),
    )
    return False

def parse_time_hhmm(value: str) -> dt_time:
    # Accept strict HH:MM 24-hour format only.
    value = value.strip()
    if len(value) != 5 or value[2] != ":":
        raise ValueError("Time must be HH:MM (24h), e.g. 07:30")
    parsed = dt_time.fromisoformat(value)
    if parsed.second != 0 or parsed.microsecond != 0:
        raise ValueError("Time must be HH:MM (24h), e.g. 07:30")
    return parsed


def get_optional_env_int(name: str) -> Optional[int]:
    v = os.getenv(name)
    if v is None or not v.strip():
        return None
    return int(v)


def get_update_chat_id(update: Update) -> Optional[int]:
    return update.effective_chat.id if update.effective_chat is not None else None


def get_request_id(update: Update) -> str:
    # Deterministic request id to dedupe retried Telegram updates/commands.
    env = os.getenv("ENVIRONMENT", "dev")  # or "prod"
    scope = f"env:{env}"

    msg = getattr(update, "effective_message", None)
    if msg is not None and msg.message_id is not None:
        chat_id = getattr(update.effective_chat, "id", None)
        return str(uuid5(NAMESPACE_URL, f"{scope}:telegram-msg:{chat_id}:{msg.message_id}"))

    if update.update_id is not None:
        return str(uuid5(NAMESPACE_URL, f"{scope}:telegram-update:{update.update_id}"))

    # Fallback means: we cannot dedupe this request reliably
    return str(uuid4())


def _is_admin_override_chat(update: Update, admin_chat_id_override: Optional[int]) -> bool:
    chat_id = get_update_chat_id(update)
    return admin_chat_id_override is not None and chat_id == admin_chat_id_override


def register_user_from_update(update: Update) -> Optional[Dict[str, Any]]:
    # Register/update Telegram identity in DB and ensure defaults in app.user_settings.
    if update.effective_user is None or update.effective_chat is None:
        _log_bot_event("user_registration_skipped", level=logging.WARNING)
        return None
    user = upsert_user(
        telegram_user_id=update.effective_user.id,
        telegram_chat_id=update.effective_chat.id,
        username=update.effective_user.username,
    )
    ensure_user_settings(user["id"])
    _log_bot_event(
        "user_registered",
        user_id=user["id"],
        telegram_chat_id=update.effective_chat.id,
    )
    return user


async def require_allowlisted_user(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> Optional[Dict[str, Any]]:
    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None
    admin_chat_id_override = context.application.bot_data["admin_chat_id_override"]

    try:
        user = register_user_from_update(update)
    except Exception as e:
        if chat_id is not None:
            await notify_error(context, chat_id, f"Failed to register user for {action}", e)
        _log_bot_event(
            "user_registration_failed",
            level=logging.ERROR,
            message=f"user_registration_failed action={action}",
            exc_info=True,
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        return None

    if user is None:
        return None

    if _is_admin_override_chat(update, admin_chat_id_override):
        _log_bot_event(
            "access_granted_admin_override",
            message=f"access_granted_admin_override action={action}",
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        return user

    if not bool(user.get("allowlisted")):
        _log_bot_event(
            "access_denied_not_allowlisted",
            message=f"access_denied_not_allowlisted action={action} status={user.get('status')}",
            user_id=user.get("id"),
            telegram_chat_id=chat_id,
        )
        await reply(update, context, "Registered. Waiting for approval.")
        return None

    _log_bot_event(
        "access_granted",
        message=f"access_granted action={action} status={user.get('status')}",
        user_id=user.get("id"),
        telegram_chat_id=chat_id,
    )
    return user


async def require_admin_override(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> bool:
    admin_chat_id_override = context.application.bot_data["admin_chat_id_override"]
    chat_id = get_update_chat_id(update)
    user_id = update.effective_user.id if update.effective_user else None

    if admin_chat_id_override is None:
        _log_bot_event(
            "admin_command_denied_unconfigured",
            level=logging.WARNING,
            message=f"admin_command_denied_unconfigured action={action}",
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        await reply(update, context, "Admin commands are disabled: ALLOWED_CHAT_ID is not configured.")
        return False

    if not _is_admin_override_chat(update, admin_chat_id_override):
        _log_bot_event(
            "admin_command_denied_unauthorized",
            level=logging.WARNING,
            message=f"admin_command_denied_unauthorized action={action}",
            user_id=user_id,
            telegram_chat_id=chat_id,
        )
        await reply(update, context, "This command is admin-only.")
        return False

    _log_bot_event(
        "admin_command_allowed",
        message=f"admin_command_allowed action={action}",
        user_id=user_id,
        telegram_chat_id=chat_id,
    )
    return True

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Log exceptions from handlers/jobs.
    _log_bot_event("handler_error", level=logging.ERROR, exc_info=context.error)

async def notify_error(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    title: str,
    exc: Exception,
) -> None:
    """
    Sends a user-friendly error message to Telegram and logs details.
    """
    error_id = uuid4().hex[:8]
    _log_bot_event(
        "notify_error",
        level=logging.ERROR,
        message=f"{title} [error_id={error_id}]",
        exc_info=exc,
        telegram_chat_id=chat_id,
    )

    # Do not expose raw exception details in user-visible messages.
    msg = (
        f"❌ {title}\n\n"
        "Internal error occurred. Check service logs for details.\n"
        f"error_id: {error_id}"
    )

    if is_auth_error(exc):
        msg = f"{msg}\n\n{format_auth_help()}"

    await context.bot.send_message(chat_id=chat_id, text=msg)

async def reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None) -> None:
    # Reply either to a normal message (/command) or to a callback query (button press).
    if update.message is not None:
        await update.message.reply_text(text=text, reply_markup=reply_markup)
        return

    if update.callback_query is not None and update.callback_query.message is not None:
        await update.callback_query.message.reply_text(text=text, reply_markup=reply_markup)
        return

    # Fallback: no route to reply.
    _log_bot_event("reply_skipped", level=logging.WARNING)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        chat_id = get_update_chat_id(update)
        user_id = update.effective_user.id if update.effective_user else None
        if chat_id is None:
            _log_bot_event("start_missing_chat", level=logging.WARNING, user_id=user_id)
            status = "rejected"
            return

        _log_bot_event("command_start", user_id=user_id, telegram_chat_id=chat_id)
        try:
            user = register_user_from_update(update)
        except Exception as e:
            await notify_error(context, chat_id, "Failed to register user", e)
            status = "error"
            return
        if user is None:
            status = "rejected"
            return

        if not bool(user.get("allowlisted")):
            await reply(update, context, "Registered. Waiting for approval.")
            status = "rejected"
            return

        await reply(
            update,
            context,
            "Welcome, you're active. Use /settime and /settz.",
            reply_markup=build_keyboard(None),
        )
        status = "success"
    finally:
        record_command("start", status)


async def cmd_approve(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        if not await require_admin_override(update, context, "approve"):
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return
        if not context.args:
            await reply(update, context, "Usage: /approve <telegram_user_id>")
            status = "rejected"
            return
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await reply(update, context, "Invalid user id. Usage: /approve <telegram_user_id>")
            status = "rejected"
            return

        try:
            row = approve_user(target_user_id)
        except Exception as e:
            await notify_error(context, chat_id, f"Failed to approve user {target_user_id}", e)
            status = "error"
            return
        if row is None:
            await reply(update, context, f"User not found: telegram_user_id={target_user_id}")
            status = "rejected"
            return

        _log_bot_event(
            "user_approved",
            user_id=row["id"],
            telegram_chat_id=row["telegram_chat_id"],
        )

        await reply(
            update,
            context,
            (
                f"✅ Approved telegram_user_id={row['telegram_user_id']}\n"
                f"status={row['status']} allowlisted={row['allowlisted']}"
            ),
        )
        status = "success"
    finally:
        record_command("approve", status)


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        if not await require_admin_override(update, context, "block"):
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return
        if not context.args:
            await reply(update, context, "Usage: /block <telegram_user_id>")
            status = "rejected"
            return
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await reply(update, context, "Invalid user id. Usage: /block <telegram_user_id>")
            status = "rejected"
            return

        try:
            row = block_user(target_user_id)
        except Exception as e:
            await notify_error(context, chat_id, f"Failed to block user {target_user_id}", e)
            status = "error"
            return
        if row is None:
            await reply(update, context, f"User not found: telegram_user_id={target_user_id}")
            status = "rejected"
            return

        _log_bot_event(
            "user_blocked",
            user_id=row["id"],
            telegram_chat_id=row["telegram_chat_id"],
        )

        await reply(
            update,
            context,
            (
                f"⛔ Blocked telegram_user_id={row['telegram_user_id']}\n"
                f"status={row['status']} allowlisted={row['allowlisted']}"
            ),
        )
        status = "success"
    finally:
        record_command("block", status)


async def cmd_admin_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        if not await require_admin_override(update, context, "admin_status"):
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return

        try:
            snapshot = get_admin_status_snapshot(pending_limit=20)
        except Exception as e:
            await notify_error(context, chat_id, "Failed to load admin status", e)
            status = "error"
            return

        _log_bot_event("admin_status_requested", telegram_chat_id=chat_id)

        pending_users = snapshot["pending_users"]
        lines = ["Pending users:"]
        if pending_users:
            for row in pending_users:
                lines.append(
                    f"- tg_user={row['telegram_user_id']} chat={row['telegram_chat_id']} created={row['created_at']} username={row['username']}"
                )
        else:
            lines.append("- none")

        lines.extend(
            [
                "",
                f"Queued jobs count: {snapshot['queued_jobs_count']}",
                f"Running jobs count: {snapshot['running_jobs_count']}",
                f"Failed/dead jobs count: {snapshot['failed_dead_jobs_count']}",
                "",
                "Last delivery per user:",
            ]
        )

        last_deliveries = snapshot["last_delivery_per_user"]
        if last_deliveries:
            for row in last_deliveries:
                delivered_at = row.get("delivered_at")
                delivered_text = _fmt_ts(delivered_at, context.application.bot_data["tz"]) if delivered_at else "n/a"
                lines.append(
                    f"- user={row['user_id']} tg_user={row['telegram_user_id']} chat={row['telegram_chat_id']} "
                    f"last={delivered_text} album={row.get('album_id') or 'n/a'} cycle={row.get('cycle_number') or 'n/a'}"
                )
        else:
            lines.append("- none")

        await reply(update, context, "\n".join(lines))
        status = "success"
    finally:
        record_command("admin_status", status)


async def cmd_settz(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "settz")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return
        if not context.args:
            await reply(update, context, "Usage: /settz <IANA_TZ>\nExample: /settz Europe/Riga")
            status = "rejected"
            return

        tz_name = context.args[0].strip()
        try:
            ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            await reply(
                update,
                context,
                "Invalid timezone. Use IANA format, for example: Europe/Riga",
            )
            status = "rejected"
            return

        try:
            settings = set_user_timezone(user["id"], tz_name)
        except Exception as e:
            await notify_error(context, chat_id, "Failed to save timezone", e)
            status = "error"
            return

        _log_bot_event(
            "schedule_updated",
            message=f"schedule_updated field=timezone timezone={settings['timezone']}",
            user_id=user["id"],
            telegram_chat_id=chat_id,
        )

        await reply(
            update,
            context,
            f"✅ Timezone saved: {settings['timezone']}\nDaily time: {settings['daily_time_local']}",
        )
        status = "success"
    finally:
        record_command("settz", status)


async def cmd_settime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "settime")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return
        if not context.args:
            await reply(update, context, "Usage: /settime <HH:MM>\nExample: /settime 07:30")
            status = "rejected"
            return

        raw_time = context.args[0].strip()
        try:
            parsed_time = parse_time_hhmm(raw_time)
        except ValueError:
            await reply(update, context, "Invalid time. Use HH:MM (24h), for example: 07:30")
            status = "rejected"
            return

        try:
            settings = set_user_daily_time(user["id"], parsed_time)
        except Exception as e:
            await notify_error(context, chat_id, "Failed to save daily time", e)
            status = "error"
            return

        _log_bot_event(
            "schedule_updated",
            message=f"schedule_updated field=daily_time_local daily_time_local={settings['daily_time_local']}",
            user_id=user["id"],
            telegram_chat_id=chat_id,
        )

        await reply(
            update,
            context,
            f"✅ Daily time saved: {settings['daily_time_local']}\nTimezone: {settings['timezone']}",
        )
        status = "success"
    finally:
        record_command("settime", status)


async def cmd_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "now")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return
        if not await enforce_request_dedupe(update, context, "now"):
            status = "rejected"
            return
        if not await enforce_rate_limit(update, context, "now", int(user["id"])):
            status = "rejected"
            return
        if not await enforce_command_lock(update, context, "now", int(user["id"])):
            status = "rejected"
            return

        _log_bot_event("command_now", user_id=user["id"], telegram_chat_id=chat_id)
        idem_key = f"manual:{user['id']}:{get_request_id(update)}"

        try:
            now_utc = datetime.now(timezone.utc)
            row = enqueue_job_once(
                idempotency_key=idem_key,
                idempotency_expires_at=now_utc + timedelta(days=2),
                job_id=uuid4(),
                user_id=user["id"],
                job_type=JOB_TYPE_DELIVER_NOW,
                run_at=now_utc,
                payload={
                    "telegram_chat_id": chat_id,
                    "idempotency_key": idem_key,
                },
            )
            if row is not None:
                _log_bot_event(
                    "manual_delivery_requested",
                    user_id=user["id"],
                    telegram_chat_id=chat_id,
                    job_id=row.get("id"),
                    job_type=JOB_TYPE_DELIVER_NOW,
                    attempt=row.get("attempt"),
                    idempotency_key=idem_key,
                )
                await reply(update, context, "Queued ✅")
                status = "success"
            else:
                _log_bot_event(
                    "manual_delivery_requested",
                    message="manual_delivery_requested duplicate=true",
                    user_id=user["id"],
                    telegram_chat_id=chat_id,
                    job_type=JOB_TYPE_DELIVER_NOW,
                    idempotency_key=idem_key,
                )
                await reply(update, context, "Expect the previous album to arrive soon 🙂")
                status = "rejected"
        except Exception as e:
            await notify_error(context, chat_id, "Failed to queue /now delivery", e)
            status = "error"
            return
    finally:
        record_command("now", status)


async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "refresh")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return
        if not await enforce_request_dedupe(update, context, "refresh"):
            status = "rejected"
            return
        if not await enforce_rate_limit(update, context, "refresh", int(user["id"])):
            status = "rejected"
            return
        if not await enforce_command_lock(update, context, "refresh", int(user["id"])):
            status = "rejected"
            return

        _log_bot_event("command_refresh", user_id=user["id"], telegram_chat_id=chat_id)

        provider_account = get_active_user_provider_account(user["id"])
        if provider_account is None:
            await reply(update, context, "No active provider account is configured yet.")
            status = "rejected"
            return
        if str(provider_account.get("status") or "") != PROVIDER_ACCOUNT_STATUS_CONNECTED:
            await reply(
                update,
                context,
                f"Active provider cannot sync right now (status={provider_account.get('status')}).",
            )
            status = "rejected"
            return

        idem_key = f"sync:{provider_account['id']}:{get_request_id(update)}"
        try:
            now_utc = datetime.now(timezone.utc)
            row = enqueue_job_once(
                idempotency_key=idem_key,
                idempotency_expires_at=now_utc + timedelta(minutes=30),
                job_id=uuid4(),
                user_id=user["id"],
                job_type=JOB_TYPE_SYNC_LIBRARY,
                run_at=now_utc,
                payload={
                    "telegram_chat_id": chat_id,
                    "idempotency_key": idem_key,
                    "user_provider_account_id": int(provider_account["id"]),
                    "provider": provider_account["provider"],
                },
            )
            if row is not None:
                await reply(update, context, "Queued ✅ Library sync will run soon", reply_markup=build_keyboard(None))
                status = "success"
            else:
                await reply(update, context, "A sync is already queued or running for your library 🙂")
                status = "rejected"
        except Exception as e:
            await notify_error(context, chat_id, "Failed to queue library sync", e)
            status = "error"
    finally:
        record_command("refresh", status)


async def cmd_provider(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "provider")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return

        try:
            accounts = list_user_provider_accounts(user["id"])
        except Exception as e:
            await notify_error(context, chat_id, "Failed to load provider accounts", e)
            status = "error"
            return

        if not accounts:
            await reply(update, context, "No provider accounts are configured yet.")
            status = "rejected"
            return

        if not context.args:
            lines = ["Providers:"]
            for account in accounts:
                marker = "*" if account.get("is_active") else "-"
                lines.append(f"{marker} {account.get('provider')} status={account.get('status')}")
            lines.append("")
            lines.append("Usage: /provider <provider_name>")
            await reply(update, context, "\n".join(lines))
            status = "success"
            return

        target_provider = context.args[0].strip().lower()
        try:
            account = set_active_user_provider_account(user["id"], target_provider)
        except Exception as e:
            await notify_error(context, chat_id, "Failed to switch provider", e)
            status = "error"
            return

        if account is None:
            await reply(update, context, f"Provider not found: {target_provider}")
            status = "rejected"
            return

        await reply(
            update,
            context,
            f"✅ Active provider set to {account['provider']} (status={account['status']})",
        )
        status = "success"
    finally:
        record_command("provider", status)


async def cmd_connect_ytmusic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "connect_ytmusic")
        if user is None:
            status = "rejected"
            return
        await reply(
            update,
            context,
            "YT Music connection is still manual in Phase 2.\nSend the credential blob to the admin so it can be stored for your account.",
        )
        status = "success"
    finally:
        record_command("connect_ytmusic", status)


async def cmd_connect_spotify(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "connect_spotify")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return

        client_id = os.getenv("SPOTIFY_CLIENT_ID", "").strip()
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "").strip()
        if not client_id or not redirect_uri:
            await reply(
                update,
                context,
                "Spotify OAuth is not configured yet. Set SPOTIFY_CLIENT_ID and SPOTIFY_REDIRECT_URI.",
            )
            status = "rejected"
            return

        state = generate_oauth_state()
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=get_spotify_oauth_state_ttl_seconds())
        try:
            create_oauth_session(
                user_id=int(user["id"]),
                provider="spotify",
                state=state,
                requested_chat_id=chat_id,
                expires_at=expires_at,
            )
        except Exception as e:
            await notify_error(context, chat_id, "Failed to start Spotify OAuth", e)
            status = "error"
            return

        authorize_url = build_spotify_authorize_url(
            client_id=client_id,
            redirect_uri=redirect_uri,
            state=state,
        )
        await reply(
            update,
            context,
            (
                "Open this link to connect Spotify:\n"
                f"{authorize_url}\n\n"
                "This link is single-use and expires soon."
            ),
        )
        status = "success"
    finally:
        record_command("connect_spotify", status)


async def cmd_nextcycle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "nextcycle")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return
        if not await enforce_request_dedupe(update, context, "nextcycle"):
            status = "rejected"
            return
        if not await enforce_rate_limit(update, context, "nextcycle", int(user["id"])):
            status = "rejected"
            return
        if not await enforce_command_lock(update, context, "nextcycle", int(user["id"])):
            status = "rejected"
            return

        _log_bot_event("command_nextcycle", user_id=user["id"], telegram_chat_id=chat_id)
        idem_key = f"nextcycle:{user['id']}:{get_request_id(update)}"
        try:
            now_utc = datetime.now(timezone.utc)
            row = enqueue_job_once(
                idempotency_key=idem_key,
                idempotency_expires_at=now_utc + timedelta(days=2),
                job_id=uuid4(),
                user_id=user["id"],
                job_type=JOB_TYPE_NEXT_CYCLE_NOW,
                run_at=now_utc,
                payload={
                    "telegram_chat_id": chat_id,
                    "idempotency_key": idem_key,
                    "force_next_cycle": True,
                },
            )
            if row is not None:
                _log_bot_event(
                    "manual_delivery_requested",
                    message="manual_delivery_requested force_next_cycle=true",
                    user_id=user["id"],
                    telegram_chat_id=chat_id,
                    job_id=row.get("id"),
                    job_type=JOB_TYPE_NEXT_CYCLE_NOW,
                    attempt=row.get("attempt"),
                    idempotency_key=idem_key,
                )
                await reply(update, context, "Queued ✅ New cycle album will arrive soon")
                status = "success"
            else:
                _log_bot_event(
                    "manual_delivery_requested",
                    message="manual_delivery_requested duplicate=true force_next_cycle=true",
                    user_id=user["id"],
                    telegram_chat_id=chat_id,
                    job_type=JOB_TYPE_NEXT_CYCLE_NOW,
                    idempotency_key=idem_key,
                )
                await reply(update, context, "Expect the previous new-cycle album to arrive soon 🙂")
                status = "rejected"
        except Exception as e:
            await notify_error(context, chat_id, "Failed to queue /nextcycle delivery", e)
            status = "error"
            return
    finally:
        record_command("nextcycle", status)

def _fmt_ts(ts: Optional[Any], tz: ZoneInfo) -> str:
    if not ts:
        return "n/a"
    if isinstance(ts, (int, float)):
        ts = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError:
            return ts
    if not isinstance(ts, datetime):
        return str(ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    dt = ts.astimezone(tz)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def resolve_app_timezone(
    admin_chat_id_override: Optional[int],
    default_timezone_name: str = "UTC",
) -> ZoneInfo:
    timezone_name = default_timezone_name

    if admin_chat_id_override is not None:
        db_timezone_name = get_user_timezone_by_chat_id(admin_chat_id_override)
        if db_timezone_name:
            timezone_name = db_timezone_name

    return ZoneInfo(timezone_name)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    status = "error"
    try:
        user = await require_allowlisted_user(update, context, "status")
        if user is None:
            status = "rejected"
            return
        chat_id = get_update_chat_id(update)
        if chat_id is None:
            status = "rejected"
            return

        _log_bot_event("command_status", user_id=user["id"], telegram_chat_id=chat_id)

        tz = context.application.bot_data["tz"]
        try:
            provider_account = get_active_user_provider_account(user["id"])
            settings = get_user_settings(user["id"])
            db_stats = get_user_delivery_stats(user["id"])
            recent_deliveries = list_recent_deliveries(user["id"], limit=5)
        except Exception as e:
            await notify_error(context, chat_id, "Failed to load DB status", e)
            status = "error"
            return

        sync_state = None
        if provider_account is not None:
            try:
                sync_state = get_user_provider_sync_state(int(provider_account["id"]))
            except Exception as e:
                await notify_error(context, chat_id, "Failed to load provider sync status", e)
                status = "error"
                return

        _log_bot_event(
            "status_snapshot",
            message="status_snapshot",
            user_id=user["id"],
            telegram_chat_id=chat_id,
        )

        msg_lines = [
            "DB user:",
            f"Access: allowlisted={user.get('allowlisted')} status={user.get('status')}",
            f"Timezone: {settings.get('timezone')}",
            f"Daily time: {settings.get('daily_time_local')}",
            f"Active provider: {(provider_account or {}).get('provider') or 'n/a'}",
            f"Provider status: {(provider_account or {}).get('status') or 'n/a'}",
            f"Last sync result: {(sync_state or {}).get('last_sync_result') or 'n/a'}",
            f"Last sync: {_fmt_ts((sync_state or {}).get('last_successful_sync_at'), tz)}",
            f"Last sync error: {(sync_state or {}).get('last_error') or 'n/a'}",
            f"Cached albums: {(sync_state or {}).get('library_item_count') or 0}",
            f"DB deliveries total: {db_stats.get('total_deliveries')}",
            f"DB latest cycle: {db_stats.get('latest_cycle_number') or 'n/a'}",
            f"DB sent in latest cycle: {db_stats.get('latest_cycle_count')}",
            f"DB last delivered: {_fmt_ts(db_stats.get('last_delivered_at'), tz)}",
            "",
            "Recent DB deliveries:",
        ]
        if recent_deliveries:
            for row in recent_deliveries:
                msg_lines.append(
                    f"- {_fmt_ts(row.get('delivered_at'), tz)} album={row.get('album_id')} cycle={row.get('cycle_number')}"
                )
        else:
            msg_lines.append("- none")

        await reply(update, context, "\n".join(msg_lines), reply_markup=build_keyboard(None))
        status = "success"
    finally:
        record_command("status", status)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    # Acknowledge callback immediately so Telegram stops showing the loading spinner.
    # In throttled branches we answer again with a short toast for user feedback.
    await query.answer()

    data = query.data
    user = await require_allowlisted_user(update, context, f"callback:{data or 'unknown'}")
    if user is None:
        return
    chat_id = get_update_chat_id(update)
    if chat_id is None:
        return
    _log_bot_event(
        "callback_received",
        message=f"callback_received action={data}",
        user_id=user.get("id"),
        telegram_chat_id=chat_id,
    )
    if data == CB_NEXT:
        # Reuse the /now logic.
        fake_update = update
        await cmd_now(fake_update, context)
        return

    if data == CB_REFRESH:
        await cmd_refresh(update, context)
        return

    if data == CB_STATUS:
        await cmd_status(update, context)
        return


def main() -> None:
    load_dotenv()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    admin_chat_id_override = get_optional_env_int("ALLOWED_CHAT_ID")
    default_timezone_name = os.getenv("DEFAULT_TIMEZONE", "UTC").strip() or "UTC"
    tz = resolve_app_timezone(admin_chat_id_override, default_timezone_name)

    library_limit = int(os.getenv("LIBRARY_LIMIT", "500"))
    redis_url = get_env_str("REDIS_URL", "redis://localhost:6379/0")
    http_host = get_env_str("HTTP_HOST", "0.0.0.0")
    http_port = get_env_int("HTTP_PORT", 8080)
    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    log_level = getattr(logging, log_level_name, None)
    if not isinstance(log_level, int):
        raise RuntimeError(f"Invalid LOG_LEVEL: {log_level_name}")
    if redis is None:
        raise RuntimeError("redis package is not installed")

    configure_logging(log_level)
    start_metrics_server(get_optional_env_int("PROMETHEUS_METRICS_PORT") or 8000)
    http_server = start_http_server(http_host, http_port)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    _log_bot_event(
        "bot_started",
        message=(
            f"bot_started tz={tz.key} "
            f"library_limit={library_limit} allowed_chat_id={admin_chat_id_override} "
            f"http={http_host}:{http_port}"
        ),
    )

    app = Application.builder().token(token).defaults(Defaults(tzinfo=tz)).build()
    redis_client = redis.from_url(redis_url, decode_responses=True)

    # Store config in bot_data so handlers/jobs can access it.
    app.bot_data["admin_chat_id_override"] = admin_chat_id_override
    app.bot_data["library_limit"] = library_limit
    app.bot_data["tz"] = tz
    app.bot_data["redis"] = redis_client

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("approve", cmd_approve))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("admin_status", cmd_admin_status))
    app.add_handler(CommandHandler("settz", cmd_settz))
    app.add_handler(CommandHandler("settime", cmd_settime))
    app.add_handler(CommandHandler("provider", cmd_provider))
    app.add_handler(CommandHandler("connect_ytmusic", cmd_connect_ytmusic))
    app.add_handler(CommandHandler("connect_spotify", cmd_connect_spotify))
    app.add_handler(CommandHandler("now", cmd_now))
    app.add_handler(CommandHandler("nextcycle", cmd_nextcycle))
    app.add_handler(CommandHandler("refresh", cmd_refresh))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_error_handler(on_error)

    # Buttons (callback queries)
    app.add_handler(CallbackQueryHandler(on_callback))

    # Start polling (no public IP required)
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        http_server.shutdown()


if __name__ == "__main__":
    main()
