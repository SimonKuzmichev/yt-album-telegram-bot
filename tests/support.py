import sys
from types import ModuleType, SimpleNamespace


def install_module_stubs() -> None:
    if "dotenv" not in sys.modules:
        dotenv = ModuleType("dotenv")
        dotenv.load_dotenv = lambda: None
        sys.modules["dotenv"] = dotenv

    if "fastapi" not in sys.modules:
        fastapi = ModuleType("fastapi")

        class _FastAPI:
            def get(self, *_args, **_kwargs):
                def decorator(func):
                    return func

                return decorator

        fastapi.FastAPI = _FastAPI
        sys.modules["fastapi"] = fastapi

    if "fastapi.responses" not in sys.modules:
        fastapi_responses = ModuleType("fastapi.responses")
        fastapi_responses.HTMLResponse = type("HTMLResponse", (), {})
        fastapi_responses.PlainTextResponse = type("PlainTextResponse", (), {})
        sys.modules["fastapi.responses"] = fastapi_responses

    if "telegram" not in sys.modules:
        telegram = ModuleType("telegram")
        telegram.Update = type("Update", (), {"ALL_TYPES": object()})
        telegram.Bot = type("Bot", (), {})
        sys.modules["telegram"] = telegram

    if "telegram.ext" not in sys.modules:
        telegram_ext = ModuleType("telegram.ext")

        class _Application:
            @classmethod
            def builder(cls):
                return cls()

            def token(self, _value):
                return self

            def defaults(self, _value):
                return self

            def build(self):
                return self

        telegram_ext.Application = _Application
        telegram_ext.CallbackQueryHandler = type("CallbackQueryHandler", (), {})
        telegram_ext.CommandHandler = type("CommandHandler", (), {})
        telegram_ext.ContextTypes = SimpleNamespace(DEFAULT_TYPE=object)
        telegram_ext.Defaults = type("Defaults", (), {})
        sys.modules["telegram.ext"] = telegram_ext

    if "src.errors" not in sys.modules:
        errors = ModuleType("src.errors")
        errors.is_auth_error = lambda exc: False
        errors.is_rate_limited = lambda exc: False
        errors.format_auth_help = lambda: "auth help"
        sys.modules["src.errors"] = errors

    if "src.logging_utils" not in sys.modules:
        logging_utils = ModuleType("src.logging_utils")
        logging_utils.configure_logging = lambda *args, **kwargs: None
        logging_utils.log_event = lambda *args, **kwargs: None
        sys.modules["src.logging_utils"] = logging_utils

    if "src.db" not in sys.modules:
        db = ModuleType("src.db")
        db.OAUTH_SESSION_STATUS_CONSUMED = "consumed"
        db.OAUTH_SESSION_STATUS_EXPIRED = "expired"
        db.OAUTH_SESSION_STATUS_FAILED = "failed"
        db.OAUTH_SESSION_STATUS_PENDING = "pending"
        db.PROVIDER_ACCOUNT_STATUS_CONNECTED = "connected"
        db.PROVIDER_ACCOUNT_STATUS_NEEDS_REAUTH = "needs_reauth"
        db.SYNC_RESULT_AUTH_ERROR = "auth_error"
        db.SYNC_RESULT_EMPTY_LIBRARY = "empty_library"
        db.SYNC_RESULT_OK = "ok"
        db.SYNC_RESULT_TRANSIENT_ERROR = "transient_error"
        for name in [
            "approve_user",
            "block_user",
            "claim_runnable_jobs",
            "create_oauth_session",
            "enqueue_job_once",
            "ensure_user_settings",
            "get_active_user_provider_account",
            "get_admin_status_snapshot",
            "get_metrics_snapshot",
            "get_oauth_session_by_state",
            "get_user_provider_sync_state",
            "get_latest_cycle_number",
            "get_user_provider_account_by_id",
            "get_user_provider_account_credentials",
            "get_user_delivery_stats",
            "get_user_settings",
            "get_user_timezone_by_chat_id",
            "insert_delivery_history",
            "list_user_provider_accounts",
            "list_active_users_with_delivery_context",
            "list_available_user_library_albums",
            "list_cycle_album_ids",
            "list_provider_accounts_due_for_sync",
            "list_provider_accounts_needing_token_refresh",
            "list_recent_deliveries",
            "mark_job_failed",
            "mark_job_succeeded",
            "mark_user_provider_account_status",
            "mark_user_provider_sync_failed",
            "mark_user_provider_sync_started",
            "mark_user_provider_sync_succeeded",
            "requeue_stale_running_jobs",
            "set_active_user_provider_account",
            "set_user_daily_time",
            "set_user_timezone",
            "update_oauth_session_status",
            "upsert_user_provider_account_credentials",
            "upsert_user_library_albums",
            "upsert_user",
        ]:
            setattr(db, name, lambda *args, **kwargs: None)
        sys.modules["src.db"] = db

    if "src.telegram_delivery" not in sys.modules:
        telegram_delivery = ModuleType("src.telegram_delivery")
        telegram_delivery.CB_NEXT = "next"
        telegram_delivery.CB_REFRESH = "refresh"
        telegram_delivery.CB_STATUS = "status"
        telegram_delivery.build_keyboard = lambda *args, **kwargs: None
        telegram_delivery.send_album_message = lambda *args, **kwargs: None
        sys.modules["src.telegram_delivery"] = telegram_delivery

    if "uvicorn" not in sys.modules:
        uvicorn = ModuleType("uvicorn")

        class _Config:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        class _Server:
            def __init__(self, config):
                self.config = config
                self.should_exit = False

            def run(self):
                return None

        uvicorn.Config = _Config
        uvicorn.Server = _Server
        sys.modules["uvicorn"] = uvicorn
