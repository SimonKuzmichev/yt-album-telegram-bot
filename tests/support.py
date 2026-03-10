import sys
from types import ModuleType, SimpleNamespace


def install_module_stubs() -> None:
    if "dotenv" not in sys.modules:
        dotenv = ModuleType("dotenv")
        dotenv.load_dotenv = lambda: None
        sys.modules["dotenv"] = dotenv

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
        errors.format_auth_help = lambda: "auth help"
        sys.modules["src.errors"] = errors

    if "src.logging_utils" not in sys.modules:
        logging_utils = ModuleType("src.logging_utils")
        logging_utils.configure_logging = lambda *args, **kwargs: None
        logging_utils.log_event = lambda *args, **kwargs: None
        sys.modules["src.logging_utils"] = logging_utils

    if "src.db" not in sys.modules:
        db = ModuleType("src.db")
        for name in [
            "approve_user",
            "block_user",
            "claim_runnable_jobs",
            "enqueue_job_once",
            "ensure_user_settings",
            "get_admin_status_snapshot",
            "get_latest_cycle_number",
            "get_user_delivery_stats",
            "get_user_settings",
            "get_user_timezone_by_chat_id",
            "insert_delivery_history",
            "list_active_users_with_settings",
            "list_cycle_album_ids",
            "list_recent_deliveries",
            "mark_job_failed",
            "mark_job_succeeded",
            "requeue_stale_running_jobs",
            "set_user_daily_time",
            "set_user_timezone",
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
