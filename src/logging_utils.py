import json
import logging
from datetime import datetime, timezone
from typing import Any


LOG_FIELDS = (
    "event",
    "job_id",
    "job_type",
    "user_id",
    "telegram_chat_id",
    "attempt",
    "idempotency_key",
    "worker_id",
    "provider",
    "user_provider_account_id",
    "sync_job_id",
    "provider_status",
    "sync_result",
    "rate_limited",
)


def _serialize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in LOG_FIELDS:
            payload[field] = _serialize(getattr(record, field, None))

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        return json.dumps(payload, ensure_ascii=True)


def configure_logging(level: int) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    message: str | None = None,
    exc_info: Any = None,
    stack_info: bool = False,
    **fields: Any,
) -> None:
    extra = {field: fields.get(field) for field in LOG_FIELDS}
    extra["event"] = event
    logger.log(level, message or event, extra=extra, exc_info=exc_info, stack_info=stack_info)
