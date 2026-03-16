import logging
import os
import re
from typing import Any

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except ModuleNotFoundError:  # pragma: no cover - exercised only when dependency is absent
    class _NoopMetric:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, amount: float = 1.0) -> None:
            return None

        def observe(self, value: float) -> None:
            return None

        def set(self, value: float) -> None:
            return None

        def clear(self) -> None:
            return None

    def Counter(*args, **kwargs):  # type: ignore[misc]
        return _NoopMetric()

    def Gauge(*args, **kwargs):  # type: ignore[misc]
        return _NoopMetric()

    def Histogram(*args, **kwargs):  # type: ignore[misc]
        return _NoopMetric()

    def start_http_server(*args, **kwargs) -> None:
        return None


logger = logging.getLogger(__name__)


album_deliveries_total = Counter(
    "album_deliveries_total",
    "Album delivery outcomes by provider and status.",
    ["provider", "status"],
)
delivery_attempts_total = Counter(
    "delivery_attempts_total",
    "Album delivery attempts by provider.",
    ["provider"],
)
delivery_failures_total = Counter(
    "delivery_failures_total",
    "Album delivery failures by provider and error type.",
    ["provider", "error_type"],
)
delivery_duration_seconds = Histogram(
    "delivery_duration_seconds",
    "Album delivery duration in seconds.",
)
provider_sync_total = Counter(
    "provider_sync_total",
    "Provider sync outcomes by provider and status.",
    ["provider", "status"],
)
provider_sync_failures_total = Counter(
    "provider_sync_failures_total",
    "Provider sync failures by provider and error type.",
    ["provider", "error_type"],
)
provider_sync_duration_seconds = Histogram(
    "provider_sync_duration_seconds",
    "Provider sync duration in seconds.",
)
provider_library_album_count = Gauge(
    "provider_library_album_count",
    "Available cached library album count by provider and user.",
    ["provider", "user_id"],
)
provider_accounts_total = Gauge(
    "provider_accounts_total",
    "Provider account totals by provider and status.",
    ["provider", "status"],
)
provider_accounts_needing_reauth = Gauge(
    "provider_accounts_needing_reauth",
    "Provider accounts needing reauth by provider.",
    ["provider"],
)
oauth_start_total = Counter(
    "oauth_start_total",
    "OAuth start attempts by provider.",
    ["provider"],
)
oauth_callback_total = Counter(
    "oauth_callback_total",
    "OAuth callback outcomes by provider and result.",
    ["provider", "result"],
)
oauth_token_exchange_total = Counter(
    "oauth_token_exchange_total",
    "OAuth token exchange outcomes by provider and result.",
    ["provider", "result"],
)
oauth_refresh_total = Counter(
    "oauth_refresh_total",
    "OAuth token refresh outcomes by provider and result.",
    ["provider", "result"],
)
oauth_state_validation_fail_total = Counter(
    "oauth_state_validation_fail_total",
    "OAuth state validation failures by provider.",
    ["provider"],
)
token_refresh_failures_total = Counter(
    "token_refresh_failures_total",
    "Provider token refresh failures by provider.",
    ["provider"],
)
rate_limit_hits_total = Counter(
    "rate_limit_hits_total",
    "Rate limit hits by command.",
    ["command"],
)
commands_total = Counter(
    "commands_total",
    "Bot command outcomes by command and status.",
    ["command", "status"],
)
job_queue_depth = Gauge(
    "job_queue_depth",
    "Job counts by job type and status.",
    ["type", "status"],
)


def normalize_provider(provider: Any) -> str:
    value = str(provider or "").strip().lower()
    return value or "unknown"


def normalize_command(command: Any) -> str:
    value = str(command or "").strip().lower()
    return value or "unknown"


def normalize_status(status: Any) -> str:
    value = str(status or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    return value.strip("_") or "unknown"


def classify_error(exc: Exception) -> str:
    name = exc.__class__.__name__
    value = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    return value.strip("_") or "unknown_error"


def start_metrics_server(port: int | None, *, host: str | None = None) -> None:
    if port is None:
        return
    bind_host = (host or os.getenv("PROMETHEUS_METRICS_ADDR", "0.0.0.0")).strip() or "0.0.0.0"
    start_http_server(port, addr=bind_host)
    logger.info("Prometheus metrics exporter listening on %s:%s", bind_host, port)


def record_command(command: str, status: str) -> None:
    commands_total.labels(
        command=normalize_command(command),
        status=normalize_status(status),
    ).inc()


def record_rate_limit_hit(command: str) -> None:
    rate_limit_hits_total.labels(command=normalize_command(command)).inc()


def record_token_refresh_failure(provider: str) -> None:
    token_refresh_failures_total.labels(provider=normalize_provider(provider)).inc()


def record_oauth_start(provider: str) -> None:
    oauth_start_total.labels(provider=normalize_provider(provider)).inc()


def record_oauth_callback(provider: str, result: str) -> None:
    oauth_callback_total.labels(
        provider=normalize_provider(provider),
        result=normalize_status(result),
    ).inc()


def record_oauth_token_exchange(provider: str, result: str) -> None:
    oauth_token_exchange_total.labels(
        provider=normalize_provider(provider),
        result=normalize_status(result),
    ).inc()


def record_oauth_refresh(provider: str, result: str) -> None:
    oauth_refresh_total.labels(
        provider=normalize_provider(provider),
        result=normalize_status(result),
    ).inc()


def record_oauth_state_validation_failure(provider: str) -> None:
    oauth_state_validation_fail_total.labels(provider=normalize_provider(provider)).inc()


def update_runtime_snapshot(snapshot: dict[str, list[dict[str, Any]]]) -> None:
    provider_accounts_total.clear()
    provider_accounts_needing_reauth.clear()
    provider_library_album_count.clear()
    job_queue_depth.clear()

    for row in snapshot.get("provider_accounts", []):
        provider_accounts_total.labels(
            provider=normalize_provider(row.get("provider")),
            status=normalize_status(row.get("status")),
        ).set(int(row.get("count") or 0))

    for row in snapshot.get("provider_needs_reauth", []):
        provider_accounts_needing_reauth.labels(
            provider=normalize_provider(row.get("provider")),
        ).set(int(row.get("count") or 0))

    for row in snapshot.get("provider_library_counts", []):
        provider_library_album_count.labels(
            provider=normalize_provider(row.get("provider")),
            user_id=str(row.get("user_id")),
        ).set(int(row.get("count") or 0))

    for row in snapshot.get("job_queue_depth", []):
        job_queue_depth.labels(
            type=normalize_status(row.get("job_type")),
            status=normalize_status(row.get("status")),
        ).set(int(row.get("count") or 0))
