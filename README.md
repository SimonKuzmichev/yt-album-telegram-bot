# Daily YouTube Music Album Bot

Telegram bot that sends one random album per day from your YouTube Music library. Delivery state is DB-backed, so albums do not repeat within a cycle until that user's current cycle is exhausted.

## Features

- DB-backed delivery history and cycle tracking
- Worker-based daily scheduling per user timezone and local delivery time
- Manual commands: `/now`, `/nextcycle`, `/refresh`, `/status`
- Inline buttons: another album, refresh library, status, open album
- Per-user provider account sync and album cache stored in PostgreSQL
- Basic auth-expiration diagnostics for YT Music credentials

## Requirements

- Python 3.11+
- PostgreSQL
- Docker Compose
- Telegram bot token from BotFather
- `CREDENTIALS_MASTER_KEY` for encrypted provider credentials at rest

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Create `.env`:

```env
BOT_TOKEN=...
DATABASE_URL=postgresql://app:app@localhost:5432/ytabot
ALLOWED_CHAT_ID=123456789

LIBRARY_LIMIT=500
CREDENTIALS_MASTER_KEY=...

DEFAULT_TIMEZONE=UTC
PROMETHEUS_METRICS_PORT=8000
WORKER_PROMETHEUS_METRICS_PORT=8001
```

Notes:

- `DATABASE_URL` is required by both `bot.py` and `worker.py`.
- `ALLOWED_CHAT_ID` is only an admin override for admin-only commands such as `/approve`, `/block`, and `/admin_status`.
- Daily scheduling is executed by `worker.py` using per-user settings stored in the DB.
- `/refresh` now queues a sync for the calling user’s active provider account.
- `DAILY_TIME` is no longer used.
- `bot.py` resolves its app timezone from the admin override user's DB settings when that user exists; otherwise it falls back to `DEFAULT_TIMEZONE`.
- `WORKER_JOB_LEASE_SECONDS` controls when `running` jobs are considered stale and requeued after a worker crash.
- The bot exports Prometheus metrics on `PROMETHEUS_METRICS_PORT` and the worker exports on `WORKER_PROMETHEUS_METRICS_PORT`.

## Setup

1. Start PostgreSQL. For local development you can use:

```bash
docker compose up -d db
```

2. Apply migrations.
```bash
alembic upgrade head
```

3. Start the bot process.
4. Start the worker process.

Example:

```bash
source .venv/bin/activate
pip install -r requirements.txt
python3 bot.py
```

```bash
source .venv/bin/activate
pip install -r requirements.txt
python3 worker.py
```

## Getting `ALLOWED_CHAT_ID`

1. Start your bot in Telegram and send it a message.
2. Run:

```bash
python3 sripts/get_chat_id.py
```

3. Copy the printed `chat_id` into `.env`.

## Bot Commands

- `/start` - register the user and show readiness
- `/now` - queue an immediate album delivery
- `/nextcycle` - queue an album from the next cycle immediately
- `/refresh` - queue a provider library sync for the current user
- `/status` - show DB delivery stats and active provider status
- `/settz` - set the user timezone
- `/settime` - set the user daily local delivery time

## Data

- `app.delivery_history` - DB delivery history used for no-repeat cycle semantics
- `app.jobs` - queued delivery work for the worker
- `app.user_library_albums` - per-user normalized album cache
- `app.user_provider_accounts` / `app.user_provider_sync_state` - provider auth and sync state

## Utility Scripts

- `python3 sripts/test_format.py` - check URL/message formatting from cache data
- `python3 scripts/upsert_provider_credentials.py` - store encrypted provider credentials and optionally queue sync jobs

## Troubleshooting

- Empty library or refresh failures: verify the user has an active provider account with valid credentials, then queue `/refresh`.
- No daily deliveries: verify the worker is running and the user has valid DB timezone and daily time settings.
- Jobs stuck in `running`: verify `WORKER_JOB_LEASE_SECONDS` is appropriate for normal job duration and that the worker loop is running.
- Unauthorized admin actions: verify `ALLOWED_CHAT_ID`.

## Prometheus Metrics

The bot process exposes command and rate-limit metrics, and the worker process exposes delivery, sync, and DB-backed gauge metrics. Scrape both endpoints if you want the full set.

Worker metrics include:

- `album_deliveries_total{provider,status}`
- `delivery_attempts_total{provider}`
- `delivery_failures_total{provider,error_type}`
- `delivery_duration_seconds`
- `provider_sync_total{provider,status}`
- `provider_sync_failures_total{provider,error_type}`
- `provider_sync_duration_seconds`
- `provider_library_album_count{provider,user_id}`
- `provider_accounts_total{provider,status}`
- `provider_accounts_needs_reauth{provider}`
- `job_queue_depth{type,status}`

Bot metrics include:

- `commands_total{command,status}`
- `rate_limit_hits_total{command}`

`token_refresh_failures_total{provider}` is registered for future provider token-refresh flows; the current codebase does not perform refreshes yet.
