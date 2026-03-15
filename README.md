# Daily YouTube Music Album Bot

Telegram bot that sends one random album per day from a user's saved music library. The current codebase uses PostgreSQL for persistent state, Redis for command locks and rate limiting, and a worker process for scheduled delivery and provider sync jobs.

## Features

- DB-backed delivery history, cycle tracking, and job queue
- Worker-based daily scheduling per user timezone and local delivery time
- Redis-backed command deduplication, short-term locks, and rate limiting
- Manual commands: `/now`, `/nextcycle`, `/refresh`, `/status`
- Admin commands: `/approve`, `/block`, `/admin_status`
- Inline buttons: another album, refresh library, status, open album
- Per-user provider accounts, encrypted credentials, sync state, and album cache stored in PostgreSQL
- Prometheus metrics for bot and worker, plus optional Grafana/Prometheus via Docker Compose

## Requirements

- Python 3.11+
- PostgreSQL
- Redis
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
REDIS_URL=redis://localhost:6379/0
ALLOWED_CHAT_ID=123456789

LIBRARY_LIMIT=500
CREDENTIALS_MASTER_KEY=...

DEFAULT_TIMEZONE=UTC
PROMETHEUS_METRICS_PORT=8000
HTTP_HOST=0.0.0.0
HTTP_PORT=8080
CADDY_SITE_ADDRESS=albums.example.com
SPOTIFY_CLIENT_ID=...
SPOTIFY_CLIENT_SECRET=...
SPOTIFY_REDIRECT_URI=https://albums.example.com/oauth/spotify/callback
SPOTIFY_OAUTH_STATE_TTL_SECONDS=600
SPOTIFY_TOKEN_REFRESH_WINDOW_SECONDS=600
WORKER_PROMETHEUS_METRICS_PORT=8001
WORKER_POLL_SECONDS=15
WORKER_DUE_WINDOW_SECONDS=60
WORKER_JOB_LEASE_SECONDS=300
PROVIDER_SYNC_INTERVAL_SECONDS=21600
```

Notes:

- `DATABASE_URL` is required by both `bot.py` and `worker.py`.
- `REDIS_URL` is required by `bot.py` for command locks, request dedupe, and rate limiting.
- `CONTAINER_DATABASE_URL` and `CONTAINER_REDIS_URL` are optional Compose-specific overrides. They default to the built-in `db` and `redis` service addresses.
- `CADDY_SITE_ADDRESS` controls the public site address Caddy serves. Set it to your NAS hostname or public IP for automatic HTTPS; if omitted, Compose defaults it to `localhost`.
- `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, and `SPOTIFY_REDIRECT_URI` are required for Spotify OAuth. `SPOTIFY_REDIRECT_URI` must exactly match the redirect URI configured in the Spotify app dashboard.
- `SPOTIFY_OAUTH_STATE_TTL_SECONDS` controls how long a Spotify OAuth state remains valid before callback validation rejects it. The default is `600`.
- `SPOTIFY_TOKEN_REFRESH_WINDOW_SECONDS` controls how early the worker refreshes Spotify access tokens before expiry. The default is `600`.
- `ALLOWED_CHAT_ID` is only an admin override for admin-only commands such as `/approve`, `/block`, and `/admin_status`.
- Daily scheduling is executed by `worker.py` using per-user settings stored in the DB.
- `/refresh` now queues a sync for the calling user’s active provider account.
- `DAILY_TIME` is no longer used.
- `bot.py` resolves its app timezone from the admin override user's DB settings when that user exists; otherwise it falls back to `DEFAULT_TIMEZONE`.
- `bot.py` also exposes a tiny HTTP server on `HTTP_HOST:HTTP_PORT` for reverse-proxy health checks and OAuth handoff. By default it listens on `0.0.0.0:8080` and serves `GET /healthz` plus `GET /oauth/spotify/callback`.
- Compose now runs Caddy as the public reverse proxy. `bot-app` stays on the internal Compose network via `expose`, while Caddy publishes ports `80` and `443` and keeps certificates in named volumes.
- `WORKER_JOB_LEASE_SECONDS` controls when `running` jobs are considered stale and requeued after a worker crash.
- `PROVIDER_SYNC_INTERVAL_SECONDS` controls periodic background provider sync scheduling.
- `NOW_RATE_LIMIT_HOURLY`, `NOW_RATE_LIMIT_DAILY`, `NEXTCYCLE_RATE_LIMIT_HOURLY`, `NEXTCYCLE_RATE_LIMIT_DAILY`, `REFRESH_RATE_LIMIT_HOURLY`, and `REFRESH_RATE_LIMIT_DAILY` are optional command rate-limit overrides.
- The bot exports Prometheus metrics on `PROMETHEUS_METRICS_PORT` and the worker exports on `WORKER_PROMETHEUS_METRICS_PORT`.

## Setup

1. Start infrastructure. For local development:

```bash
docker compose up -d db redis
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

To run the full app stack with Compose instead:

```bash
docker compose up -d --build bot-app worker caddy
```

To start the optional monitoring stack locally:

```bash
docker compose up -d prometheus grafana
```

For a release-style rollout that rebuilds the app containers, runs migrations, and restarts the Compose services:

```bash
./scripts/release.sh
```

## Getting `ALLOWED_CHAT_ID`

1. Start your bot in Telegram and send it a message.
2. Run:

```bash
python3 scripts/get_chat_id.py
```

3. Copy the printed `chat_id` into `.env`.

## Bot Commands

- `/start` - register the user and show readiness
- `/help` - show available user commands
- `/connect_spotify` - start Spotify OAuth for the user
- `/reconnect_spotify` - restart Spotify OAuth for an existing Spotify account
- `/disconnect_spotify` - disable the Spotify account
- `/now` - queue an immediate album delivery
- `/nextcycle` - queue an album from the next cycle immediately
- `/refresh` - queue a provider library sync for the current user
- `/status` - show DB delivery stats, provider status, and token/auth state
- `/approve <telegram_user_id>` - admin-only: allow a registered user
- `/block <telegram_user_id>` - admin-only: block a user
- `/admin_status` - admin-only: show queue and user summary
- `/settz` - set the user timezone
- `/settime` - set the user daily local delivery time

## Data

- `app.delivery_history` - DB delivery history used for no-repeat cycle semantics
- `app.jobs` - queued delivery work for the worker
- `app.user_library_albums` - per-user normalized album cache
- `app.user_provider_accounts` / `app.user_provider_sync_state` - provider auth and sync state

## Tests

The repository currently uses `pytest` for the phase-specific suites, and `scripts/run_tests.sh` orchestrates the full local test run.

Run the full local test flow:

```bash
./scripts/run_tests.sh
```

That script:

- starts `db` and `redis` with Docker Compose
- creates a dedicated test database
- applies Alembic migrations
- runs the legacy `unittest` suite in `tests/`
- runs the pytest contract, unit, and integration suites
- runs `ruff check .`

Run only the current pytest suites:

```bash
pytest -q tests/test_contract_pytest.py tests/test_unit_pytest.py tests/test_integration_pytest.py
```

## Utility Scripts

- `python3 scripts/get_chat_id.py` - fetch recent Telegram updates and print a likely chat id
- `python3 scripts/upsert_provider_credentials.py --user-id <id> --provider ytmusic --credentials-file creds.json --enqueue-sync` - store encrypted provider credentials and optionally queue revalidate/sync jobs
- `python3 scripts/test_format.py` - check album URL/message formatting against a local cache payload

## Troubleshooting

- Empty library or refresh failures: verify the user has an active provider account with valid credentials, then queue `/refresh`.
- No daily deliveries: verify the worker is running, Redis is reachable, and the user has valid DB timezone and daily time settings.
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
- `provider_accounts_needing_reauth{provider}`
- `job_queue_depth{type,status}`
- `oauth_refresh_total{provider,result}`
- `token_refresh_failures_total{provider}`

Bot metrics include:

- `commands_total{command,status}`
- `rate_limit_hits_total{command}`
- `oauth_start_total{provider}`
- `oauth_callback_total{provider,result}`
- `oauth_token_exchange_total{provider,result}`
- `oauth_state_validation_fail_total{provider}`
