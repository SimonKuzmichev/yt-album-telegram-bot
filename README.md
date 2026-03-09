# Daily YouTube Music Album Bot

Telegram bot that sends one random album per day from your YouTube Music library. Delivery state is DB-backed, so albums do not repeat within a cycle until that user's current cycle is exhausted.

## Features

- DB-backed delivery history and cycle tracking
- Worker-based daily scheduling per user timezone and local delivery time
- Manual commands: `/now`, `/nextcycle`, `/refresh`, `/status`
- Inline buttons: another album, refresh library, status, open album
- Local album cache for faster library reads
- Basic auth-expiration diagnostics for YT Music credentials

## Requirements

- Python 3.11+
- PostgreSQL
- Telegram bot token from BotFather
- YouTube Music auth file at `secrets/browser.json` for `ytmusicapi`

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

YTM_AUTH_PATH=secrets/browser.json
ALBUM_CACHE_PATH=data/albums_cache.json
LIBRARY_LIMIT=500

DAILY_TIME=09:30
TZ=Europe/Riga
```

Notes:

- `DATABASE_URL` is required by both `bot.py` and `worker.py`.
- `ALLOWED_CHAT_ID` is only an admin override for admin-only commands such as `/approve`, `/block`, `/pending`, and `/refresh`.
- Daily scheduling is executed by `worker.py` using per-user settings stored in the DB.
- `DAILY_TIME` and `TZ` are still validated by `bot.py` at startup, but real daily delivery timing comes from DB user settings.
- `WORKER_JOB_LEASE_SECONDS` controls when `running` jobs are considered stale and requeued after a worker crash.

## Setup

1. Start PostgreSQL. For local development you can use:

```bash
docker compose up -d db
```

2. Apply migrations.
3. Start the bot process.
4. Start the worker process.

Example:

```bash
source .venv/bin/activate
python3 bot.py
```

```bash
source .venv/bin/activate
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
- `/refresh` - force refresh of the local album cache (admin override only)
- `/status` - show DB delivery stats and local cache status
- `/settz` - set the user timezone
- `/settime` - set the user daily local delivery time

## Data

- `data/albums_cache.json` - cached normalized album list plus `updated_at`
- `app.delivery_history` - DB delivery history used for no-repeat cycle semantics
- `app.jobs` - queued delivery work for the worker

## Utility Scripts

- `python3 sripts/test_ytmusic.py` - quick check that YT Music auth works
- `python3 sripts/test_cache.py` - force cache refresh and print sample
- `python3 sripts/test_format.py` - check URL/message formatting from cache data

## Troubleshooting

- Empty library or refresh failures: verify `YTM_AUTH_PATH`, then run `python3 sripts/test_ytmusic.py`.
- No daily deliveries: verify the worker is running and the user has valid DB timezone and daily time settings.
- Jobs stuck in `running`: verify `WORKER_JOB_LEASE_SECONDS` is appropriate for normal job duration and that the worker loop is running.
- Unauthorized admin actions: verify `ALLOWED_CHAT_ID`.
