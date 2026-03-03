# Daily YouTube Music Album Bot

Telegram bot that sends one random album per day from your YouTube Music library, without repeats until the current cycle is exhausted.

## Features

- Daily scheduled album post (`DAILY_TIME` + `TZ`)
- Manual commands (`/now`, `/refresh`, `/status`)
- Inline buttons (`Another album`, `Refresh library`, `Status`, `Open album`)
- No-repeat cycle using persisted history (`data/sent_history.json`)
- Local album cache for faster picks (`data/albums_cache.json`)
- Basic auth-expiration diagnostics for YT Music credentials

## Requirements

- Python 3.11+ recommended
- Telegram bot token from BotFather
- YouTube Music auth file (`secrets/browser.json`) for `ytmusicapi`

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Create `.env` (you can start from `.env.example`):

```env
BOT_TOKEN=...
ALLOWED_CHAT_ID=123456789

YTM_AUTH_PATH=secrets/browser.json
ALBUM_CACHE_PATH=data/albums_cache.json
HISTORY_PATH=data/sent_history.json
LIBRARY_LIMIT=500

DAILY_TIME=09:30
TZ=Europe/Riga
```

Notes:

- `ALLOWED_CHAT_ID` is the only chat allowed to use the bot (single-user mode).
- `DAILY_TIME` is interpreted in `TZ`.
- `LIBRARY_LIMIT` is passed to `ytmusicapi.get_library_albums(limit=...)`.

## Getting `ALLOWED_CHAT_ID`

1. Start your bot in Telegram and send it a message (for example: `ping`).
2. Run:

```bash
python sripts/get_chat_id.py
```

3. Copy the printed `chat_id` into `.env`.

## Getting `secrets/browser.json`

Generate auth headers/cookies for `ytmusicapi` and save as `secrets/browser.json`.
If auth expires later, regenerate the file and restart the bot.

## Run

```bash
source .venv/bin/activate
python bot.py
```

The bot runs in polling mode and schedules one daily job.

## Bot Commands

- `/start` - show readiness message
- `/now` - send album immediately
- `/refresh` - force refresh of local album cache
- `/status` - show cache size, sent count, remaining count, and timestamps

## Data Files

- `data/albums_cache.json` - cached normalized album list + `updated_at`
- `data/sent_history.json` - sent `browseId`s for current no-repeat cycle + `updated_at`

When all cached albums are exhausted, the picker refreshes the library and starts a new cycle with empty history.

## Utility Scripts

- `python sripts/test_ytmusic.py` - quick check that YT Music auth works
- `python sripts/test_cache.py` - force cache refresh and print sample
- `python sripts/test_pick.py` - run several no-repeat picks
- `python sripts/test_format.py` - check URL/message formatting from cache data

## Troubleshooting

- `Failed to pick an album` / empty results: verify `YTM_AUTH_PATH` points to a valid `browser.json`, then run `python sripts/test_ytmusic.py`.
- Unauthorized Telegram interactions: verify `ALLOWED_CHAT_ID`.
- Daily post not at expected time: verify `TZ` and `DAILY_TIME`.
