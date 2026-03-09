#!/usr/bin/env bash
set -euo pipefail

source .venv/bin/activate

pip install -r requirements.txt
docker compose up -d db --wait
alembic upgrade head

sudo systemctl restart yt-album-bot
sudo systemctl restart yt-album-worker