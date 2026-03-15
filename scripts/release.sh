#!/usr/bin/env bash
set -euo pipefail

docker compose build bot-app worker
docker compose up -d db redis
docker compose up -d prometheus grafana
docker compose run --rm --no-deps bot-app alembic upgrade head
docker compose up -d --wait bot-app
docker compose up -d worker
docker compose up -d caddy
