#!/usr/bin/env bash
set -euo pipefail

if ! systemctl is-active --quiet sing-box.service; then
  echo "sing-box.service is not active; aborting release." >&2
  exit 1
fi

docker compose build bot-app worker
docker compose up -d --wait db redis
docker compose run --rm -T --no-deps bot-app alembic upgrade head
docker compose up -d --wait bot-app
docker compose up -d worker
docker compose up -d prometheus grafana caddy
