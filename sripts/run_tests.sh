#!/usr/bin/env bash
set -Eeuo pipefail

TEST_DB_NAME="ytabot_test"
TEST_DATABASE_URL="postgresql://app:app@localhost:5432/${TEST_DB_NAME}"

cleanup() {
  docker compose exec db psql -U app -d postgres \
    -c "DROP DATABASE IF EXISTS ${TEST_DB_NAME};" || true
}

trap cleanup EXIT

docker compose up -d db --wait

docker compose exec db psql -U app -d postgres \
  -c "DROP DATABASE IF EXISTS ${TEST_DB_NAME};" || true

docker compose exec db psql -U app -d postgres \
  -c "CREATE DATABASE ${TEST_DB_NAME};"

export TEST_DATABASE_URL
export DATABASE_URL="$TEST_DATABASE_URL"

alembic upgrade head
docker compose exec db psql -U app -d ytabot_test -c "\dt app.*"
python -m unittest discover -s tests -v
