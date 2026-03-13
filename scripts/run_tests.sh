#!/usr/bin/env bash
set -Eeuo pipefail

TEST_DB_NAME="ytabot_test"
TEST_DATABASE_URL="postgresql://app:app@localhost:5432/${TEST_DB_NAME}"

cleanup() {
  psql postgresql://app:app@localhost:5432/postgres -c "DROP DATABASE IF EXISTS ${TEST_DB_NAME};" || true
}

trap cleanup EXIT

docker compose up -d db redis --wait

psql postgresql://app:app@localhost:5432/postgres -c "DROP DATABASE IF EXISTS ${TEST_DB_NAME};"

psql postgresql://app:app@localhost:5432/postgres -c "CREATE DATABASE ${TEST_DB_NAME};"

export TEST_DATABASE_URL
export DATABASE_URL="$TEST_DATABASE_URL"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

alembic upgrade head
psql postgresql://app:app@localhost:5432/postgres -c "\dt app.*"
python3 -m unittest discover -s tests -v
python3 -m pytest -q tests/test_contract_pytest.py tests/test_unit_pytest.py tests/test_integration_pytest.py

ruff check .
