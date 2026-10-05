#!/usr/bin/env bash
# Запускает тесты репозитория orders на временной БД.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/.."
infrastructure_dir="$PWD"
workspace_dir="$(cd -- .. && pwd)"
set -a
source ./.env
set +a

compose=(docker compose --env-file "$infrastructure_dir/.env" -f "$infrastructure_dir/deploy/compose/compose.yaml")
test_db="orders_test_$(date +%s)_$$"
created=false

# Выполняет административный SQL внутри контейнера PostgreSQL.
admin_sql() {
  "${compose[@]}" exec -T postgres sh -ec \
    'export PGPASSWORD="$POSTGRES_PASSWORD"; exec psql -X -w -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "$1"' \
    sh "$1"
}

# Удаляет временные ресурсы, сохраняя код завершения тестов.
cleanup() {
  local status=$?
  if [[ "$created" == true ]]; then
    if ! admin_sql "DROP DATABASE \"$test_db\" WITH (FORCE);"; then
      printf 'Could not remove test database %s\n' "$test_db" >&2
      return 1
    fi
  fi
  return "$status"
}
trap cleanup EXIT

"${compose[@]}" up -d --wait --no-recreate postgres
admin_sql "CREATE DATABASE \"$test_db\" OWNER orders_app;"
created=true
admin_sql "REVOKE ALL ON DATABASE \"$test_db\" FROM PUBLIC;"
ORDERS_MIGRATION_DB="$test_db" "${compose[@]}" run --rm --no-deps -T migrate-orders up

export ORDERS_TEST_DATABASE_URL="postgres://orders_app@127.0.0.1:15432/$test_db?sslmode=disable"
export ORDERS_TEST_DB_PASSWORD="$ORDERS_DB_PASSWORD"
cd "$workspace_dir/orders-service"
go test -tags=integration -race -count=1 -run '^TestOrdersRepositoryPostgres$' -timeout 60s -v ./internal/repository
