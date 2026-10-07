#!/usr/bin/env bash
# Проверяет репозиторий и HTTP-процесс уведомлений на временной БД.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/.."
infrastructure_dir="$PWD"
workspace_dir="$(cd -- .. && pwd)"
set -a
source ./.env
set +a

compose=(docker compose --env-file "$infrastructure_dir/.env" -f "$infrastructure_dir/deploy/compose/compose.yaml")
test_db="notifications_test_$(date +%s)_$$"
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
admin_sql "CREATE DATABASE \"$test_db\" OWNER notifications_app;"
created=true
admin_sql "REVOKE ALL ON DATABASE \"$test_db\" FROM PUBLIC;"
NOTIFICATIONS_MIGRATION_DB="$test_db" "${compose[@]}" run --rm --no-deps -T migrate-notifications up

export NOTIFICATIONS_TEST_DATABASE_URL="postgres://notifications_app@127.0.0.1:15432/$test_db?sslmode=disable"
export NOTIFICATIONS_TEST_DB_PASSWORD="$NOTIFICATIONS_DB_PASSWORD"
cd "$workspace_dir/notifications-service"
go test -tags=integration -race -count=1 -run '^Test(NotificationsRepositoryPostgres|NotificationsProcess)$' -timeout 60s -v ./internal/repository ./cmd/notifications
