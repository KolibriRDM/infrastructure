#!/usr/bin/env bash
# Проверяет цепочку заказа и уведомления на временных БД и отдельном топике.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/.."
infrastructure_dir="$PWD"
workspace_dir="$(cd -- .. && pwd)"
set -a
source ./.env
set +a

compose=(docker compose --env-file "$infrastructure_dir/.env" -f "$infrastructure_dir/deploy/compose/compose.yaml")
suffix="$(date +%s)_$$"
orders_db="orders_test_events_$suffix"
catalog_db="catalog_test_events_$suffix"
notifications_db="notifications_test_events_$suffix"
notifications_created=false
orders_created=false
catalog_created=false
binary_dir="$(mktemp -d "${TMPDIR:-/tmp}/food-events-e2e.XXXXXX")"

# Выполняет административный SQL внутри контейнера PostgreSQL.
admin_sql() {
  "${compose[@]}" exec -T postgres sh -ec \
    'export PGPASSWORD="$POSTGRES_PASSWORD"; exec psql -X -w -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "$1"' \
    sh "$1"
}

# Удаляет временные ресурсы, сохраняя код завершения тестов.
cleanup() {
  local status=$?
  if [[ "$orders_created" == true ]]; then
    admin_sql "DROP DATABASE \"$orders_db\" WITH (FORCE);" || status=1
  fi
  if [[ "$catalog_created" == true ]]; then
    admin_sql "DROP DATABASE \"$catalog_db\" WITH (FORCE);" || status=1
  fi
  if [[ "$notifications_created" == true ]]; then
    admin_sql "DROP DATABASE \"$notifications_db\" WITH (FORCE);" || status=1
  fi
  rm -f -- "$binary_dir/orders" "$binary_dir/catalog" "$binary_dir/notifications"
  rmdir -- "$binary_dir" || status=1
  return "$status"
}
trap cleanup EXIT

"${compose[@]}" up -d --wait --wait-timeout 180 --no-recreate postgres kafka
admin_sql "CREATE DATABASE \"$orders_db\" OWNER orders_app;"
orders_created=true
admin_sql "REVOKE ALL ON DATABASE \"$orders_db\" FROM PUBLIC;"
admin_sql "CREATE DATABASE \"$catalog_db\" OWNER catalog_app;"
catalog_created=true
admin_sql "REVOKE ALL ON DATABASE \"$catalog_db\" FROM PUBLIC;"
admin_sql "CREATE DATABASE \"$notifications_db\" OWNER notifications_app;"
notifications_created=true
admin_sql "REVOKE ALL ON DATABASE \"$notifications_db\" FROM PUBLIC;"
NOTIFICATIONS_MIGRATION_DB="$notifications_db" "${compose[@]}" run --rm --no-deps -T migrate-notifications up
ORDERS_MIGRATION_DB="$orders_db" "${compose[@]}" run --rm --no-deps -T migrate-orders up
CATALOG_MIGRATION_DB="$catalog_db" "${compose[@]}" run --rm --no-deps -T migrate-catalog up
"${compose[@]}" exec -T postgres sh -ec \
  'export PGPASSWORD="$CATALOG_DB_PASSWORD"; exec psql -X -w -h 127.0.0.1 -U catalog_app -d "$1" -v ON_ERROR_STOP=1' \
  sh "$catalog_db" < scripts/seed-catalog.sql

(cd "$workspace_dir/orders-service" && go build -race -o "$binary_dir/orders" ./cmd/orders)
(cd "$workspace_dir/catalog-service" && go build -race -o "$binary_dir/catalog" ./cmd/catalog)
export EVENTS_ORDERS_BINARY="$binary_dir/orders"
export EVENTS_CATALOG_BINARY="$binary_dir/catalog"
export EVENTS_ORDERS_DATABASE_URL="postgres://orders_app@127.0.0.1:15432/$orders_db?sslmode=disable"
export EVENTS_CATALOG_DATABASE_URL="postgres://catalog_app@127.0.0.1:15432/$catalog_db?sslmode=disable"
(cd "$workspace_dir/notifications-service" && go build -race -o "$binary_dir/notifications" ./cmd/notifications)
export EVENTS_NOTIFICATIONS_BINARY="$binary_dir/notifications"
export EVENTS_NOTIFICATIONS_DATABASE_URL="postgres://notifications_app@127.0.0.1:15432/$notifications_db?sslmode=disable"
export EVENTS_KAFKA_BROKERS="127.0.0.1:19092"
go test -tags=integration,e2e -race -count=1 -run '^TestOrderNotificationEvents$' -timeout 150s -v ./tests/integration
