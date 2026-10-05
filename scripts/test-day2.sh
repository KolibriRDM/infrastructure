#!/usr/bin/env bash
# Проверяет два сервиса на временных БД и удаляет тестовые ресурсы.
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
orders_db="orders_test_day2_$suffix"
catalog_db="catalog_test_day2_$suffix"
orders_created=false
catalog_created=false
binary_dir="$(mktemp -d "${TMPDIR:-/tmp}/food-day2.XXXXXX")"

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
  rm -f -- "$binary_dir/orders" "$binary_dir/catalog"
  rmdir -- "$binary_dir" || status=1
  return "$status"
}
trap cleanup EXIT

"${compose[@]}" up -d --wait --no-recreate postgres
admin_sql "CREATE DATABASE \"$orders_db\" OWNER orders_app;"
orders_created=true
admin_sql "REVOKE ALL ON DATABASE \"$orders_db\" FROM PUBLIC;"
admin_sql "CREATE DATABASE \"$catalog_db\" OWNER catalog_app;"
catalog_created=true
admin_sql "REVOKE ALL ON DATABASE \"$catalog_db\" FROM PUBLIC;"
ORDERS_MIGRATION_DB="$orders_db" "${compose[@]}" run --rm --no-deps -T migrate-orders up
CATALOG_MIGRATION_DB="$catalog_db" "${compose[@]}" run --rm --no-deps -T migrate-catalog up
"${compose[@]}" exec -T postgres sh -ec \
  'export PGPASSWORD="$CATALOG_DB_PASSWORD"; exec psql -X -w -h 127.0.0.1 -U catalog_app -d "$1" -v ON_ERROR_STOP=1' \
  sh "$catalog_db" < scripts/seed-catalog.sql

(cd "$workspace_dir/orders-service" && go build -race -o "$binary_dir/orders" ./cmd/orders)
(cd "$workspace_dir/catalog-service" && go build -race -o "$binary_dir/catalog" ./cmd/catalog)
export DAY2_ORDERS_BINARY="$binary_dir/orders"
export DAY2_CATALOG_BINARY="$binary_dir/catalog"
export DAY2_ORDERS_DATABASE_URL="postgres://orders_app@127.0.0.1:15432/$orders_db?sslmode=disable"
export DAY2_CATALOG_DATABASE_URL="postgres://catalog_app@127.0.0.1:15432/$catalog_db?sslmode=disable"
go test -tags=integration -race -count=1 -timeout 90s -v ./tests/integration
