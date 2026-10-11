#!/usr/bin/env bash
# Проверяет relay на временной БД и отдельном Kafka-брокере.
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
broker_name="food-orders-relay-test-$(date +%s)-$$"
broker_created=false

# Выполняет административный SQL внутри контейнера PostgreSQL.
admin_sql() {
  "${compose[@]}" exec -T postgres sh -ec \
    'export PGPASSWORD="$POSTGRES_PASSWORD"; exec psql -X -w -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "$1"' \
    sh "$1"
}

# Удаляет временные ресурсы, сохраняя код завершения тестов.
cleanup() {
  local status=$?
  if [[ "$broker_created" == true ]]; then
    docker rm -f -v "$broker_name" >/dev/null || status=1
  fi
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
# Выбираем свободный host-порт для изолированного брокера.
broker_port="$(go run ./cmd/devcheck free-port)"
docker run -d --name "$broker_name" -p "127.0.0.1:$broker_port:9092" \
  -e CLUSTER_ID=4L6g3nShT-eMCtK--X86sw \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$broker_port" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  -e KAFKA_LOG_DIRS=/tmp/kraft-test-logs \
  apache/kafka:4.1.2 >/dev/null
broker_created=true
export ORDERS_RELAY_TEST_CONTAINER="$broker_name"
export ORDERS_RELAY_TEST_BROKER="127.0.0.1:$broker_port"
cd "$workspace_dir/orders-service"
go test -tags=integration -race -count=1 -run '^TestOrdersRelayProcess$' -timeout 150s -v ./cmd/orders
