#!/usr/bin/env bash
# Запускает команду с отдельным Redis и удаляет только свой контейнер.
set -euo pipefail
if [[ $# -eq 0 ]]; then
  printf '%s\n' 'Usage: bash scripts/with-test-redis.sh command [args...]' >&2
  exit 2
fi
redis_image='redis@sha256:7d1e4ce8b9395088377ab382d1f6cfdbd13b3690795198a0399ab8d683064d6d'
redis_container="food-test-redis-$(date +%s)-$$"
redis_id=''

# Сохраняет код проверки и удаляет Redis по полученному ID.
cleanup() {
  local status=$?
  if [[ -n "$redis_id" ]]; then
    docker rm -f "$redis_id" >/dev/null || status=1
  fi
  return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

redis_id="$(docker run -d --pull never --name "$redis_container" \
  --label food-delivery.test=redis --publish 127.0.0.1::6379 \
  --tmpfs /data:rw,size=16m "$redis_image" \
  redis-server --save '' --appendonly no --maxmemory 128mb --maxmemory-policy noeviction)"
ready=false
for ((attempt=0; attempt<50; attempt++)); do
  if [[ "$(docker exec "$redis_id" redis-cli --raw PING 2>/dev/null || true)" == PONG ]]; then
    ready=true
    break
  fi
  sleep 0.1
done
if [[ "$ready" != true ]]; then
  printf '%s\n' 'Test Redis did not become ready' >&2
  exit 1
fi
FOOD_TEST_REDIS_ADDR="$(docker port "$redis_id" 6379/tcp)"
if [[ ! "$FOOD_TEST_REDIS_ADDR" =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
  printf '%s\n' 'Unexpected test Redis address' >&2
  exit 1
fi
export FOOD_TEST_REDIS_ADDR
export ORDERS_TEST_REDIS_ADDR="$FOOD_TEST_REDIS_ADDR"
printf 'Isolated test Redis: %s (%s)\n' "$FOOD_TEST_REDIS_ADDR" "$redis_container"
"$@"
