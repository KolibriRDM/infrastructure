#!/usr/bin/env bash
# Проверяет Redis и GUI, удаляя только собственный временный ключ.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/.."
compose=(docker compose --env-file .env -f deploy/compose/compose.yaml)
test_key="food:infra-check:$(python3 -c 'import uuid; print(uuid.uuid4())')"

# Выполняет команду Redis без интерактивного терминала.
redis() {
  "${compose[@]}" exec -T redis redis-cli --raw "$@"
}

# Удаляет исключительно ключ текущей проверки.
cleanup() {
  local status=$?
  redis DEL "$test_key" >/dev/null || status=1
  return "$status"
}
trap cleanup EXIT

[[ "$(redis PING)" == PONG ]]
[[ "$(redis SET "$test_key" '{"check":"food-delivery"}' EX 60 NX)" == OK ]]
[[ "$(redis GET "$test_key")" == '{"check":"food-delivery"}' ]]
ttl="$(redis TTL "$test_key")"
[[ "$ttl" =~ ^[0-9]+$ && "$ttl" -gt 0 && "$ttl" -le 60 ]]
[[ "$(redis DEL "$test_key")" == 1 ]]
[[ "$(redis EXISTS "$test_key")" == 0 ]]
python3 - <<'PY'
from urllib.request import urlopen
with urlopen('http://127.0.0.1:5540/api/health/', timeout=5) as response:
    if response.status != 200:
        raise SystemExit('Redis Insight is not healthy')
with urlopen('http://127.0.0.1:5540/', timeout=5) as response:
    if response.status != 200 or b'<html' not in response.read(65536).lower():
        raise SystemExit('Redis Insight UI is not available')
print('Redis PING/SET/GET/TTL/DEL and Redis Insight health/UI: OK')
PY
