#!/usr/bin/env python3
# Проверяет кэш через HTTP и Redis, восстанавливая цену и остановленный Redis.
import json
from pathlib import Path
import subprocess
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".env"), "-f", str(ROOT / "deploy/compose/compose.yaml")]
BASE = "http://127.0.0.1:8082"
RESTAURANT = "00000000-0000-4000-8000-000000000001"
ITEM = "00000000-0000-4000-8000-000000000101"
MENU_URL = f"/v1/restaurants/{RESTAURANT}/menu"
KEY = f"menu:v1:{RESTAURANT}"


# Завершает проверку понятной ошибкой при нарушении условия.
def require(condition, message):
    if not condition:
        raise RuntimeError(message)


# Выполняет ограниченную по времени команду Compose.
def compose(*args):
    result = subprocess.run(COMPOSE + list(args), cwd=ROOT, text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(f"Compose {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


# Выполняет команду Redis и возвращает необработанный текст ответа.
def redis(*args):
    return compose("exec", "-T", "redis", "redis-cli", "--raw", *args)


# Отправляет JSON-запрос к локальному каталогу с ограничением времени.
def api(path, method="GET", body=None, staff=False):
    headers = {"Accept": "application/json"}
    if staff:
        headers["X-Demo-Role"] = "staff"
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=5) as response:
            require(response.status == 200, f"{method} {path}: expected HTTP 200")
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"{method} {path}: HTTP {error.code}: {error.read(2048).decode(errors='replace')}") from error


# Находит цену демонстрационного блюда в ответе публичного меню.
def menu_price(menu):
    for item in menu:
        if item["id"] == ITEM:
            return item["price_kopecks"]
    raise RuntimeError("Demo item is missing; run make up first")


# Проверяет JSON реального ключа и положительный TTL до 70 секунд.
def check_key(price):
    items = json.loads(redis("GET", KEY))
    require(any(item["ID"] == ITEM and item["PriceKopecks"] == price for item in items), "Cached price differs from the HTTP response")
    ttl = int(redis("PTTL", KEY))
    require(0 < ttl <= 70000, f"Unexpected cache TTL: {ttl} ms")
    print(f"OK: menu JSON in Redis, TTL {ttl} ms", flush=True)


# Проверяет заполнение, инвалидацию и fallback, затем восстанавливает стенд.
def main():
    require(redis("PING") == "PONG", "Redis is not ready; run make up first")
    require(api("/readyz")["status"] == "ok", "Catalog is not ready")
    # Resolve обходит кэш и даёт исходную цену из PostgreSQL.
    resolved = api("/internal/v1/menu/resolve", "POST", {"restaurant_id": RESTAURANT, "item_ids": [ITEM]})
    original_price = resolved["items"][0]["price_kopecks"]
    changed_price = original_price + 1 if original_price < 1000000 else original_price - 1
    restore_price = False
    restart_redis = False
    try:
        menu = api(MENU_URL)
        check_key(menu_price(menu))
        restore_price = True
        api(f"/v1/menu-items/{ITEM}", "PATCH", {"price_kopecks": changed_price}, staff=True)
        require(redis("EXISTS", KEY) == "0", "PATCH did not invalidate the menu key")
        require(menu_price(api(MENU_URL)) == changed_price, "Menu price did not change after PATCH")
        check_key(changed_price)
        print("OK: PATCH invalidation and cache refill", flush=True)

        restart_redis = True
        compose("stop", "redis")
        require(api("/readyz")["status"] == "ok", "Redis outage broke readiness")
        require(menu_price(api(MENU_URL)) == changed_price, "Redis outage broke PostgreSQL fallback")
        print("OK: menu and readiness work while Redis is stopped", flush=True)
    finally:
        # Сначала восстанавливаем Redis, затем цену и реальный ключ для GUI.
        cleanup_errors = []
        if restart_redis:
            try:
                compose("up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "redis")
                print("Redis restarted", flush=True)
            except Exception as error:
                cleanup_errors.append(f"restart Redis: {error}")
        if restore_price:
            try:
                api(f"/v1/menu-items/{ITEM}", "PATCH", {"price_kopecks": original_price}, staff=True)
                require(menu_price(api(MENU_URL)) == original_price, "Original price was not restored")
                print(f"Original price restored: {original_price} kopecks", flush=True)
            except Exception as error:
                cleanup_errors.append(f"restore original price {original_price}: {error}")
        require(not cleanup_errors, "; ".join(cleanup_errors))
    check_key(original_price)
    print("PASS: menu cache, invalidation and Redis fallback")
    print(f"Redis Insight: http://localhost:5540 — key {KEY}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
