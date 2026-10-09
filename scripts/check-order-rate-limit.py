#!/usr/bin/env python3
# Проверяет HTTP-лимитер на двух новых пользователях и восстанавливает Redis.
import importlib.util
import json
from pathlib import Path
import sys
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

spec = importlib.util.spec_from_file_location("menu_cache_check", Path(__file__).with_name("check-menu-cache.py"))
cache_check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache_check)
compose, redis, require = cache_check.compose, cache_check.redis, cache_check.require

BASE = "http://127.0.0.1:8080"


# Возвращает статус, заголовки и JSON как успешного, так и ошибочного ответа.
def api(path, user="", key="", body=None):
    headers = {"Accept": "application/json", "X-Demo-User-ID": user, "Idempotency-Key": key}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = Request(BASE + path, data=data, headers=headers)
    try:
        response = urlopen(request, timeout=5)
    except HTTPError as error:
        response = error
    with response:
        return response.status, response.headers, json.load(response)


# Создаёт два заказа; повторы проверяют лимит без создания ещё 29 заказов.
def main():
    require(redis("PING") == "PONG", "Run make redis-up first")
    require(api("/readyz")[0] == 200, "Orders is not ready; run make apps-up")
    user = str(uuid.uuid4())
    other_user = str(uuid.uuid4())
    key = "limiter-check-" + str(uuid.uuid4())
    counter_key = "rate_limit:orders:create:v1:" + user
    payload = {
        "restaurant_id": "00000000-0000-4000-8000-000000000001",
        "delivery_address": "Москва, Проверка лимитера, 1",
        "items": [{"menu_item_id": "00000000-0000-4000-8000-000000000101", "quantity": 1}],
    }
    order_id = None
    for attempt in range(1, 31):
        status, _, body = api("/v1/orders", user, key, payload)
        require(status == (201 if attempt == 1 else 200), f"Attempt {attempt}: HTTP {status}, {body}")
        if order_id is None:
            order_id = body["id"]
            print(f"Created check order: {order_id}, user: {user}", flush=True)
        require(body["id"] == order_id, "Idempotent replay created another order")
    require(redis("GET", counter_key) == "30", "Expected 30 counted attempts; rebuild orders with make apps-up")
    ttl = int(redis("PTTL", counter_key))
    require(0 < ttl <= 60000, f"Unexpected limiter TTL: {ttl}")
    status, headers, body = api("/v1/orders", user, key, payload)
    require(status == 429 and body.get("code") == "rate_limit_exceeded", f"Attempt 31: HTTP {status}, {body}")
    retry = headers.get("Retry-After", "")
    require(retry.isdigit() and 1 <= int(retry) <= 60, f"Invalid Retry-After: {retry}")
    require(body.get("request_id") == headers.get("X-Request-ID") and bool(body.get("request_id")), "Missing request ID")
    require(redis("GET", counter_key) == "31", "Rejected attempt was not counted")
    require(0 < int(redis("PTTL", counter_key)) <= ttl, "Request extended the fixed window")
    print(f"OK: 30 attempts, one order; attempt 31 gets 429, Retry-After={retry}", flush=True)

    status, _, other = api("/v1/orders", other_user, key, payload)
    require(status == 201 and other["id"] != order_id, "Independent user was limited")
    print(f"Created independent-user order: {other['id']}, user: {other_user}", flush=True)
    require(api("/v1/orders/" + order_id, user)[0] == 200, "Rate limit affected GET")
    require(api("/readyz")[0] == 200, "Rate limit affected readiness")

    try:
        compose("stop", "redis")
        require(api("/readyz")[0] == 200, "Redis outage broke readiness")
        status, _, replay = api("/v1/orders", user, key, payload)
        require(status == 200 and replay["id"] == order_id, "Redis outage blocked the idempotent request")
        print("OK: Redis outage permits POST and preserves readiness", flush=True)
    finally:
        # Восстанавливаем Redis даже при ошибке HTTP-проверки.
        compose("up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "redis")
    status, _, replay = api("/v1/orders", user, key, payload)
    require(status == 200 and replay["id"] == order_id, "Request failed after Redis restart")
    require(redis("GET", counter_key) == "1", "Limiter did not recover after Redis restart")
    require(0 < int(redis("PTTL", counter_key)) <= 60000, "Recovered key has no TTL")
    print("PASS: HTTP limiter, Retry-After, independent users, fail-open and recovery")
    print("Redis Insight: http://localhost:5540 — key " + counter_key)
    print("The key expires after 60 seconds; two check orders remain in PostgreSQL.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
