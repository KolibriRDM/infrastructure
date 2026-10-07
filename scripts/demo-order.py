#!/usr/bin/env python3
"""Создаёт заказ и выводит идентификаторы для проверки события в Kafka UI."""

import json
import sys
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

USER_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


# Выполняет запрос к локальному HTTP API с ограничением времени.
def request_json(url, payload=None, key=None):
    headers = {"X-Demo-User-ID": USER_ID, "Accept": "application/json"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    if key:
        headers["Idempotency-Key"] = key
    request = Request(url, data=data, headers=headers)
    with urlopen(request, timeout=3) as response:
        return json.load(response)


# Ожидает уведомление, созданное из настоящего события заказа.
def main():
    order = request_json(
        "http://127.0.0.1:8080/v1/orders",
        {
            "restaurant_id": "00000000-0000-4000-8000-000000000001",
            "delivery_address": "Москва, Демонстрационная, 1",
            "items": [{"menu_item_id": "00000000-0000-4000-8000-000000000101", "quantity": 1}],
        },
        "compose-demo-" + str(uuid.uuid4()),
    )
    order_id = order["id"]
    print("Создан заказ:", order_id, flush=True)
    deadline = time.monotonic() + 30
    url = f"http://127.0.0.1:8081/v1/orders/{order_id}/notifications"
    while time.monotonic() < deadline:
        items = request_json(url)["items"]
        if items:
            if len(items) != 1:
                raise RuntimeError(f"Ожидалось одно уведомление, получено {len(items)}")
            notification = items[0]
            if notification["order_id"] != order_id or notification["user_id"] != USER_ID:
                raise RuntimeError("Уведомление не соответствует заказу")
            print(json.dumps({
                "order_id": order_id,
                "event_id": notification["event_id"],
                "notification_id": notification["id"],
                "topic": "food.order-created.v1",
                "consumer_group": "notifications-v1",
                "kafka_ui": "http://localhost:8083",
            }, ensure_ascii=False, indent=2))
            return
        time.sleep(0.25)
    raise RuntimeError(f"Уведомление для {order_id} не появилось за 30 секунд; проверь make logs")


if __name__ == "__main__":
    try:
        main()
    except HTTPError as exc:
        print(f"HTTP {exc.code}: {exc.read(4096).decode(errors='replace')}", file=sys.stderr)
        sys.exit(1)
    except (URLError, TimeoutError, OSError, ValueError, KeyError, RuntimeError) as exc:
        print(f"Проверка не завершена: {exc}", file=sys.stderr)
        sys.exit(1)
