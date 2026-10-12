-- Число, сумма и средний размер созданных заказов после исключения дублей.
SELECT count() AS orders_created, sum(total_kopecks) AS sum_kopecks,
    round(avg(total_kopecks), 2) AS average_kopecks
FROM analytics.orders_created;

-- Почасовая динамика создания заказов в UTC.
SELECT toStartOfHour(occurred_at) AS hour_utc, count() AS orders_created,
    sum(total_kopecks) AS sum_kopecks
FROM analytics.orders_created GROUP BY hour_utc ORDER BY hour_utc DESC LIMIT 24;

-- Число повторных доставок, исключённых из расчётов.
SELECT count() AS raw_events, uniqExact(event_id) AS unique_events,
    count() - uniqExact(event_id) AS duplicate_events
FROM analytics.order_events_raw;
