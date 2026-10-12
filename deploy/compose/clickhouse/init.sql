CREATE DATABASE IF NOT EXISTS analytics;

-- Сохраняем полученные события вместе с возможными повторными доставками.
CREATE TABLE IF NOT EXISTS analytics.order_events_raw
(
    event_id UUID,
    occurred_at DateTime64(6, 'UTC'),
    order_id UUID,
    user_id UUID,
    total_kopecks Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_id, occurred_at);

-- Отдельная группа получает те же события независимо от уведомлений.
CREATE TABLE IF NOT EXISTS analytics.order_events_kafka
(
    event_id UUID,
    type String,
    schema_version UInt16,
    occurred_at DateTime64(6, 'UTC'),
    order_id UUID,
    user_id UUID,
    total_kopecks Int64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'food.order-created.v1',
    kafka_group_name = 'analytics-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 1000,
    kafka_flush_interval_ms = 1000,
    date_time_input_format = 'best_effort';

-- Представление переносит Kafka-блоки в постоянную таблицу.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.order_events_ingest
TO analytics.order_events_raw AS
SELECT event_id, occurred_at, order_id, user_id, total_kopecks
FROM analytics.order_events_kafka
WHERE type = 'order.created.v1' AND schema_version = 1;

-- Неизменяемое событие учитывается ровно один раз при каждом чтении аналитики.
CREATE VIEW IF NOT EXISTS analytics.orders_created AS
SELECT event_id, any(occurred_at) AS occurred_at, any(order_id) AS order_id,
    any(user_id) AS user_id, any(total_kopecks) AS total_kopecks
FROM analytics.order_events_raw
GROUP BY event_id;
