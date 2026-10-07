#!/usr/bin/env bash
# Проверяет retry, DLQ и rebalance на временных топиках и группах настоящей Kafka.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
infrastructure_dir="$(cd -- "$script_dir/.." && pwd)"
workspace_dir="$(cd -- "$infrastructure_dir/.." && pwd)"
compose=(docker compose --env-file "$infrastructure_dir/.env" -f "$infrastructure_dir/deploy/compose/compose.yaml")
"${compose[@]}" up -d --wait --wait-timeout 180 --no-recreate kafka
export KAFKA_TEST_BROKERS="127.0.0.1:19092"
cd "$workspace_dir/notifications-service"
go test -tags=integration -race -count=1 \
  -run '^Test(ConsumerKafka|KafkaDLQRecovery|KafkaConsumerRebalance)$' \
  -timeout=180s -v ./internal/kafkaconsumer
