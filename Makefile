COMPOSE = docker compose --env-file .env -f deploy/compose/compose.yaml

.PHONY: check-menu-cache redis-up redis-cli test-redis test-business test-notifications-kafka apps-start up apps-up apps-stop status logs demo-order test-events-e2e test-orders-relay test-notifications-postgres help db-up migrate-notifications notifications-version kafka-up kafka-topics kafka-describe migrate migrate-catalog catalog-version migrate-orders orders-version seed-catalog test-catalog-postgres test-orders-postgres test-day2
help:
	@printf '%s\n' 'make up / status / logs / apps-stop' 'make demo-order' 'make redis-up / redis-cli / test-redis' 'make db-up / migrate / seed-catalog' 'make migrate-notifications / notifications-version' 'make kafka-up / kafka-topics / kafka-describe' 'make test-orders-relay / test-events-e2e / test-notifications-kafka' 'make test-catalog-postgres / test-orders-postgres / test-notifications-postgres / test-business'
db-up:
	$(COMPOSE) up -d --wait postgres
migrate: migrate-catalog migrate-orders migrate-notifications
migrate-catalog:
	$(COMPOSE) run --rm -T migrate-catalog up
catalog-version:
	$(COMPOSE) run --rm -T migrate-catalog version
migrate-orders:
	$(COMPOSE) run --rm -T migrate-orders up
orders-version:
	$(COMPOSE) run --rm -T migrate-orders version
seed-catalog:
	$(COMPOSE) exec -T postgres sh -ec 'export PGPASSWORD="$${CATALOG_DB_PASSWORD}"; exec psql -X -w -h 127.0.0.1 -U catalog_app -d catalog_db -v ON_ERROR_STOP=1' < scripts/seed-catalog.sql
	@$(COMPOSE) exec -T redis redis-cli -e DEL menu:v1:00000000-0000-4000-8000-000000000001 menu:v1:00000000-0000-4000-8000-000000000002 >/dev/null 2>&1 || printf '%s\n' 'Redis cache invalidation skipped: Redis unavailable; existing menu keys will expire by TTL.'
test-catalog-postgres:
	./scripts/test-catalog-postgres.sh
test-orders-postgres:
	./scripts/test-orders-postgres.sh
test-day2:
	./scripts/test-day2.sh

# Запускает Kafka и интерфейс управления.
kafka-up:
	$(COMPOSE) --profile ui up -d --wait --wait-timeout 180 kafka kafka-ui

# Создаёт отсутствующие топики заказов и ошибочных событий.
kafka-topics: kafka-up
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --create --if-not-exists \
		--topic food.order-created.v1 --partitions 3 --replication-factor 1 \
		--config cleanup.policy=delete --config retention.ms=604800000
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --create --if-not-exists \
		--topic food.order-created.dlq.v1 --partitions 3 --replication-factor 1 \
		--config cleanup.policy=delete --config retention.ms=1209600000

# Показывает партиции и настройки топиков проекта.
kafka-describe:
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --describe --topic food.order-created.v1
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --describe --topic food.order-created.dlq.v1

# Применяет миграции базы сервиса уведомлений.
migrate-notifications:
	$(COMPOSE) run --rm -T migrate-notifications up

# Показывает текущую версию миграций уведомлений.
notifications-version:
	$(COMPOSE) run --rm -T migrate-notifications version

# Проверяет репозиторий уведомлений на временной базе.
test-notifications-postgres:
	bash ./scripts/test-notifications-postgres.sh

# Проверяет relay и восстановление после отказа отдельного тестового брокера.
test-orders-relay:
	bash ./scripts/test-orders-relay.sh

# Проверяет всю цепочку заказа и уведомления с настоящей Kafka.
test-events-e2e:
	bash ./scripts/test-events-e2e.sh

# Подготавливает БД, миграции, меню и топики перед запуском приложений.
up:
	$(MAKE) db-up
	$(MAKE) migrate
	$(MAKE) seed-catalog
	$(MAKE) kafka-topics
	$(MAKE) redis-up
	$(MAKE) apps-up

# Собирает и запускает приложения после подготовки инфраструктуры.
apps-up:
	$(COMPOSE) up -d --build --wait --wait-timeout 120 catalog orders notifications

# Останавливает приложения, сохраняя инфраструктуру и её данные.
apps-stop:
	$(COMPOSE) stop catalog orders notifications

# Показывает состояние приложений, инфраструктуры и GUI.
status:
	$(COMPOSE) --profile ui ps

# Показывает последние сообщения приложений и продолжает чтение.
logs:
	$(COMPOSE) logs --tail=100 -f catalog orders notifications

# Создаёт демонстрационный заказ и ждёт уведомление для просмотра в GUI.
demo-order:
	python3 scripts/demo-order.py

# Запускает ранее собранные образы без обращения к registry для сборки.
apps-start:
	$(COMPOSE) up -d --no-build --pull never --wait --wait-timeout 120 catalog orders notifications

# Проверяет retry, DLQ и смену владельцев партиций на настоящем брокере.
test-notifications-kafka:
	bash ./scripts/test-notifications-kafka.sh

# Проверяет PATCH меню, снимки заказов, статусы и права на двух процессах.
test-business:
	bash ./scripts/test-day2.sh

# Запускает Redis и его GUI с проверкой готовности.
redis-up:
	$(COMPOSE) --profile ui up -d --wait --wait-timeout 120 redis redis-insight

# Открывает интерактивную консоль Redis.
redis-cli:
	$(COMPOSE) exec redis redis-cli

# Проверяет запись, TTL, удаление ключа и готовность GUI.
test-redis: redis-up
	bash ./scripts/test-redis.sh

# Проверяет кэш на запущенном стенде и восстанавливает цену и Redis.
check-menu-cache:
	python3 scripts/check-menu-cache.py
