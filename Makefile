COMPOSE = docker compose --env-file .env -f deploy/compose/compose.yaml

.PHONY: help db-up migrate migrate-catalog catalog-version migrate-orders orders-version seed-catalog test-catalog-postgres test-orders-postgres test-day2
help:
	@printf '%s\n' 'make db-up / migrate / seed-catalog' 'make test-catalog-postgres / test-orders-postgres / test-day2'
db-up:
	$(COMPOSE) up -d --wait postgres
migrate: migrate-catalog migrate-orders
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
test-catalog-postgres:
	./scripts/test-catalog-postgres.sh
test-orders-postgres:
	./scripts/test-orders-postgres.sh
test-day2:
	./scripts/test-day2.sh
