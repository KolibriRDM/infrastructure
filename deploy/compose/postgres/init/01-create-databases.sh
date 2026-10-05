#!/usr/bin/env bash
# Создаёт отдельные БД и роли сервисов при первом запуске PostgreSQL.
set -euo pipefail

psql \
  --username="$POSTGRES_USER" \
  --dbname="$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=catalog_password="$CATALOG_DB_PASSWORD" \
  --set=orders_password="$ORDERS_DB_PASSWORD" \
  --set=notifications_password="$NOTIFICATIONS_DB_PASSWORD" <<'SQL'
CREATE ROLE catalog_app WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'catalog_password';
CREATE ROLE orders_app WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'orders_password';
CREATE ROLE notifications_app WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD :'notifications_password';

CREATE DATABASE catalog_db OWNER catalog_app;
CREATE DATABASE orders_db OWNER orders_app;
CREATE DATABASE notifications_db OWNER notifications_app;

REVOKE ALL ON DATABASE catalog_db FROM PUBLIC;
REVOKE ALL ON DATABASE orders_db FROM PUBLIC;
REVOKE ALL ON DATABASE notifications_db FROM PUBLIC;
REVOKE CONNECT ON DATABASE postgres, template1 FROM PUBLIC;
SQL
