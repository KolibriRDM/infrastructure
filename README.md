# Food Delivery Infrastructure

Отдельный Git-репозиторий для локального PostgreSQL, совместных команд и сквозных тестов.
Сервисы остаются самостоятельными проектами. Go-модуль здесь нужен только
сквозным тестам; он не импортирует код сервисов.

## Расположение

```text
food-delivery/
  catalog-service/     свой .git, go.mod, миграции и Dockerfile
  orders-service/      свой .git, go.mod, миграции и Dockerfile
  infrastructure/      этот репозиторий
```

Compose монтирует миграции из соседних репозиториев. Скрипты собирают каждый
бинарник в его собственном модуле. Общий go.work и локальные replace не нужны.
HTTP-модели сквозных тестов объявлены отдельно от реализаций сервисов.

## Запуск

```sh
cp .env.example .env # только если .env ещё отсутствует
make db-up
make migrate
make seed-catalog
```

При разделении прежний `.env` уже скопирован с сохранением паролей.
Затем в двух терминалах: `make -C ../catalog-service run` и
`make -C ../orders-service run`. HTTP-порты: catalog 8082, orders 8080.
Compose приложений, gRPC, Kafka и notifications — следующие этапы.

## Существующие данные

Имя Compose-проекта остаётся `food-delivery`; volume — `food-delivery_postgres_data`.
Порт PostgreSQL сохраняется: `127.0.0.1:15432`. Разделение исходников не переносит
и не очищает БД. Роли catalog_app, orders_app и notifications_app разделены;
последняя база подготовлена заранее, сам сервис пока не реализован.

Init-скрипт создаёт роли и базы только на пустом volume. Его изменение не обновит
пароли существующих ролей. Обычный `docker compose ... down` сохраняет volume;
`down -v` удаляет данные и для обычной остановки не нужен.

```sh
docker compose --env-file .env -f deploy/compose/compose.yaml ps
docker compose --env-file .env -f deploy/compose/compose.yaml logs postgres
make catalog-version
make orders-version
```

## Проверки

```sh
make test-catalog-postgres
make test-orders-postgres
make test-day2
```

Скрипты создают отдельные БД с уникальными именами, применяют миграции и удаляют
их через EXIT trap. Рабочие таблицы не изменяются. При SIGKILL или отказе Docker
удаление временных БД не гарантируется. Используется race detector.

Сквозной тест проверяет HTTP-связь двух процессов, идемпотентность (включая
10 конкурентных повторов), права чтения, снимки цен, outbox, rollback и рестарт
orders при остановленном catalog. Фикстуры не подменяют источник данных приложения.

`.env` содержит настройки локального стенда; сервисы используют собственные `.env`
только со своими настройками. После изменения паролей обновляй соответствующие
локальные файлы согласованно. Реальные секреты в Git не входят.
