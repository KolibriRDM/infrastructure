#!/usr/bin/env bash
# Проверяет Redis и GUI через Go-утилиту инфраструктуры.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir/.."
exec go run ./cmd/devcheck check-redis
