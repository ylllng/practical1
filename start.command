#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
command -v docker >/dev/null || { echo 'Установите и запустите Docker Desktop'; exit 1; }
docker info >/dev/null
mkdir -p results
docker compose up -d --build
python3 run.py "$@"
