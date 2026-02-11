#!/usr/bin/env bash
set -euo pipefail

# Запуск бота (polling) и вебхука (uvicorn) в одном контейнере
python bot.py &
BOT_PID=$!

uvicorn webhook:app --host 0.0.0.0 --port 8000 &
UVICORN_PID=$!

cleanup() {
  kill "$BOT_PID" "$UVICORN_PID" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

# Если любой из процессов завершился, останавливаем второй и выходим с ошибкой.
# Это позволит Docker restart policy корректно перезапустить контейнер.
wait -n "$BOT_PID" "$UVICORN_PID"
EXIT_CODE=$?
cleanup
exit "$EXIT_CODE"

