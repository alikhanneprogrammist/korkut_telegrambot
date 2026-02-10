#!/usr/bin/env bash
set -euo pipefail

# Запуск бота (polling) и вебхука (uvicorn) в одном контейнере
python bot.py &
BOT_PID=$!

uvicorn webhook:app --host 0.0.0.0 --port 8000 &
UVICORN_PID=$!

wait $BOT_PID $UVICORN_PID

