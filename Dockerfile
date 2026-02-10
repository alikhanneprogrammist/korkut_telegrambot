FROM python:3.11-slim

# Системные зависимости для psycopg2 и python-telegram-bot
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости и ставим
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код бота
COPY . .

# Запуск: бот в фоне, вебхук на uvicorn в переднем плане
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["sh", "-c", "python bot.py & exec uvicorn webhook:app --host 0.0.0.0 --port 8000"]

