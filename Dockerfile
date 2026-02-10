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

# Скрипт запускает и бота (polling), и вебхук на uvicorn
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["./start.sh"]

