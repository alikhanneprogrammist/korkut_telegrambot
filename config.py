import os
from pathlib import Path
from dotenv import load_dotenv

# Определяем директорию, где находится этот файл
BASE_DIR = Path(__file__).resolve().parent

# Загружаем переменные окружения из .env файла в той же директории
load_dotenv(BASE_DIR / '.env')

# Токен бота (получить у @BotFather)
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')

# ID канала (можно получить через @userinfobot или через API)
# Для приватного канала используйте формат: -1001234567890
CHANNEL_ID = int(os.getenv('CHANNEL_ID', '0'))

# Ссылка на канал (пригласительная ссылка)
CHANNEL_LINK = os.getenv('CHANNEL_LINK', '')

# ID администратора (ваш Telegram ID)
ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))
# Список админов (через запятую). ADMIN_ID будет добавлен для совместимости.
ADMIN_IDS = []
raw_admin_ids = os.getenv("ADMIN_IDS", "")
if raw_admin_ids:
    try:
        ADMIN_IDS = [int(x.strip()) for x in raw_admin_ids.split(",") if x.strip()]
    except ValueError:
        ADMIN_IDS = []
if ADMIN_ID and ADMIN_ID not in ADMIN_IDS:
    ADMIN_IDS.append(ADMIN_ID)

# === Robokassa ===
# Идентификатор магазина в Robokassa
ROBOKASSA_MERCHANT_LOGIN = os.getenv('ROBOKASSA_MERCHANT_LOGIN', '')

# Пароль #1 (для формирования подписи запроса на оплату)
ROBOKASSA_PASSWORD_1 = os.getenv('ROBOKASSA_PASSWORD_1', '')

# Пароль #2 (для проверки подписи уведомления об оплате)
ROBOKASSA_PASSWORD_2 = os.getenv('ROBOKASSA_PASSWORD_2', '')

# Тестовый режим Robokassa (True - тестовый, False - боевой)
ROBOKASSA_TEST_MODE = os.getenv('ROBOKASSA_TEST_MODE', 'True').lower() in ('true', '1', 'yes')

# Цена подписки в тенге
SUBSCRIPTION_PRICE = int(os.getenv('SUBSCRIPTION_PRICE', '20000'))

# Период продления подписки (в днях), для автосписаний
RENEWAL_PERIOD_DAYS = int(os.getenv('RENEWAL_PERIOD_DAYS', '30'))

# === Postgres ===
# Строка подключения к БД, например:
# postgresql+psycopg2://telegram_user:password@localhost:5432/telegram_sales
DATABASE_URL = os.getenv('DATABASE_URL', '')
