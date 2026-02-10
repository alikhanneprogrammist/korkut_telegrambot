import logging
import hashlib
import urllib.parse
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import NetworkError as TelegramNetworkError
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    ChatJoinRequestHandler,
    filters,
)

# ✅ NEW: request таймауты для polling
from telegram.request import HTTPXRequest

from robokassa import Robokassa, HashAlgorithm

from database import Database
from config import (
    TELEGRAM_TOKEN,
    CHANNEL_ID,
    CHANNEL_LINK,
    ADMIN_ID,
    ADMIN_IDS,
    DATABASE_URL,
    ROBOKASSA_MERCHANT_LOGIN,
    ROBOKASSA_PASSWORD_1,
    ROBOKASSA_PASSWORD_2,
    ROBOKASSA_TEST_MODE,
    SUBSCRIPTION_PRICE,
    RENEWAL_PERIOD_DAYS,
)

# Ссылка на договор оферты
OFFER_AGREEMENT_URL = "https://drive.google.com/file/d/1Y86DaO-KKsDoAiwPEXU-dHuDht8X13tM/view"

# Ссылка на политику конфиденциальности
PRIVACY_POLICY_URL = "https://drive.google.com/file/d/1BuO7HQnGaJY__HiPV-CV_pj2JkA2dFTp/view?usp=drivesdk"

# Изображения для шагов воронки
BASE_DIR = Path(__file__).resolve().parent
WELCOME_IMAGE_PATH = BASE_DIR / "приветсвие.jpeg"
PROGRAM_IMAGE_PATH = BASE_DIR / "основная фото.jpeg"


# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

robokassa_client: Optional[Robokassa] = None
ADMIN_SET = set(ADMIN_IDS or [])

TEXTS = {
    "start": """Привет! Это Korkut ipoteka — закрытый канал для ипотечных брокеров и риелторов.

Если ты:
— боишься ошибиться в сделке
— не всегда уверен(а) в выборе банка
— теряешь время на поиск актуальных условий
— хочешь работать спокойно и уверенно""",

    "story2": """В ипотеке чаще всего ломает сделку не клиент, а:
— устаревшая информация
— неверная стратегия
— отсутствие поддержки в сложный момент.

Korkut ipoteka создан, чтобы ты не оставался(лась) с этим один на один.""",

    "story3": """Я — практикующий ипотечный брокер с 9-летним опытом.
Каждый день сопровождаю реальные сделки и вижу, где чаще всего теряют клиентов и деньги.

В Korkut ipoteka — только практика и то, что реально работает.""",

    "story4": """Что внутри канала Korkut ipoteka:

✔ актуальные ипотечные программы
✔ изменения по банкам без поиска по чатам
✔ разборы реальных кейсов
✔ помощь в сложных сделках

Это не обучение. Это рабочий инструмент.""",

    "story5": """Кейс из практики 👇
После отказа в двух банках клиент получил одобрение с лучшими условиями — за счёт правильной стратегии.

В канале Korkut ipoteka такие ситуации разбираются регулярно.""",

    "story6": """Одна ошибка в ипотеке может стоить десятков тысяч тенге и репутации.

💳 Подписка на Korkut ipoteka — {price} тг / месяц

Ты получаешь:
— актуальную информацию
— поддержку и разборы
— уверенность в каждой сделке""",

    "story7": """Можно дальше разбираться в ипотеке самостоятельно.
А можно быть в среде, где ответы уже есть.

Korkut ipoteka — про спокойную и уверенную работу.""",

    "want": """В ипотеке чаще всего ломает сделку не клиент, а:
— устаревшая информация
— неверная стратегия
— отсутствие поддержки в сложный момент.

Korkut ipoteka создан, чтобы ты не оставался(лась) с этим один на один.""",

    "questions": """С какими сложностями по ипотеке ты сейчас сталкиваешься?

Напиши одним сообщением — я подскажу, решается ли это внутри канала.""",

    "questions_reply": """Я — практикующий ипотечный брокер.
Каждый день сопровождаю реальные сделки и вижу, где чаще всего теряют клиентов и деньги.

В Korkut ipoteka — только практика и то, что реально работает.""",

    "details": """Что внутри канала Korkut ipoteka:

✔ актуальные ипотечные программы
✔ изменения по банкам без поиска по чатам
✔ разборы реальных кейсов
✔ помощь в сложных сделках

Это не обучение. Это рабочий инструмент.""",

    "offer_agreement": """💳 Подписка на канал Korkut Ipoteka
Стоимость — {price} ₸ / месяц
Автопродление каждый месяц
Отписаться можно в любой момент

Нажимая «Оплатить», я соглашаюсь на регулярные списания, на обработку персональных данных и принимаю условия публичной оферты:
""",

    "payment": """Одна ошибка в ипотеке может стоить десятков тысяч тенге и репутации.

💳 Подписка на Korkut ipoteka — {price} тг / месяц

Ты получаешь:
— актуальную информацию
— поддержку и разборы
— уверенность в каждой сделке
""",

    "after_payment": """Оплата прошла успешно ✅
Доступ к каналу Korkut ipoteka открыт.
Спасибо, что вы с нами!""",

    "retarget_24h": """Я — практикующий ипотечный брокер.
Каждый день сопровождаю реальные сделки и вижу, где чаще всего теряют клиентов и деньги.

В Korkut ipoteka — только практика и то, что реально работает.""",

    "retarget_48h": """Кейс из практики 👇
После отказа в двух банках клиент получил одобрение с лучшими условиями — за счёт правильной стратегии.

В канале Korkut ipoteka такие ситуации разбираются регулярно.""",

    "retarget_72h": """Можно дальше разбираться в ипотеке самостоятельно.
А можно быть в среде, где ответы уже есть.

Korkut ipoteka — про спокойную и уверенную работу.""",
}


def init_robokassa() -> Optional[Robokassa]:
    """Инициализация клиента Robokassa"""
    if not all([ROBOKASSA_MERCHANT_LOGIN, ROBOKASSA_PASSWORD_1, ROBOKASSA_PASSWORD_2]):
        logger.error("Не все параметры Robokassa настроены!")
        return None

    return Robokassa(
        merchant_login=ROBOKASSA_MERCHANT_LOGIN,
        password1=ROBOKASSA_PASSWORD_1,
        password2=ROBOKASSA_PASSWORD_2,
        is_test=ROBOKASSA_TEST_MODE,
        algorithm=HashAlgorithm.md5,
    )


def _now_for(dt: datetime) -> datetime:
    """Текущее время с учётом tzinfo dt (если есть)."""
    return datetime.now(dt.tzinfo) if getattr(dt, "tzinfo", None) else datetime.now()


def is_subscription_active(subscription: Optional[dict]) -> bool:
    """Проверка активности подписки с учётом tzinfo."""
    if not subscription:
        return False
    expires_at = subscription.get("expires_at")
    if not expires_at:
        return False
    return expires_at > _now_for(expires_at)


def format_expires_at(expires_at: datetime) -> str:
    return expires_at.strftime('%d.%m.%Y %H:%M')


def describe_subscription(subscription: dict) -> str:
    expires_at = format_expires_at(subscription["expires_at"])
    if subscription.get("cancel_requested"):
        return (
            f"🔕 Автоплатеж отключён.\n"
            f"Доступ действует до: {expires_at}.\n"
            "После этой даты подписка не продлится автоматически."
        )
    return (
        f"✅ Подписка активна до: {expires_at}.\n"
        "Автопродление включено."
    )


# ✅ Глобальный error handler: сетевые ошибки логируем кратко, остальные — полностью
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, TelegramNetworkError):
        logger.warning(
            "Сетевая ошибка Telegram API (временная, библиотека повторит запрос): %s",
            err,
        )
        return
    logger.exception("Unhandled error", exc_info=err)


async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not chat_id or not message_id:
        return
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning("Не удалось удалить сообщение %s:%s: %s", chat_id, message_id, e)


def schedule_message_deletion(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    delay_seconds: int = 300,
):
    if not context or not getattr(context, "application", None):
        return
    job_queue = context.application.job_queue
    job_queue.run_once(
        delete_message_job,
        when=timedelta(seconds=delay_seconds),
        data={"chat_id": chat_id, "message_id": message_id},
        name=f"del_msg_{chat_id}_{message_id}",
    )


async def reply_with_cleanup(message_obj, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, delete_after: int = 300):
    msg = await message_obj.reply_text(text, reply_markup=reply_markup)
    schedule_message_deletion(context, msg.chat_id, msg.message_id, delete_after)
    return msg


async def bot_send_with_cleanup(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, reply_markup=None, delete_after: int = 300):
    msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    schedule_message_deletion(context, chat_id, msg.message_id, delete_after)
    return msg


def generate_payment_link_manual(
    inv_id: int,
    out_sum: float,
    description: str,
    user_id: int,
    *,
    recurring: bool = False,
) -> str:
    out_sum_str = f"{float(out_sum):.6f}"
    shp_interface = "Shp_interface=link"
    shp_user_id = f"Shp_user_id={user_id}"
    signature_string = (
        f"{ROBOKASSA_MERCHANT_LOGIN}:{out_sum_str}:{inv_id}:"
        f"{ROBOKASSA_PASSWORD_1}:{shp_interface}:{shp_user_id}"
    )
    signature = hashlib.md5(signature_string.encode()).hexdigest()

    enc_description = urllib.parse.quote_plus(description)
    base_url = "https://auth.robokassa.kz/Merchant/Index.aspx"

    params = [
        f"MerchantLogin={ROBOKASSA_MERCHANT_LOGIN}",
        f"OutSum={out_sum_str}",
        f"InvId={inv_id}",
        f"Description={enc_description}",
        f"SignatureValue={signature}",
        "Culture=ru",
        "Encoding=utf-8",
        "Shp_interface=link",
        f"Shp_user_id={user_id}",
    ]
    if recurring:
        params.append("Recurring=true")
    if ROBOKASSA_TEST_MODE:
        params.append("IsTest=1")

    return f"{base_url}?{'&'.join(params)}"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_SET or user_id == ADMIN_ID


def verify_payment_signature(out_sum: str, inv_id: str, signature: str, user_id: str) -> bool:
    """Проверка подписи Result URL Robokassa (Пароль #2). Параметры Shp_ в алфавитном порядке."""
    sig_string = (
        f"{ROBOKASSA_MERCHANT_LOGIN}:{out_sum}:{inv_id}:"
        f"{ROBOKASSA_PASSWORD_2}:Shp_interface=link:Shp_user_id={user_id}"
    )
    expected = hashlib.md5(sig_string.encode()).hexdigest()
    return expected.lower() == (signature or "").strip().lower()


def build_after_payment_keyboard():
    """Клавиатура после оплаты: ссылка на канал и отключение автоплатежа."""
    keyboard = [
        [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")],
    ]
    return InlineKeyboardMarkup(keyboard)


# =====================================================
# ВОРОНКА ПРОДАЖ - ОБРАБОТЧИКИ
# =====================================================

async def send_start_block(message_obj, reply_markup):
    caption = TEXTS["start"]
    if WELCOME_IMAGE_PATH.exists():
        with WELCOME_IMAGE_PATH.open("rb") as photo:
            await message_obj.reply_photo(
                photo=photo,
                caption=caption,
                reply_markup=reply_markup
            )
    else:
        await message_obj.reply_text(
            caption,
            reply_markup=reply_markup
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user is None:
        return

    subscription = db.get_subscription(user.id)

    if is_subscription_active(subscription):
        expires_at = format_expires_at(subscription['expires_at'])
        keyboard = [
            [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        status_text = (
            f"🔕 Автоплатеж отключён.\n"
            f"Доступ действует до: {expires_at}\n\n"
            f"🔗 Ссылка на канал ниже 👇"
            if subscription.get("cancel_requested")
            else f"✅ У тебя есть активная подписка до {expires_at}\n\n"
                 f"🔗 Ссылка на канал ниже 👇"
        )

        # ✅ используем effective_message
        msg = update.effective_message
        if msg:
            await reply_with_cleanup(
                msg,
                context,
                f"👋 Привет, {user.first_name}!\n\n{status_text}",
                reply_markup=reply_markup,
            )
        return

    db.update_user_state(user.id, user.username or user.first_name, "start")

    keyboard = [[InlineKeyboardButton("🔘 Это про меня", callback_data="funnel_story2")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    msg = update.effective_message
    if msg:
        await send_start_block(msg, reply_markup)


async def funnel_story2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "story2")

    keyboard = [[InlineKeyboardButton("✨ Хочу без ошибок", callback_data="funnel_story3")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(TEXTS["story2"], reply_markup=reply_markup)


async def funnel_story3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "story3")

    keyboard = [[InlineKeyboardButton("👀 Интересно", callback_data="funnel_story4")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(TEXTS["story3"], reply_markup=reply_markup)


async def funnel_story4(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "story4")

    keyboard = [[InlineKeyboardButton("📥 Хочу доступ", callback_data="funnel_story5")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(TEXTS["story4"], reply_markup=reply_markup)


async def funnel_story5(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "story5")

    keyboard = [[InlineKeyboardButton("✅ Мне это нужно", callback_data="funnel_story6")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(TEXTS["story5"], reply_markup=reply_markup)


async def funnel_story6(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "story6")

    keyboard = [
        [InlineKeyboardButton("💳 Оплатить подписку", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("➡️ Дальше", callback_data="funnel_story7")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(
        TEXTS["story6"].format(price=SUBSCRIPTION_PRICE),
        reply_markup=reply_markup
    )


async def funnel_story7(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "story7")

    keyboard = [[InlineKeyboardButton("🚀 Присоединиться сейчас", callback_data="funnel_offer_agreement")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(TEXTS["story7"], reply_markup=reply_markup)


# ✅ FIXED: handler теперь безопасный и не использует update.message
async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if user is None or message is None or not getattr(message, "text", None):
        return

    subscription = db.get_subscription(user.id)
    if is_subscription_active(subscription):
        keyboard = [
            [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await reply_with_cleanup(
            message,
            context,
            "У тебя есть активная подписка! Вот ссылка на канал 👇",
            reply_markup=reply_markup,
        )
        return

    db.save_user_question(user.id, message.text)
    db.update_user_state(user.id, user.username or user.first_name, "question_answered")

    keyboard = [
        [InlineKeyboardButton("Оформить подписку", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("Узнать подробнее", callback_data="funnel_details")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await message.reply_text(TEXTS["questions_reply"], reply_markup=reply_markup)


# =====================================================
# ЗАПРОС НА ВСТУПЛЕНИЕ В КАНАЛ
# =====================================================

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Одобряем заявку, если у пользователя активная подписка; иначе отклоняем."""
    if not update.chat_join_request:
        return
    user = update.chat_join_request.from_user
    user_id = user.id
    subscription = db.get_subscription(user_id)
    if is_subscription_active(subscription):
        await update.chat_join_request.approve()
        logger.info("Заявка одобрена: user_id=%s", user_id)
    else:
        await update.chat_join_request.decline()
        logger.info("Заявка отклонена (нет подписки): user_id=%s", user_id)


# =====================================================
# ОФЕРТА, ОПЛАТА, ОТМЕНА АВТОПЛАТЕЖА, ПОДРОБНОСТИ
# =====================================================

async def funnel_offer_agreement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать оферту и кнопку оплаты."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "offer_agreement")

    inv_id = int(time.time() * 1000)
    payment_link = generate_payment_link_manual(
        inv_id=inv_id,
        out_sum=float(SUBSCRIPTION_PRICE),
        description="Подписка Korkut ipoteka",
        user_id=user.id,
        recurring=True,
    )
    text = TEXTS["offer_agreement"].format(price=SUBSCRIPTION_PRICE)
    keyboard = [
        [InlineKeyboardButton("Оплатить", url=payment_link)],
        [InlineKeyboardButton("Договор оферты", url=OFFER_AGREEMENT_URL)],
        [InlineKeyboardButton("Политика конфиденциальности", url=PRIVACY_POLICY_URL)],
    ]
    await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def cancel_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отключить автоплатёж по кнопке."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    result = db.request_cancel_subscription(user.id)
    if result:
        desc = describe_subscription(result)
        await query.message.reply_text(f"✅ Автоплатёж отключён.\n\n{desc}")
    else:
        await query.message.reply_text("У вас нет активной подписки.")


async def funnel_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать подробности канала и кнопку оплаты."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "details")
    keyboard = [[InlineKeyboardButton("💳 Оформить подписку", callback_data="funnel_offer_agreement")]]
    await query.message.reply_text(TEXTS["details"], reply_markup=InlineKeyboardMarkup(keyboard))


# =====================================================
# КОМАНДЫ /help И /stats
# =====================================================

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Справка по боту."""
    if not update.effective_message:
        return
    text = (
        "🤖 Korkut ipoteka — бот подписки на закрытый канал.\n\n"
        "Команды:\n"
        "/start — начать или проверить подписку\n"
        "/help — эта справка\n"
        "Если у тебя активная подписка — в /start будет ссылка на канал и кнопка отключения автоплатежа."
    )
    await update.effective_message.reply_text(text)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика (только для админов)."""
    user = update.effective_user
    if not user or not update.effective_message:
        return
    if not is_admin(user.id):
        await update.effective_message.reply_text("Команда только для администратора.")
        return
    stats = db.get_statistics()
    funnel = db.get_funnel_statistics()
    lines = [
        "📊 Статистика",
        f"Пользователей: {stats['total_users']}",
        f"Активных подписок: {stats['active_subscriptions']}",
        f"Истекших (ещё не отменённых): {stats['expired_subscriptions']}",
        f"Платежей: {stats['total_payments']}",
        "",
        "Воронка по шагам:",
    ]
    for state, count in sorted(funnel.items(), key=lambda x: -x[1]):
        lines.append(f"  {state}: {count}")
    await update.effective_message.reply_text("\n".join(lines))


# =====================================================
# ЧАСОВОЙ ПОЯС / ПЛАНИРОВЩИК
# =====================================================

TIMEZONE = pytz.timezone('Asia/Almaty')


# =====================================================
# ЗАПУСК БОТА
# =====================================================

def main():
    global robokassa_client
    global db

    load_dotenv()

    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не установлен!")
        return

    robokassa_client = init_robokassa()
    if robokassa_client:
        logger.info("Robokassa клиент инициализирован")
    else:
        logger.warning("Используем ручной метод создания ссылок")

    if not DATABASE_URL:
        logger.error("DATABASE_URL не установлен!")
        return

    db = Database(DATABASE_URL)
    db.init_database()

    # Таймауты и пул для Telegram API: меньше обрывов при нестабильной сети
    request = HTTPXRequest(
        connect_timeout=15.0,
        read_timeout=60.0,
        write_timeout=60.0,
        pool_timeout=15.0,
    )

    application = ApplicationBuilder().token(TELEGRAM_TOKEN).request(request).build()

    # ✅ NEW: глобальный error handler
    application.add_error_handler(on_error)

    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("stats", cmd_stats))

    # ✅ ВАЖНО: добавь остальные handlers как у тебя были (я не трогал их логику)
    # Ниже только ключевые части, которые менялись для стабильности:

    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    application.add_handler(CallbackQueryHandler(funnel_story2, pattern="^funnel_story2$"))
    application.add_handler(CallbackQueryHandler(funnel_story3, pattern="^funnel_story3$"))
    application.add_handler(CallbackQueryHandler(funnel_story4, pattern="^funnel_story4$"))
    application.add_handler(CallbackQueryHandler(funnel_story5, pattern="^funnel_story5$"))
    application.add_handler(CallbackQueryHandler(funnel_story6, pattern="^funnel_story6$"))
    application.add_handler(CallbackQueryHandler(funnel_story7, pattern="^funnel_story7$"))
    application.add_handler(CallbackQueryHandler(funnel_offer_agreement, pattern="^funnel_offer_agreement$"))
    application.add_handler(CallbackQueryHandler(cancel_subscription, pattern="^cancel_subscription$"))
    application.add_handler(CallbackQueryHandler(funnel_details, pattern="^funnel_details$"))

    # ✅ FIX: сообщения только из ЛИЧКИ (убирает апдейты из каналов/групп)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_user_message)
    )

    logger.info("🤖 Бот запущен и готов к работе!")

    # ✅ FIX: не слушаем ALL_TYPES, только нужные
    application.run_polling(
        allowed_updates=["message", "callback_query", "chat_join_request"]
    )


if __name__ == '__main__':
    main()
