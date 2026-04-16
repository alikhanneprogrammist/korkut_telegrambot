"""
Telegram бот для приёма платежей через Robokassa
Воронка продаж для ипотечного канала

Использует библиотеку: https://github.com/byBenPuls/robokassa
Установка: pip install robokassa
"""

import logging
import hashlib
import urllib.parse
import time
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import pytz
import httpx

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Conflict, NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    ChatJoinRequestHandler,
    filters,
)

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
    RECURRING_LEAD_DAYS,
    RECURRING_RETRY_DAYS,
    RECURRING_MAX_FAILURES,
)

# Ссылка на договор оферты
OFFER_AGREEMENT_URL = "https://drive.google.com/file/d/1Y86DaO-KKsDoAiwPEXU-dHuDht8X13tM/view"

# Ссылка на политику конфиденциальности
PRIVACY_POLICY_URL = "https://drive.google.com/file/d/1BuO7HQnGaJY__HiPV-CV_pj2JkA2dFTp/view?usp=drivesdk"

# Изображения для шагов воронки
BASE_DIR = Path(__file__).resolve().parent
WELCOME_IMAGE_PATH = BASE_DIR / "приветсвие.jpeg"
PROGRAM_IMAGE_PATH = BASE_DIR / "основная фото.jpeg"


class DropGetUpdatesFilter(logging.Filter):
    """Убирает шумные polling-логи Telegram getUpdates."""
    def filter(self, record: logging.LogRecord) -> bool:
        return "/getUpdates" not in record.getMessage()


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").addFilter(DropGetUpdatesFilter())

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


def _to_local_naive(dt: datetime) -> datetime:
    """Привести datetime к локальному наивному формату для безопасных сравнений."""
    if dt is None:
        return dt
    if getattr(dt, "tzinfo", None):
        return dt.astimezone(TIMEZONE).replace(tzinfo=None)
    return dt


def is_subscription_active(subscription: Optional[dict]) -> bool:
    """Проверка активности подписки с учётом tzinfo."""
    if not subscription:
        return False
    expires_at = subscription.get("expires_at")
    if not expires_at:
        return False
    return expires_at > _now_for(expires_at)


def format_expires_at(expires_at: datetime) -> str:
    """Форматирование даты истечения подписки."""
    return expires_at.strftime('%d.%m.%Y %H:%M')


def describe_subscription(subscription: dict) -> str:
    """Человекочитаемый статус подписки."""
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


async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Удалить сообщение по расписанию."""
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
    """Поставить задачу на удаление сообщения с ссылкой."""
    if not context or not getattr(context, "application", None):
        return
    job_queue = context.application.job_queue
    job_queue.run_once(
        delete_message_job,
        when=timedelta(seconds=delay_seconds),
        data={"chat_id": chat_id, "message_id": message_id},
        name=f"del_msg_{chat_id}_{message_id}",
    )


async def reply_with_cleanup(
    message_obj,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
    delete_after: int = 300,
):
    msg = await message_obj.reply_text(text, reply_markup=reply_markup)
    schedule_message_deletion(context, msg.chat_id, msg.message_id, delete_after)
    return msg


async def bot_send_with_cleanup(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup=None,
    delete_after: int = 300,
):
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
    previous_inv_id: Optional[int] = None,
) -> str:
    """
    Ручное создание ссылки на оплату (KZ хост).
    Формат подписи: MerchantLogin:OutSum:InvId:Password1:Shp_interface=link:Shp_user_id=value
    recurring=True добавляет флаг Recurring, previous_inv_id пробрасывает PreviousInvoiceID.
    """
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
        if previous_inv_id is not None:
            params.append(f"PreviousInvoiceID={previous_inv_id}")
    if ROBOKASSA_TEST_MODE:
        params.append("IsTest=1")

    return f"{base_url}?{'&'.join(params)}"


def verify_payment_signature(out_sum: str, inv_id: str, signature: str, user_id: str) -> bool:
    """Проверка подписи от Robokassa при уведомлении об оплате."""
    shp_interface = "Shp_interface=link"
    shp_user_id = f"Shp_user_id={user_id}"
    expected_string = (
        f"{out_sum}:{inv_id}:{ROBOKASSA_PASSWORD_2}:{shp_interface}:{shp_user_id}"
    )
    expected_signature = hashlib.md5(expected_string.encode()).hexdigest().upper()
    return signature.upper() == expected_signature


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_SET or user_id == ADMIN_ID


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _make_recurring_signature(
    merchant: str,
    out_sum: str,
    inv_id: int,
    password1: str,
    shp: dict | None = None,
) -> str:
    base = f"{merchant}:{out_sum}:{inv_id}:{password1}"
    if shp:
        for k in sorted(shp.keys()):
            base += f":{k}={shp[k]}"
    return _md5(base)


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
    subscription = db.get_subscription(user.id)

    if is_subscription_active(subscription):
        expires_at = format_expires_at(subscription["expires_at"])
        keyboard = [
            [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")],
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

        await reply_with_cleanup(
            update.message,
            context,
            f"👋 Привет, {user.first_name}!\n\n{status_text}",
            reply_markup=reply_markup,
        )
        return

    db.update_user_state(user.id, user.username or user.first_name, "start")
    keyboard = [[InlineKeyboardButton("🔘 Это про меня", callback_data="funnel_story2")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_start_block(update.message, reply_markup)


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


async def funnel_want(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "want")

    keyboard = [[InlineKeyboardButton("👉 Подписка и доступ", callback_data="funnel_offer_agreement")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    caption = TEXTS["want"]
    if PROGRAM_IMAGE_PATH.exists():
        with PROGRAM_IMAGE_PATH.open("rb") as photo:
            await query.message.reply_photo(
                photo=photo,
                caption=caption,
                reply_markup=reply_markup
            )
    else:
        await query.message.reply_text(
            caption,
            reply_markup=reply_markup
        )


async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        logger.warning("Сообщение без effective_user, update_id=%s", getattr(update, "update_id", None))
        return

    subscription = db.get_subscription(user.id)
    if is_subscription_active(subscription):
        keyboard = [
            [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await reply_with_cleanup(
            update.message,
            context,
            "У тебя есть активная подписка! Вот ссылка на канал 👇",
            reply_markup=reply_markup,
        )
        return

    db.save_user_question(user.id, update.message.text)
    db.update_user_state(user.id, user.username or user.first_name, "question_answered")

    keyboard = [
        [InlineKeyboardButton("Оформить подписку", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("Узнать подробнее", callback_data="funnel_details")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        TEXTS["questions_reply"],
        reply_markup=reply_markup
    )


async def send_payment_block(query, context: ContextTypes.DEFAULT_TYPE, text: str):
    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "payment")

    inv_id = int(time.time() * 1000) % 2147483647
    context.user_data["pending_inv_id"] = inv_id
    context.user_data["pending_amount"] = SUBSCRIPTION_PRICE

    description = "Подписка на канал Korkut Ipoteka"

    try:
        payment_link = generate_payment_link_manual(
            inv_id=inv_id,
            out_sum=SUBSCRIPTION_PRICE,
            description=description,
            user_id=user.id,
            recurring=True,
        )

        keyboard = [
            [InlineKeyboardButton("💳 Оплатить", url=payment_link)],
            [InlineKeyboardButton("📄 Публичная оферта", url=OFFER_AGREEMENT_URL)],
            [InlineKeyboardButton("🔒 Политика конфиденциальности", url=PRIVACY_POLICY_URL)],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.message.reply_text(
            text,
            reply_markup=reply_markup
        )

        schedule_retargeting(context, user.id)

    except Exception as e:
        logger.error("Ошибка при создании ссылки на оплату: %s", e)
        await query.message.reply_text(
            "❌ Произошла ошибка при создании ссылки на оплату.\n"
            "Попробуйте позже или обратитесь к администратору."
        )


async def funnel_offer_agreement(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    await send_payment_block(
        query,
        context,
        TEXTS["offer_agreement"].format(
            price=SUBSCRIPTION_PRICE,
            offer_url=OFFER_AGREEMENT_URL,
            privacy_url=PRIVACY_POLICY_URL,
        )
    )


async def funnel_confirm_offer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("✅ Оферта принята! Теперь вы можете оплатить подписку.")

    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "offer_confirmed")
    context.user_data["offer_confirmed"] = True

    await send_payment_block(
        query,
        context,
        TEXTS["offer_agreement"].format(
            price=SUBSCRIPTION_PRICE,
            offer_url=OFFER_AGREEMENT_URL,
            privacy_url=PRIVACY_POLICY_URL,
        )
    )


async def funnel_payment_after_offer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    await send_payment_block(
        query,
        context,
        TEXTS["offer_agreement"].format(
            price=SUBSCRIPTION_PRICE,
            offer_url=OFFER_AGREEMENT_URL,
            privacy_url=PRIVACY_POLICY_URL,
        )
    )


async def funnel_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "details")

    keyboard = [
        [InlineKeyboardButton("👉 Подписка и доступ", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("Назад", callback_data="funnel_back_to_want")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(
        TEXTS["details"].format(price=SUBSCRIPTION_PRICE),
        reply_markup=reply_markup
    )


async def funnel_back_to_want(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "want")

    keyboard = [
        [InlineKeyboardButton("👉 Подписка и доступ", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("Узнать подробнее", callback_data="funnel_details")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(
        TEXTS["want"],
        reply_markup=reply_markup
    )


async def funnel_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    await send_payment_block(
        query,
        context,
        TEXTS["offer_agreement"].format(price=SUBSCRIPTION_PRICE)
    )


async def funnel_doubt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    db.update_user_state(user.id, user.username or user.first_name, "doubt")

    keyboard = [
        [InlineKeyboardButton("Перейти к оформлению", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("Узнать подробнее", callback_data="funnel_details")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.message.reply_text(
        TEXTS["details"].format(price=SUBSCRIPTION_PRICE),
        reply_markup=reply_markup
    )


def schedule_retargeting(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    jobs_to_remove = [
        job for job in context.job_queue.jobs()
        if job.name and job.name.startswith(f"retarget_{user_id}_")
    ]
    for job in jobs_to_remove:
        job.schedule_removal()

    context.job_queue.run_once(
        send_retarget_24h,
        when=timedelta(hours=24),
        data=user_id,
        name=f"retarget_{user_id}_24h"
    )

    context.job_queue.run_once(
        send_retarget_48h,
        when=timedelta(hours=48),
        data=user_id,
        name=f"retarget_{user_id}_48h"
    )

    context.job_queue.run_once(
        send_retarget_72h,
        when=timedelta(hours=72),
        data=user_id,
        name=f"retarget_{user_id}_72h"
    )

    logger.info("Запланирован ретаргетинг для пользователя %s", user_id)


def cancel_retargeting(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    jobs_to_remove = [
        job for job in context.job_queue.jobs()
        if job.name and job.name.startswith(f"retarget_{user_id}_")
    ]
    for job in jobs_to_remove:
        job.schedule_removal()
    logger.info("Отменён ретаргетинг для пользователя %s", user_id)


def build_after_payment_keyboard(include_offer: bool = False) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")],
    ]
    if include_offer:
        keyboard.append([InlineKeyboardButton("📄 Публичная оферта", url=OFFER_AGREEMENT_URL)])
    return InlineKeyboardMarkup(keyboard)


async def send_retarget_24h(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data
    subscription = db.get_subscription(user_id)
    if is_subscription_active(subscription):
        return

    keyboard = [[InlineKeyboardButton("Оформить подписку", callback_data="funnel_offer_agreement")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=TEXTS["retarget_24h"],
            reply_markup=reply_markup
        )
        logger.info("Отправлено напоминание 24ч пользователю %s", user_id)
    except Exception as e:
        logger.warning("Не удалось отправить напоминание 24ч пользователю %s: %s", user_id, e)


async def send_retarget_48h(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data
    subscription = db.get_subscription(user_id)
    if is_subscription_active(subscription):
        return

    keyboard = [
        [InlineKeyboardButton("Да, вступить", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("Сомневаюсь", callback_data="funnel_doubt")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=TEXTS["retarget_48h"],
            reply_markup=reply_markup
        )
        logger.info("Отправлено напоминание 48ч пользователю %s", user_id)
    except Exception as e:
        logger.warning("Не удалось отправить напоминание 48ч пользователю %s: %s", user_id, e)


async def send_retarget_72h(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data
    subscription = db.get_subscription(user_id)
    if is_subscription_active(subscription):
        return

    keyboard = [[InlineKeyboardButton("Оформить подписку", callback_data="funnel_offer_agreement")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=TEXTS["retarget_72h"],
            reply_markup=reply_markup
        )
        logger.info("Отправлено напоминание 72ч пользователю %s", user_id)
    except Exception as e:
        logger.warning("Не удалось отправить напоминание 72ч пользователю %s: %s", user_id, e)


async def check_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    inv_id = query.data.replace("check_payment_", "")

    await query.message.reply_text(
        f"🔍 Проверка оплаты заказа #{inv_id}\n\n"
        f"Если оплата прошла успешно, доступ откроется автоматически.\n\n"
        f"Если возникли проблемы, напишите администратору."
    )


async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not is_admin(user.id):
        await update.message.reply_text("❌ У вас нет доступа к этой команде")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Использование: /confirm_payment <user_id> <inv_id>"
        )
        return

    try:
        target_user_id = int(args[0])
        inv_id = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Неверный формат параметров")
        return

    expires_at = datetime.now() + timedelta(days=RENEWAL_PERIOD_DAYS)
    next_charge_at = expires_at - timedelta(days=RECURRING_LEAD_DAYS)

    db.add_subscription(
        user_id=target_user_id,
        username=f"user_{target_user_id}",
        expires_at=expires_at,
        payment_amount=SUBSCRIPTION_PRICE,
        anchor_inv_id=inv_id,
        next_charge_at=next_charge_at,
    )

    db.add_payment(
        user_id=target_user_id,
        amount=SUBSCRIPTION_PRICE,
        currency="KZT",
        invoice_payload=f"robokassa_{inv_id}",
        inv_id=inv_id,
    )

    db.update_user_state(target_user_id, f"user_{target_user_id}", "paid")
    cancel_retargeting(context, target_user_id)

    logger.info("Подписка активирована админом для пользователя %s", target_user_id)

    await update.message.reply_text(
        f"✅ Оплата подтверждена!\n\n"
        f"👤 Пользователь: {target_user_id}\n"
        f"🧾 Заказ: #{inv_id}\n"
        f"📅 Подписка до: {expires_at.strftime('%d.%m.%Y %H:%M')}"
    )

    try:
        await bot_send_with_cleanup(
            context,
            target_user_id,
            TEXTS["after_payment"].format(channel_link=CHANNEL_LINK),
            reply_markup=build_after_payment_keyboard(),
        )
    except Exception as e:
        logger.warning("Не удалось отправить уведомление пользователю %s: %s", target_user_id, e)


def build_account_keyboard(subscription: Optional[dict]) -> InlineKeyboardMarkup:
    if subscription and is_subscription_active(subscription):
        if subscription.get("cancel_requested"):
            keyboard = [
                [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
                [InlineKeyboardButton("💳 Оформить подписку заново", callback_data="funnel_offer_agreement")],
                [InlineKeyboardButton("📄 Оферта", url=OFFER_AGREEMENT_URL)],
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
                [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")],
                [InlineKeyboardButton("📄 Оферта", url=OFFER_AGREEMENT_URL)],
            ]
    else:
        keyboard = [
            [InlineKeyboardButton("Оформить подписку", callback_data="funnel_offer_agreement")],
            [InlineKeyboardButton("Узнать подробнее", callback_data="funnel_details")],
        ]

    return InlineKeyboardMarkup(keyboard)


async def show_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message_obj = query.message
    else:
        message_obj = update.message

    user = update.effective_user
    subscription = db.get_subscription(user.id)

    if subscription and is_subscription_active(subscription):
        text = (
            "👤 Личный кабинет\n\n"
            f"{describe_subscription(subscription)}"
        )
        await reply_with_cleanup(
            message_obj,
            context,
            text,
            reply_markup=build_account_keyboard(subscription),
        )
        return

    text = (
        "👤 Личный кабинет\n\n"
        "Статус: нет активной подписки.\n"
        "Вы можете оформить доступ к каналу в любой момент."
    )

    await message_obj.reply_text(
        text,
        reply_markup=build_account_keyboard(subscription)
    )


async def cancel_subscription_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        message_obj = query.message
    else:
        message_obj = update.message

    user = update.effective_user
    subscription = db.get_subscription(user.id)

    if not subscription or not is_subscription_active(subscription):
        keyboard = [
            [InlineKeyboardButton("Оформить подписку", callback_data="funnel_offer_agreement")],
            [InlineKeyboardButton("Узнать подробнее", callback_data="funnel_details")],
        ]
        await message_obj.reply_text(
            "❌ У тебя нет активной подписки.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    expires_str = format_expires_at(subscription["expires_at"])

    if subscription.get("cancel_requested"):
        keyboard = [
            [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("👤 Личный кабинет", callback_data="account")],
        ]
        await reply_with_cleanup(
            message_obj,
            context,
            f"🔕 Автоплатёж уже отключён.\nДоступ действует до: {expires_str}.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    db.request_cancel_subscription(user.id)
    cancel_retargeting(context, user.id)

    keyboard = [
        [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton("👤 Личный кабинет", callback_data="account")],
    ]

    await reply_with_cleanup(
        message_obj,
        context,
        "✅ Автоплатёж отключён.\nСписания больше не будут выполняться автоматически.\n"
        f"Доступ к каналу сохранится до: {expires_str}.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    user_id = req.from_user.id
    username = req.from_user.username or req.from_user.first_name or "user"
    subscription = db.get_subscription(user_id)

    if is_subscription_active(subscription):
        await context.bot.approve_chat_join_request(chat_id=req.chat.id, user_id=user_id)
        try:
            await bot_send_with_cleanup(
                context,
                user_id,
                "✅ Доступ в канал подтверждён. Добро пожаловать!",
            )
        except Exception as e:
            logger.warning("Не удалось отправить сообщение после approve %s: %s", user_id, e)
        logger.info("Join approved: user=%s (%s)", user_id, username)
        return

    await context.bot.decline_chat_join_request(chat_id=req.chat.id, user_id=user_id)
    keyboard = [
        [InlineKeyboardButton("Оформить подписку", callback_data="funnel_offer_agreement")],
        [InlineKeyboardButton("Узнать подробнее", callback_data="funnel_details")],
    ]
    try:
        await bot_send_with_cleanup(
            context,
            user_id,
            "❌ Заявка отклонена: активной подписки нет.\nОформите подписку, и доступ будет открыт автоматически.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.warning("Не удалось отправить отказ пользователю %s: %s", user_id, e)
    logger.info("Join declined (no active sub): user=%s (%s)", user_id, username)


async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    subscription = db.get_subscription(user.id)

    if is_subscription_active(subscription):
        expires_at = format_expires_at(subscription["expires_at"])
        keyboard = [
            [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        status_text = (
            f"🔕 Автоплатеж отключён.\n"
            f"Доступ действует до {expires_at}\n\n"
            f"🔗 Ссылка на канал ниже 👇"
            if subscription.get("cancel_requested")
            else f"✅ У тебя уже есть активная подписка до {expires_at}\n\n"
                 f"Вот ссылка на канал 👇"
        )

        await reply_with_cleanup(
            update.message,
            context,
            status_text,
            reply_markup=reply_markup,
        )
        return

    db.update_user_state(user.id, user.username or user.first_name, "offer_agreement")

    inv_id = int(time.time() * 1000) % 2147483647
    context.user_data["pending_inv_id"] = inv_id
    context.user_data["pending_amount"] = SUBSCRIPTION_PRICE

    description = "Подписка на канал Korkut Ipoteka"

    try:
        payment_link = generate_payment_link_manual(
            inv_id=inv_id,
            out_sum=SUBSCRIPTION_PRICE,
            description=description,
            user_id=user.id,
            recurring=True,
        )

        keyboard = [
            [InlineKeyboardButton("💳 Оплатить", url=payment_link)],
            [InlineKeyboardButton("📄 Публичная оферта", url=OFFER_AGREEMENT_URL)],
            [InlineKeyboardButton("🔒 Политика конфиденциальности", url=PRIVACY_POLICY_URL)],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            TEXTS["offer_agreement"].format(
                price=SUBSCRIPTION_PRICE,
                offer_url=OFFER_AGREEMENT_URL,
                privacy_url=PRIVACY_POLICY_URL,
            ),
            reply_markup=reply_markup
        )

        schedule_retargeting(context, user.id)
    except Exception as e:
        logger.error("Ошибка при создании ссылки на оплату: %s", e)
        await update.message.reply_text(
            "❌ Произошла ошибка при создании ссылки на оплату.\n"
            "Попробуйте позже или обратитесь к администратору."
        )


async def check_subscription_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    subscription = db.get_subscription(user.id)

    if subscription:
        expires_at = subscription["expires_at"]
        if is_subscription_active(subscription):
            expires_str = format_expires_at(expires_at)
            keyboard = [
                [InlineKeyboardButton("🔗 Перейти в канал", url=CHANNEL_LINK)],
                [InlineKeyboardButton("🚫 Отключить автоплатёж", callback_data="cancel_subscription")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            status_line = (
                f"🔕 Автоплатеж отключён\n"
                f"📅 Доступ до: {expires_str}"
                if subscription.get("cancel_requested")
                else f"✅ У тебя есть активная подписка\n\n"
                     f"📅 Действует до: {expires_str}"
            )

            await reply_with_cleanup(
                update.message,
                context,
                status_line,
                reply_markup=reply_markup,
            )
        else:
            keyboard = [[InlineKeyboardButton("👉 Подписка и доступ", callback_data="funnel_offer_agreement")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"❌ Твоя подписка истекла {expires_at.strftime('%d.%m.%Y %H:%M')}\n\n"
                f"Нажми кнопку ниже, чтобы продлить 👇",
                reply_markup=reply_markup
            )
    else:
        keyboard = [[InlineKeyboardButton("👉 Что внутри", callback_data="funnel_want")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "❌ У тебя нет активной подписки\n\n"
            "Хочешь узнать, что внутри канала? 👇",
            reply_markup=reply_markup
        )


async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not is_admin(user.id):
        await update.message.reply_text("❌ У вас нет доступа к этой команде")
        return

    stats = db.get_statistics()
    funnel_stats = db.get_funnel_statistics()

    mode = "🧪 ТЕСТОВЫЙ" if ROBOKASSA_TEST_MODE else "💳 БОЕВОЙ"

    await update.message.reply_text(
        f"📊 Статистика бота:\n\n"
        f"👥 Всего пользователей: {stats['total_users']}\n"
        f"✅ Активных подписок: {stats['active_subscriptions']}\n"
        f"❌ Истекших подписок: {stats['expired_subscriptions']}\n"
        f"💰 Всего платежей: {stats['total_payments']}\n\n"
        f"📈 Воронка продаж:\n"
        f"• Начали: {funnel_stats.get('start', 0)}\n"
        f"• Нажали 'Хочу': {funnel_stats.get('want', 0)}\n"
        f"• Дошли до оплаты: {funnel_stats.get('payment', 0)}\n"
        f"• Оплатили: {funnel_stats.get('paid', 0)}\n\n"
        f"Режим Robokassa: {mode}"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📚 Доступные команды:\n\n"
        "/start - Начать работу с ботом\n"
        "/subscribe - Оформить подписку\n"
        "/check - Проверить статус подписки\n"
        "/account - Личный кабинет\n"
        "/unsubscribe - Отключить автоплатёж\n"
        "/help - Показать справку\n"
    )

    if is_admin(update.effective_user.id):
        help_text += (
            "\n👑 Команды администратора:\n"
            "/stats - Статистика бота\n"
            "/confirm_payment <user_id> <inv_id> - Подтвердить оплату\n"
            "/check_subs - Ручная проверка подписок\n"
        )

    await update.message.reply_text(help_text)


TIMEZONE = pytz.timezone("Asia/Almaty")
RECURRING_LEAD_TIME = timedelta(days=RECURRING_LEAD_DAYS)
RECURRING_RETRY_DELAY = timedelta(days=RECURRING_RETRY_DAYS)


async def perform_recurring_charge(
    user_id: int,
    previous_inv_id: int,
    amount: float,
    *,
    new_inv_id: int,
    description: str = "Подписка на канал Korkut Ipoteka",
) -> tuple[bool, Optional[str]]:
    """
    Дочерний рекуррентный платёж Robokassa.
    OK от Robokassa = операция создана, а не факт списания.
    """
    out_sum_str = f"{float(amount):.6f}"

    shp = {"Shp_user_id": str(user_id), "Shp_interface": "link"}
    signature = _make_recurring_signature(
        ROBOKASSA_MERCHANT_LOGIN,
        out_sum_str,
        new_inv_id,
        ROBOKASSA_PASSWORD_1,
        shp=shp,
    )

    payload = {
        "MerchantLogin": ROBOKASSA_MERCHANT_LOGIN,
        "InvoiceID": str(new_inv_id),
        "PreviousInvoiceID": str(previous_inv_id),
        "OutSum": out_sum_str,
        "Description": description,
        "SignatureValue": signature,
        "Shp_interface": "link",
        "Shp_user_id": str(user_id),
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post("https://auth.robokassa.kz/Merchant/Recurring", data=payload)

        if resp.status_code == 200 and resp.text.strip().startswith("OK"):
            return True, None

        return False, f"Recurring failed: {resp.status_code} {resp.text}"
    except Exception as e:
        return False, f"Recurring exception: {e}"


async def process_recurring_charges(context: ContextTypes.DEFAULT_TYPE):
    """
    Ежедневная обработка автосписаний.

    Логика:
    - создаём pending за RECURRING_LEAD_TIME до expires_at
    - если pending уже есть, новый recurring не создаём
    - expires_at не трогаем до подтверждения через Result URL
    """
    now_local = datetime.now(TIMEZONE).replace(tzinfo=None)
    charge_window_end = now_local + RECURRING_LEAD_TIME

    subs = db.get_recurring_candidates()

    for sub in subs:
        if sub.get("cancel_requested"):
            continue

        user_id = sub["user_id"]
        username = sub.get("username", "Пользователь")
        anchor_inv_id = sub.get("anchor_inv_id")
        expires_at = _to_local_naive(sub.get("expires_at"))
        next_charge_at = _to_local_naive(sub.get("next_charge_at"))

        if not anchor_inv_id or not expires_at:
            continue

        current_sub = db.get_subscription(user_id)
        if not current_sub:
            continue

        if expires_at > charge_window_end:
            continue

        if next_charge_at and next_charge_at > now_local:
            continue

        if current_sub.get("pending_inv_id"):
            logger.info(
                "Skip recurring: pending exists user=%s pending_inv_id=%s",
                user_id,
                current_sub.get("pending_inv_id"),
            )
            continue

        new_inv_id = int(time.time() * 1000) % 2147483647

        success, error = await perform_recurring_charge(
            user_id=user_id,
            previous_inv_id=anchor_inv_id,
            amount=SUBSCRIPTION_PRICE,
            new_inv_id=new_inv_id,
            description="Подписка на канал Korkut Ipoteka",
        )

        if success:
            db.set_pending_charge(
                user_id=user_id,
                pending_inv_id=new_inv_id,
                amount=float(SUBSCRIPTION_PRICE),
                created_at=now_local,
            )

            db.update_charge_schedule(
                user_id=user_id,
                next_charge_at=now_local + RECURRING_RETRY_DELAY,
                anchor_inv_id=anchor_inv_id,
            )

            logger.info(
                "Recurring created: user=%s anchor=%s new_inv_id=%s (pending set)",
                user_id,
                anchor_inv_id,
                new_inv_id,
            )
        else:
            failures = db.increment_recurring_failures(user_id)
            logger.warning(
                "Recurring failed: user=%s anchor=%s failures=%s error=%s",
                user_id,
                anchor_inv_id,
                failures,
                error,
            )

            warn_text = (
                "❌ Не удалось выполнить автосписание.\n"
                f"Попытка {failures} из {RECURRING_MAX_FAILURES}.\n"
                "Попробуйте оплатить вручную через кнопку ниже."
            )
            keyboard = [[InlineKeyboardButton("Оплатить", callback_data="funnel_offer_agreement")]]

            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=warn_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
            except Exception as e:
                logger.warning("Не удалось отправить предупреждение пользователю %s: %s", user_id, e)

            if ADMIN_SET or ADMIN_ID:
                for admin_id in (ADMIN_SET or {ADMIN_ID}):
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=(
                                "❌ Автосписание не удалось: "
                                f"user={user_id}, attempt={failures}/{RECURRING_MAX_FAILURES}, err={error}"
                            ),
                        )
                    except Exception:
                        pass

            if failures >= RECURRING_MAX_FAILURES:
                db.clear_pending_charge(user_id)
                db.deactivate_subscription(user_id)
                await kick_user_from_channel(context, user_id, username)

                if ADMIN_SET or ADMIN_ID:
                    for admin_id in (ADMIN_SET or {ADMIN_ID}):
                        try:
                            await context.bot.send_message(
                                chat_id=admin_id,
                                text=(
                                    f"🚫 Пользователь исключен после {failures} неудачных автосписаний: "
                                    f"user={user_id}"
                                ),
                            )
                        except Exception:
                            pass
                continue

            db.update_charge_schedule(
                user_id=user_id,
                next_charge_at=now_local + RECURRING_RETRY_DELAY,
                anchor_inv_id=anchor_inv_id,
            )


async def check_expired_subscriptions(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🔍 Запуск ежедневной проверки подписок...")

    kicked_count = 0
    warned_count = 0

    try:
        all_subscriptions = db.get_all_active_subscriptions()

        for sub in all_subscriptions:
            user_id = sub["user_id"]
            username = sub.get("username", "Пользователь")
            expires_at = sub["expires_at"]
            now_local = _now_for(expires_at)

            if expires_at <= now_local:
                if sub.get("pending_inv_id"):
                    pending_created_at = _to_local_naive(sub.get("pending_created_at"))
                    pending_is_fresh = (
                        pending_created_at is not None
                        and (now_local - pending_created_at) <= RECURRING_RETRY_DELAY
                    )

                    if pending_is_fresh:
                        logger.info(
                            "Expired but fresh pending exists, skip kick: user=%s pending_inv_id=%s pending_created_at=%s",
                            user_id,
                            sub.get("pending_inv_id"),
                            pending_created_at,
                        )
                        continue

                    # Pending завис: очищаем блокировку кика и исключаем пользователя.
                    db.clear_pending_charge(user_id)
                    logger.warning(
                        "Expired with stale pending, clearing pending and kicking user=%s pending_inv_id=%s pending_created_at=%s",
                        user_id,
                        sub.get("pending_inv_id"),
                        pending_created_at,
                    )

                await kick_user_from_channel(context, user_id, username)
                kicked_count += 1

        logger.info(
            "✅ Проверка завершена: предупреждений отправлено: %s, кикнуто: %s",
            warned_count,
            kicked_count,
        )

        if (ADMIN_SET or ADMIN_ID) and (kicked_count > 0 or warned_count > 0):
            for admin_id in (ADMIN_SET or {ADMIN_ID}):
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=f"📊 Ежедневная проверка подписок:\n\n"
                             f"⚠️ Предупреждений отправлено: {warned_count}\n"
                             f"🚫 Пользователей кикнуто: {kicked_count}"
                    )
                except Exception:
                    pass

    except Exception as e:
        logger.error("Ошибка при проверке подписок: %s", e)
        if ADMIN_SET or ADMIN_ID:
            for admin_id in (ADMIN_SET or {ADMIN_ID}):
                try:
                    await context.bot.send_message(
                        chat_id=admin_id,
                        text=f"❌ Ошибка при проверке подписок:\n{e}"
                    )
                except Exception:
                    pass


async def send_expiration_warning(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    days_left: int,
    expires_at: datetime,
):
    if days_left == 3:
        message = (
            f"Напоминание: через 3 дня произойдёт автоматическое списание {SUBSCRIPTION_PRICE} ₸ "
            f"за доступ к Korkut Ipoteka."
        )
    else:
        return

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=message,
        )
        logger.info("Отправлено предупреждение пользователю %s (осталось %s дней)", user_id, days_left)
    except Exception as e:
        logger.warning("Не удалось отправить предупреждение пользователю %s: %s", user_id, e)


async def kick_user_from_channel(context: ContextTypes.DEFAULT_TYPE, user_id: int, username: str):
    try:
        await context.bot.ban_chat_member(
            chat_id=CHANNEL_ID,
            user_id=user_id
        )

        await context.bot.unban_chat_member(
            chat_id=CHANNEL_ID,
            user_id=user_id
        )

        db.deactivate_subscription(user_id)

        logger.info("Пользователь %s (%s) кикнут из канала (подписка истекла)", user_id, username)

        keyboard = [[InlineKeyboardButton("🔄 Продлить подписку", callback_data="funnel_offer_agreement")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await context.bot.send_message(
            chat_id=user_id,
            text="❌ Ваша подписка истекла.\n\n"
                 "Доступ к закрытому каналу приостановлен.\n\n"
                 "Чтобы вернуться, продлите подписку 👇",
            reply_markup=reply_markup
        )

    except Exception as e:
        logger.error("Ошибка при кике пользователя %s: %s", user_id, e)


async def manual_check_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not is_admin(user.id):
        await update.message.reply_text("❌ У вас нет доступа к этой команде")
        return

    await update.message.reply_text("🔍 Запускаю проверку подписок...")
    await check_expired_subscriptions(context)
    await update.message.reply_text("✅ Проверка завершена!")


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error

    if isinstance(err, Conflict):
        logger.error(
            "Telegram polling conflict: запущено несколько инстансов бота "
            "(terminated by other getUpdates request)."
        )
        return

    if isinstance(err, (NetworkError, TimedOut)):
        logger.warning("Telegram network issue: %s", err)
        return

    if isinstance(err, RetryAfter):
        logger.warning("Telegram rate limit, retry after: %s sec", err.retry_after)
        return

    logger.exception("Unhandled bot error: %s", err)


def main():
    global robokassa_client

    load_dotenv()

    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не установлен!")
        return

    if not ROBOKASSA_MERCHANT_LOGIN:
        logger.error("ROBOKASSA_MERCHANT_LOGIN не установлен!")
        return

    if not ROBOKASSA_PASSWORD_1:
        logger.error("ROBOKASSA_PASSWORD_1 не установлен!")
        return

    if not ROBOKASSA_PASSWORD_2:
        logger.warning("ROBOKASSA_PASSWORD_2 не установлен - проверка подписи недоступна")

    robokassa_client = init_robokassa()
    if robokassa_client:
        logger.info("Robokassa клиент инициализирован")
    else:
        logger.warning("Используем ручной метод создания ссылок")

    global db
    if not DATABASE_URL:
        logger.error("DATABASE_URL не установлен!")
        return

    db = Database(DATABASE_URL)
    db.init_database()

    mode = "ТЕСТОВЫЙ" if ROBOKASSA_TEST_MODE else "БОЕВОЙ"
    logger.info("Режим Robokassa: %s", mode)
    logger.info("Merchant Login: %s", ROBOKASSA_MERCHANT_LOGIN)
    logger.info("Цена подписки: %s KZT", SUBSCRIPTION_PRICE)

    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("check", check_subscription_cmd))
    application.add_handler(CommandHandler("account", show_account))
    application.add_handler(CommandHandler(["unsubscribe", "cancel"], cancel_subscription_action))
    application.add_handler(CommandHandler("stats", admin_stats))
    application.add_handler(CommandHandler("confirm_payment", confirm_payment))
    application.add_handler(CommandHandler("check_subs", manual_check_subscriptions))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    application.add_handler(CallbackQueryHandler(funnel_story2, pattern="^funnel_story2$"))
    application.add_handler(CallbackQueryHandler(funnel_story3, pattern="^funnel_story3$"))
    application.add_handler(CallbackQueryHandler(funnel_story4, pattern="^funnel_story4$"))
    application.add_handler(CallbackQueryHandler(funnel_story5, pattern="^funnel_story5$"))
    application.add_handler(CallbackQueryHandler(funnel_story6, pattern="^funnel_story6$"))
    application.add_handler(CallbackQueryHandler(funnel_story7, pattern="^funnel_story7$"))

    job_queue = application.job_queue
    job_queue.run_daily(
        check_expired_subscriptions,
        time=dt_time(hour=12, minute=0, second=0, tzinfo=TIMEZONE),
        name="daily_subscription_check"
    )
    job_queue.run_daily(
        process_recurring_charges,
        time=dt_time(hour=3, minute=0, second=0, tzinfo=TIMEZONE),
        name="daily_recurring_charge"
    )
    logger.info("📅 Запланирована ежедневная проверка подписок в 12:00")
    logger.info("📅 Запланирована ежедневная обработка автосписаний в 03:00")

    application.add_handler(CallbackQueryHandler(funnel_want, pattern="^funnel_want$"))
    application.add_handler(CallbackQueryHandler(funnel_details, pattern="^funnel_details$"))
    application.add_handler(CallbackQueryHandler(funnel_offer_agreement, pattern="^funnel_offer_agreement$"))
    application.add_handler(CallbackQueryHandler(funnel_confirm_offer, pattern="^funnel_confirm_offer$"))
    application.add_handler(CallbackQueryHandler(funnel_payment, pattern="^funnel_payment$"))
    application.add_handler(CallbackQueryHandler(funnel_back_to_want, pattern="^funnel_back_to_want$"))
    application.add_handler(CallbackQueryHandler(funnel_doubt, pattern="^funnel_doubt$"))
    application.add_handler(CallbackQueryHandler(check_payment_callback, pattern="^check_payment_"))
    application.add_handler(CallbackQueryHandler(show_account, pattern="^account$"))
    application.add_handler(CallbackQueryHandler(cancel_subscription_action, pattern="^cancel_subscription$"))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_message))
    application.add_error_handler(global_error_handler)

    logger.info("🤖 Бот запущен и готов к работе!")

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        bootstrap_retries=-1,
        poll_interval=1.0,
        timeout=30,
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()