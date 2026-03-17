"""
Простой FastAPI-вебхук для приема Result URL от Robokassa.
- Проверяет MD5-подпись (Пароль #2)
- Идемпотентно сохраняет платёж
- Продлевает/активирует подписку (pending-инвойсы поддерживаются)
- Отправляет пользователю ссылку на канал

Запуск (пример):
    uvicorn webhook:app --host 0.0.0.0 --port 8000
Result URL в кабинете Robokassa:
    https://<ваш-домен>/robokassa/result
Метод: POST
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import PlainTextResponse
from dotenv import load_dotenv
from telegram import Bot

from config import (
    TELEGRAM_TOKEN,
    DATABASE_URL,
    CHANNEL_LINK,
    ROBOKASSA_TEST_MODE,
    RENEWAL_PERIOD_DAYS,
    RECURRING_LEAD_DAYS,
)
from bot import verify_payment_signature, TEXTS, build_after_payment_keyboard
from database import Database

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN не задан (проверьте .env)")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан (проверьте .env)")

db = Database(DATABASE_URL)
db.init_database()

bot = Bot(token=TELEGRAM_TOKEN)

app = FastAPI(title="Robokassa Webhook", version="1.0.0")


def _ensure_utc(dt: datetime) -> datetime:
    """
    Приводит datetime к aware UTC.
    Если из БД пришёл naive datetime, считаем его UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def delete_message_later(chat_id: int, message_id: int, delay_seconds: int = 300):
    """Отложенное удаление сообщения с ссылкой, чтобы нельзя было использовать её позже."""
    await asyncio.sleep(delay_seconds)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning("Не удалось удалить сообщение %s:%s: %s", chat_id, message_id, e)


@app.on_event("startup")
async def on_startup():
    mode = "ТЕСТОВЫЙ" if ROBOKASSA_TEST_MODE else "БОЕВОЙ"
    logger.info("🚀 Robokassa webhook запущен (%s)", mode)


def _form_to_dict(form_data) -> Dict[str, str]:
    """Приводим starlette.datastructures.FormData к обычному dict."""
    return {k: v for k, v in form_data.items()}


@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"


@app.post("/robokassa/result", response_class=PlainTextResponse)
async def robokassa_result(request: Request):
    """
    Обработка Result URL от Robokassa.
    Требуемые поля: OutSum, InvId, SignatureValue, Shp_user_id, Shp_interface.
    """
    form = await request.form()
    payload = _form_to_dict(form)

    required = ["OutSum", "InvId", "SignatureValue", "Shp_user_id", "Shp_interface"]
    missing = [k for k in required if k not in payload]
    if missing:
        logger.warning("Нет обязательных полей: %s", missing)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"missing fields: {', '.join(missing)}",
        )

    if payload.get("Shp_interface") != "link":
        logger.warning("Некорректный Shp_interface: %s", payload.get("Shp_interface"))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="bad Shp_interface",
        )

    out_sum = payload["OutSum"]
    inv_id = payload["InvId"]
    user_id = payload["Shp_user_id"]
    signature = payload["SignatureValue"]

    if not verify_payment_signature(out_sum, inv_id, signature, user_id):
        logger.warning(
            "Неверная подпись: inv_id=%s user_id=%s signature=%s",
            inv_id,
            user_id,
            signature,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="bad signature",
        )

    try:
        amount_float = float(str(out_sum).replace(",", "."))
    except ValueError:
        amount_float = 0.0

    try:
        inv_id_int = int(inv_id)
        user_id_int = int(user_id)
    except ValueError:
        logger.warning("Некорректные числовые поля: inv_id=%s user_id=%s", inv_id, user_id)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="bad numeric fields",
        )

    # Идемпотентность: если платёж уже записан — просто отвечаем OK
    if db.payment_exists(inv_id_int):
        logger.info(
            "Duplicate ResultURL ignored (payment exists): user=%s inv_id=%s",
            user_id,
            inv_id,
        )
        return PlainTextResponse(content=f"OK{inv_id}")

    period_days = RENEWAL_PERIOD_DAYS or 30
    recurring_lead_days = RECURRING_LEAD_DAYS or 1
    now_dt = datetime.now(timezone.utc)

    existing = db.get_subscription(user_id_int)

    # Продлеваем от max(expires_at, now)
    if existing and existing.get("expires_at"):
        existing_expires = _ensure_utc(existing["expires_at"])
        base_dt = existing_expires if existing_expires > now_dt else now_dt
    else:
        base_dt = now_dt

    new_expires_at = base_dt + timedelta(days=period_days)
    new_next_charge_at = new_expires_at - timedelta(days=recurring_lead_days)

    # Якорь: первый успешный inv_id фиксируем, дальше не меняем
    anchor_inv_id = existing.get("anchor_inv_id") if existing else None
    if not anchor_inv_id:
        anchor_inv_id = inv_id_int

    is_confirmed_pending = (
        existing
        and existing.get("pending_inv_id")
        and int(existing["pending_inv_id"]) == inv_id_int
    )

    # Подтверждённый recurring pending
    if is_confirmed_pending:
        db.clear_pending_charge(user_id_int)
        db.renew_subscription(
            user_id=user_id_int,
            expires_at=new_expires_at,
            next_charge_at=new_next_charge_at,
            anchor_inv_id=anchor_inv_id,
        )
        logger.info(
            "Pending recurring confirmed: user=%s inv_id=%s new_expires_at=%s",
            user_id,
            inv_id,
            new_expires_at,
        )
    else:
        # Обычный первый / ручной / не-pending платёж
        if existing:
            # Если у пользователя завис старый pending, а он оплатил вручную —
            # очищаем pending, чтобы логика не залипла.
            if existing.get("pending_inv_id"):
                db.clear_pending_charge(user_id_int)

            db.renew_subscription(
                user_id=user_id_int,
                expires_at=new_expires_at,
                next_charge_at=new_next_charge_at,
                anchor_inv_id=anchor_inv_id,
            )
            logger.info(
                "Manual/initial payment applied to existing subscription: user=%s inv_id=%s new_expires_at=%s",
                user_id,
                inv_id,
                new_expires_at,
            )
        else:
            db.add_subscription(
                user_id=user_id_int,
                username=f"user_{user_id}",
                expires_at=new_expires_at,
                payment_amount=amount_float,
                anchor_inv_id=anchor_inv_id,
                next_charge_at=new_next_charge_at,
            )
            logger.info(
                "New subscription created from payment: user=%s inv_id=%s new_expires_at=%s",
                user_id,
                inv_id,
                new_expires_at,
            )

    # Пишем платёж после успешной обработки
    db.add_payment(
        user_id=user_id_int,
        amount=amount_float,
        currency="KZT",
        invoice_payload=f"robokassa_{inv_id}",
        inv_id=inv_id_int,
        raw_payload=payload,
    )

    # Отправляем пользователю ссылку на канал и управление автоплатежом
    try:
        msg = await bot.send_message(
            chat_id=user_id_int,
            text=TEXTS["after_payment"].format(channel_link=CHANNEL_LINK),
            reply_markup=build_after_payment_keyboard(),
        )
        asyncio.create_task(delete_message_later(user_id_int, msg.message_id))
    except Exception as e:
        logger.warning("Не удалось отправить сообщение пользователю %s: %s", user_id, e)

    logger.info("Оплата подтверждена: user=%s inv_id=%s", user_id, inv_id)
    return PlainTextResponse(content=f"OK{inv_id}")