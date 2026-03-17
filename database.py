import json
import logging
import re
from datetime import datetime
from typing import Optional, Dict, List, Any

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


class Database:
    """
    Postgres-реализация хранилища.
    Ожидает, что таблицы уже созданы (users, subscriptions, payments, questions).
    Интерфейс сохранён, чтобы минимально менять остальной код бота.
    """

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url, future=True)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, future=True)

    def init_database(self):
        """Проверка подключения и добавление недостающих колонок/индексов."""
        try:
            with self.engine.begin() as conn:
                conn.execute(sa.text("SELECT 1"))

                conn.execute(
                    sa.text(
                        """
                        ALTER TABLE subscriptions
                        ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN DEFAULT FALSE
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        """
                        ALTER TABLE subscriptions
                        ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMP
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        """
                        ALTER TABLE subscriptions
                        ADD COLUMN IF NOT EXISTS anchor_inv_id BIGINT
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        """
                        ALTER TABLE subscriptions
                        ADD COLUMN IF NOT EXISTS next_charge_at TIMESTAMP
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        """
                        ALTER TABLE subscriptions
                        ADD COLUMN IF NOT EXISTS pending_inv_id BIGINT
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        """
                        ALTER TABLE subscriptions
                        ADD COLUMN IF NOT EXISTS pending_amount NUMERIC
                        """
                    )
                )
                conn.execute(
                    sa.text(
                        """
                        ALTER TABLE subscriptions
                        ADD COLUMN IF NOT EXISTS pending_created_at TIMESTAMP
                        """
                    )
                )

                # Полезный индекс для защиты от дублей по inv_id
                conn.execute(
                    sa.text(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_payments_inv_id
                        ON payments (inv_id)
                        WHERE inv_id IS NOT NULL
                        """
                    )
                )

            logger.info("Подключено к Postgres")
        except Exception as e:
            logger.error(f"Не удалось подключиться к Postgres: {e}")
            raise

    # -------------------
    # Пользователи / воронка
    # -------------------
    def update_user_state(self, user_id: int, username: str, state: str):
        """Upsert пользователя и сохранение состояния воронки."""
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    INSERT INTO users (user_id, username, state)
                    VALUES (:uid, :uname, :state)
                    ON CONFLICT (user_id) DO UPDATE
                    SET username = EXCLUDED.username,
                        state = EXCLUDED.state,
                        updated_at = now()
                    """
                ),
                {"uid": user_id, "uname": username, "state": state},
            )
        logger.info("Состояние пользователя %s обновлено: %s", user_id, state)

    def save_user_question(self, user_id: int, question: str):
        """Сохранить вопрос пользователя."""
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    INSERT INTO questions (user_id, text, created_at)
                    VALUES (:uid, :txt, now())
                    """
                ),
                {"uid": user_id, "txt": question},
            )
        logger.info("Вопрос пользователя %s сохранён", user_id)

    # -------------------
    # Подписки
    # -------------------
    def add_subscription(
        self,
        user_id: int,
        username: str,
        expires_at: datetime,
        payment_amount: float,
        anchor_inv_id: Optional[int] = None,
        next_charge_at: Optional[datetime] = None,
    ):
        """Создать/обновить подписку и пользователя."""
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    INSERT INTO users (user_id, username)
                    VALUES (:uid, :uname)
                    ON CONFLICT (user_id) DO UPDATE
                    SET username = EXCLUDED.username,
                        updated_at = now()
                    """
                ),
                {"uid": user_id, "uname": username},
            )

            s.execute(
                sa.text(
                    """
                    UPDATE subscriptions
                    SET active = FALSE, updated_at = now()
                    WHERE user_id = :uid AND active = TRUE
                    """
                ),
                {"uid": user_id},
            )

            s.execute(
                sa.text(
                    """
                    INSERT INTO subscriptions (
                        user_id,
                        expires_at,
                        active,
                        created_at,
                        updated_at,
                        anchor_inv_id,
                        next_charge_at,
                        cancel_requested,
                        cancel_requested_at,
                        pending_inv_id,
                        pending_amount,
                        pending_created_at
                    )
                    VALUES (
                        :uid,
                        :exp,
                        TRUE,
                        now(),
                        now(),
                        :anchor,
                        :next_charge,
                        FALSE,
                        NULL,
                        NULL,
                        NULL,
                        NULL
                    )
                    """
                ),
                {
                    "uid": user_id,
                    "exp": expires_at,
                    "anchor": anchor_inv_id,
                    "next_charge": next_charge_at,
                },
            )
        logger.info("Подписка создана/обновлена для пользователя %s", user_id)

    def get_subscription(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получить активную подписку пользователя (самую свежую)."""
        with self.Session() as s:
            row = (
                s.execute(
                    sa.text(
                        """
                        SELECT user_id, expires_at, active, cancel_requested, cancel_requested_at,
                               anchor_inv_id, next_charge_at, pending_inv_id, pending_amount, pending_created_at
                        FROM subscriptions
                        WHERE user_id = :uid AND active = TRUE
                        ORDER BY expires_at DESC
                        LIMIT 1
                        """
                    ),
                    {"uid": user_id},
                )
                .mappings()
                .first()
            )

            if row:
                return {
                    "user_id": row["user_id"],
                    "expires_at": row["expires_at"],
                    "active": row["active"],
                    "cancel_requested": row["cancel_requested"],
                    "cancel_requested_at": row["cancel_requested_at"],
                    "anchor_inv_id": row.get("anchor_inv_id"),
                    "next_charge_at": row.get("next_charge_at"),
                    "pending_inv_id": row.get("pending_inv_id"),
                    "pending_amount": row.get("pending_amount"),
                    "pending_created_at": row.get("pending_created_at"),
                }
        return None

    def get_expired_subscriptions(self) -> List[Dict[str, Any]]:
        """Истекшие активные подписки."""
        with self.Session() as s:
            rows = (
                s.execute(
                    sa.text(
                        """
                        SELECT s.user_id, s.expires_at, u.username,
                               s.cancel_requested, s.anchor_inv_id, s.next_charge_at,
                               s.pending_inv_id, s.pending_amount, s.pending_created_at
                        FROM subscriptions s
                        LEFT JOIN users u ON u.user_id = s.user_id
                        WHERE s.active = TRUE AND s.expires_at < now()
                        """
                    )
                )
                .mappings()
                .all()
            )
            return [dict(r) for r in rows]

    def get_all_active_subscriptions(self) -> List[Dict[str, Any]]:
        """Все активные подписки."""
        with self.Session() as s:
            rows = (
                s.execute(
                    sa.text(
                        """
                        SELECT s.user_id, u.username, s.expires_at, s.cancel_requested,
                               s.anchor_inv_id, s.next_charge_at,
                               s.pending_inv_id, s.pending_amount, s.pending_created_at
                        FROM subscriptions s
                        LEFT JOIN users u ON u.user_id = s.user_id
                        WHERE s.active = TRUE
                        """
                    )
                )
                .mappings()
                .all()
            )
            return [dict(r) for r in rows]

    def get_recurring_candidates(self) -> List[Dict[str, Any]]:
        """
        Кандидаты для логики автосписаний.
        Берём все активные подписки с anchor_inv_id и без отключённого автоплатежа.
        """
        with self.Session() as s:
            rows = (
                s.execute(
                    sa.text(
                        """
                        SELECT s.user_id, u.username, s.expires_at, s.cancel_requested,
                               s.cancel_requested_at, s.anchor_inv_id, s.next_charge_at,
                               s.pending_inv_id, s.pending_amount, s.pending_created_at
                        FROM subscriptions s
                        LEFT JOIN users u ON u.user_id = s.user_id
                        WHERE s.active = TRUE
                          AND COALESCE(s.cancel_requested, FALSE) = FALSE
                          AND s.anchor_inv_id IS NOT NULL
                        """
                    )
                )
                .mappings()
                .all()
            )
            return [dict(r) for r in rows]

    def deactivate_subscription(self, user_id: int):
        """Деактивировать подписку пользователя."""
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    UPDATE subscriptions
                    SET active = FALSE, updated_at = now()
                    WHERE user_id = :uid AND active = TRUE
                    """
                ),
                {"uid": user_id},
            )
        logger.info("Подписка деактивирована для пользователя %s", user_id)

    def renew_subscription(
        self,
        user_id: int,
        expires_at: datetime,
        next_charge_at: Optional[datetime],
        anchor_inv_id: Optional[int] = None,
    ):
        """
        Обновить сроки активной подписки пользователя.
        Использовать ТОЛЬКО после подтверждённой оплаты.
        """
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    UPDATE subscriptions
                    SET expires_at = :exp,
                        next_charge_at = :next_charge,
                        anchor_inv_id = COALESCE(:anchor, anchor_inv_id),
                        updated_at = now()
                    WHERE user_id = :uid AND active = TRUE
                    """
                ),
                {
                    "uid": user_id,
                    "exp": expires_at,
                    "next_charge": next_charge_at,
                    "anchor": anchor_inv_id,
                },
            )
        logger.info("Подписка обновлена для пользователя %s", user_id)

    def update_charge_schedule(
        self,
        user_id: int,
        *,
        next_charge_at: Optional[datetime],
        anchor_inv_id: Optional[int] = None,
    ):
        """
        Обновить только график списания, не меняя expires_at.
        Использовать для pending / повторных попыток recurring.
        """
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    UPDATE subscriptions
                    SET next_charge_at = :next_charge,
                        anchor_inv_id = COALESCE(:anchor, anchor_inv_id),
                        updated_at = now()
                    WHERE user_id = :uid AND active = TRUE
                    """
                ),
                {
                    "uid": user_id,
                    "next_charge": next_charge_at,
                    "anchor": anchor_inv_id,
                },
            )
        logger.info("График списания обновлён для пользователя %s", user_id)

    def request_cancel_subscription(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Пометить автоплатеж как отключённый, оставить доступ до конца оплаченного периода.
        Возвращает данные по подписке или None, если активной нет.
        """
        with self.Session() as s, s.begin():
            row = (
                s.execute(
                    sa.text(
                        """
                        SELECT user_id, expires_at, cancel_requested
                        FROM subscriptions
                        WHERE user_id = :uid AND active = TRUE
                        ORDER BY expires_at DESC
                        LIMIT 1
                        """
                    ),
                    {"uid": user_id},
                )
                .mappings()
                .first()
            )

            if not row:
                return None

            result = dict(row)

            if not row["cancel_requested"]:
                s.execute(
                    sa.text(
                        """
                        UPDATE subscriptions
                        SET cancel_requested = TRUE,
                            cancel_requested_at = now(),
                            updated_at = now()
                        WHERE user_id = :uid AND active = TRUE
                        """
                    ),
                    {"uid": user_id},
                )
                result["cancel_requested"] = True

        return result

    # -------------------
    # Платежи
    # -------------------
    @staticmethod
    def _extract_inv_id(invoice_payload: str) -> Optional[int]:
        """Пытаемся вытащить числовой inv_id из строки payload."""
        if not invoice_payload:
            return None
        match = re.search(r"(\d+)", invoice_payload)
        return int(match.group(1)) if match else None

    def payment_exists(self, inv_id: int) -> bool:
        """Проверить, есть ли уже платёж с этим inv_id."""
        with self.Session() as s:
            row = s.execute(
                sa.text("SELECT 1 FROM payments WHERE inv_id = :inv LIMIT 1"),
                {"inv": inv_id},
            ).first()
            return bool(row)

    def add_payment(
        self,
        user_id: int,
        amount: float,
        currency: str = "KZT",
        invoice_payload: str = "",
        inv_id: Optional[int] = None,
        raw_payload: Optional[Dict[str, Any]] = None,
    ):
        """Записать платёж как оплаченный."""
        inv = inv_id or self._extract_inv_id(invoice_payload)
        raw_json = json.dumps(raw_payload or {"invoice_payload": invoice_payload})

        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    INSERT INTO payments (user_id, inv_id, amount, currency, status, raw_payload, created_at)
                    VALUES (:uid, :inv, :amt, :cur, 'paid', CAST(:rawp AS jsonb), now())
                    ON CONFLICT (inv_id) DO NOTHING
                    """
                ),
                {"uid": user_id, "inv": inv, "amt": amount, "cur": currency, "rawp": raw_json},
            )
        logger.info("Платёж записан: user=%s, inv_id=%s, amount=%s %s", user_id, inv, amount, currency)

    # -------------------
    # Pending recurring
    # -------------------
    def set_pending_charge(self, user_id: int, pending_inv_id: int, amount: float, created_at: datetime):
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    UPDATE subscriptions
                    SET pending_inv_id = :pinv,
                        pending_amount = :amt,
                        pending_created_at = :pcreated,
                        updated_at = now()
                    WHERE user_id = :uid AND active = TRUE
                    """
                ),
                {"pinv": pending_inv_id, "amt": amount, "pcreated": created_at, "uid": user_id},
            )
        logger.info("Pending charge set: user=%s inv=%s", user_id, pending_inv_id)

    def clear_pending_charge(self, user_id: int):
        with self.Session() as s, s.begin():
            s.execute(
                sa.text(
                    """
                    UPDATE subscriptions
                    SET pending_inv_id = NULL,
                        pending_amount = NULL,
                        pending_created_at = NULL,
                        updated_at = now()
                    WHERE user_id = :uid AND active = TRUE
                    """
                ),
                {"uid": user_id},
            )
        logger.info("Pending charge cleared: user=%s", user_id)

    # -------------------
    # Статистика
    # -------------------
    def get_statistics(self) -> Dict[str, Any]:
        """Агрегированные числа по пользователям/подпискам/платежам."""
        with self.Session() as s:
            total_users = s.execute(sa.text("SELECT count(*) FROM users")).scalar() or 0
            active_subs = s.execute(
                sa.text("SELECT count(*) FROM subscriptions WHERE active = TRUE AND expires_at > now()")
            ).scalar() or 0
            expired_subs = s.execute(
                sa.text("SELECT count(*) FROM subscriptions WHERE active = TRUE AND expires_at <= now()")
            ).scalar() or 0
            total_payments = s.execute(sa.text("SELECT count(*) FROM payments")).scalar() or 0

        return {
            "total_users": total_users,
            "active_subscriptions": active_subs,
            "expired_subscriptions": expired_subs,
            "total_payments": total_payments,
        }

    def get_funnel_statistics(self) -> Dict[str, int]:
        """Количество пользователей по состояниям воронки (поле state в users)."""
        with self.Session() as s:
            rows = (
                s.execute(sa.text("SELECT state, count(*) AS c FROM users GROUP BY state"))
                .mappings()
                .all()
            )
        return {r["state"]: r["c"] for r in rows if r["state"] is not None}

    def close(self):
        """Закрыть соединение."""
        self.engine.dispose()