import asyncio
import os
import sys

from telegram import Bot


async def main():
    """
    Одноразовый скрипт для кика и мгновенного разбана пользователя.
    Требуются переменные окружения TELEGRAM_TOKEN и CHANNEL_ID.
    Пример: USER_ID=382728138 python kick_user.py
    """
    token = os.getenv("TELEGRAM_TOKEN")
    channel_id = os.getenv("CHANNEL_ID")
    user_id = os.getenv("USER_ID")

    if not token or not channel_id:
        print("Ошибка: TELEGRAM_TOKEN и CHANNEL_ID должны быть заданы в окружении")
        sys.exit(1)

    if not user_id:
        print("Ошибка: USER_ID нужно передать в окружении (например, USER_ID=382728138)")
        sys.exit(1)

    try:
        channel_id = int(channel_id)
        user_id = int(user_id)
    except ValueError:
        print("Ошибка: CHANNEL_ID и USER_ID должны быть числами")
        sys.exit(1)

    bot = Bot(token=token)

    # Кик + мгновенный разбан, чтобы пользователь мог вернуться после повторного приглашения
    await bot.ban_chat_member(chat_id=channel_id, user_id=user_id)
    await bot.unban_chat_member(chat_id=channel_id, user_id=user_id)

    print(f"Готово: пользователь {user_id} кикнут и разбанен в канале {channel_id}")


if __name__ == "__main__":
    asyncio.run(main())

