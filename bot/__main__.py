import asyncio
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.fsm.storage.memory import MemoryStorage


from bot.handlers.usermode import user_router
from bot.handlers.adminmode import admin_router
from bot.middlewares.throttling import ThrottlingMiddleware
from bot.db.config import settings
from bot.screeners.binance_ws_alerts import run_binance_kline_alert_engine


async def main():

    logging.basicConfig(
        level=logging.INFO,
        # filename='data/logs.log',
        format="%(asctime)s - %(message)s"
    )

    bot = Bot(token=settings.bot.token, proxy=settings.bot.proxy_url)
    # Create Dispatcher
    dp = Dispatcher(storage=MemoryStorage())

    # Add admin filter to admin_router and user_router
    admin_router.message.filter(F.from_user.id.in_(settings.bot.admin_list))
    user_router.message.filter()

    dp.include_router(user_router)
    dp.include_router(admin_router)


    # Registration middleware on throttling
    # dp.message.middleware(ThrottlingMiddleware())


    await bot.delete_webhook(drop_pending_updates=True)

    # Фоновый WebSocket-движок для постоянных уведомлений по свечам
    asyncio.create_task(run_binance_kline_alert_engine(bot))

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
