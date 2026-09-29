from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.screeners.binance import BinanceScreener
from bot.screeners.binance_ws_alerts import get_binance_kline_alert_engine_stats

admin_router = Router()


@admin_router.message(Command("upd_symbols"))
async def update_symbols_handler(message: Message):
    screener = BinanceScreener()
    await screener.update_symbols_list()
    await message.answer('Символы обновлены')


@admin_router.message(Command("ws_stats"))
async def ws_stats_handler(message: Message):
    stats = get_binance_kline_alert_engine_stats()
    text = (
        "WS stats:\n"
        f"started: {stats.get('started')}\n"
        f"window_size: {stats.get('window_size')}\n"
        f"ws_connections: {stats.get('ws_connections')}\n"
        f"ws_reconnect_tasks: {stats.get('ws_reconnect_tasks')}\n"
        f"token_streams_active: {stats.get('token_streams_active')}\n"
        f"symbols_loaded: {stats.get('symbols_loaded')}\n"
        f"active_user_subscriptions: {stats.get('active_user_subscriptions')}\n"
        f"queue_size: {stats.get('queue_size')}\n"
        f"workers: {stats.get('workers')}\n"
        f"connect_retries: {stats.get('connect_retries')}\n"
        f"resubscribe_retries: {stats.get('resubscribe_retries')}\n"
        f"subscribe_errors: {stats.get('subscribe_errors')}\n"
        f"worker_errors: {stats.get('worker_errors')}\n"
        f"handle_errors: {stats.get('handle_errors')}\n"
        f"last_error: {stats.get('last_error')}"
    )
    await message.answer(text)
