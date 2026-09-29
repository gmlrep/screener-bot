import asyncio
from dataclasses import dataclass

import loguru
from aiogram import Bot
from binance_common.constants import WebsocketMode
from binance_common.configuration import ConfigurationWebSocketStreams
from binance_sdk_derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

from bot.db.config import settings
from bot.db.services.binance_margin_symbols import BinanceMarginSymbolsService
from bot.db.services.kline_alert_subscription import KlineAlertSubscriptionService
from bot.screeners.utils import LRUCache


@dataclass(frozen=True)
class KlineSubscription:
    user_id: int
    diff_percent: float


WINDOW_SIZE = settings.binance.alert_window_size
_ENGINE_INSTANCE: "BinanceKlineAlertEngine | None" = None


class BinanceKlineAlertEngine:
    def __init__(
        self,
        bot: Bot,
    ) -> None:
        self.bot = bot
        self.subscriptions_refresh_seconds = settings.binance.alert_subscriptions_refresh_seconds
        self.symbols_refresh_seconds = settings.binance.alert_symbols_refresh_seconds
        self.reconnect_delay_seconds = settings.binance.alert_reconnect_delay_seconds
        self.subscribe_delay_seconds = settings.binance.alert_subscribe_delay_seconds
        self._subscribe_call_semaphore = asyncio.Semaphore(settings.binance.alert_subscribe_concurrency)

        # Распределяем подписки по нескольким ws-соединениям, чтобы не убить один transport
        conf_ws = ConfigurationWebSocketStreams(
            stream_url=settings.binance.ws_stream_url,
            mode=WebsocketMode.POOL,
            pool_size=settings.binance.ws_pool_size,
            reconnect_delay=settings.binance.ws_reconnect_delay_ms,
        )
        self.client_ws = DerivativesTradingUsdsFutures(config_ws_streams=conf_ws)

        # symbol -> title
        self.symbol_titles: dict[str, str] = {}
        self._symbols_fingerprint: str = ""

        # list of subscriptions for fixed WINDOW_SIZE
        self._subs_ref: list[KlineSubscription] = []
        self._subs_fingerprint: str = ""

        # symbol -> RequestStreamHandle (нужно, чтобы можно было отписаться при изменении списка символов)
        self._symbol_stream_handles: dict[str, object] = {}

        # (symbol, window_size, candle_close_time) -> True
        self._notified_cache = LRUCache(capacity=20000)

        # ограничитель одновременных send_message (Telegram rate limit)
        self._send_semaphore = asyncio.Semaphore(settings.binance.alert_send_concurrency)

        # Очередь входящих закрытых свечей и воркеры обработки.
        # Это защищает event loop от всплесков create_task при большом количестве символов.
        self._kline_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=settings.binance.alert_queue_maxsize)
        self._kline_workers: list[asyncio.Task] = []
        self._kline_workers_count = settings.binance.alert_workers_count

        self._stop_event = asyncio.Event()
        self._started = False

        # Runtime metrics for admin diagnostics
        self._connect_retries = 0
        self._subscribe_errors = 0
        self._worker_errors = 0
        self._handle_errors = 0
        self._resubscribe_retries = 0
        self._last_error: str | None = None

    async def stop(self) -> None:
        self._stop_event.set()
        # ручная отмена subscribe не нужна: stop_event завершит main loop
        for worker in self._kline_workers:
            worker.cancel()

    async def _kline_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                payload = await self._kline_queue.get()
                try:
                    await self._handle_kline_message(data=payload, window_size=WINDOW_SIZE)
                finally:
                    self._kline_queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._worker_errors += 1
                self._last_error = str(e)
                loguru.logger.error(f"kline worker error: {e}")

    async def _reload_symbols(self) -> bool:
        service = BinanceMarginSymbolsService()
        symbols_info = await service.get_symbols()
        symbols = sorted(symbols_info.keys())
        fingerprint = ",".join(symbols)
        if fingerprint == self._symbols_fingerprint:
            return False

        self.symbol_titles = {sym: dt["title"] for sym, dt in symbols_info.items()}
        self._symbols_fingerprint = fingerprint
        return True

    async def _reload_subscriptions(self) -> bool:
        service = KlineAlertSubscriptionService()
        subs = await service.get_all_subscriptions()

        # window_size фиксирован на 15m, поэтому игнорируем значение из БД,
        # чтобы все существующие подписки начали работать.
        # Дедуп: если у пользователя окажется несколько записей - берем минимальный diff_percent.
        by_user: dict[int, float] = {}
        for sub in subs:
            if str(sub.get("window_size")) != WINDOW_SIZE:
                continue
            if not bool(sub.get("active", True)):
                continue
            user_id = int(sub["user_id"])
            diff = float(sub["diff_percent"])
            by_user[user_id] = min(diff, by_user.get(user_id, diff))

        self._subs_ref = [KlineSubscription(user_id=uid, diff_percent=diff) for uid, diff in by_user.items()]

        fingerprint = str(sorted((s.user_id, s.diff_percent) for s in self._subs_ref))
        if fingerprint == self._subs_fingerprint:
            return False

        self._subs_fingerprint = fingerprint
        return True

    async def _subscribe_symbol(self, ws_streams: object, *, symbol: str) -> None:
        """
        Подписываемся на kline_15m для одного символа.
        ws_streams: объект, который возвращает SDK из create_connection().
        """
        async with self._subscribe_call_semaphore:
            stream_handle = await ws_streams.kline_candlestick_streams(
                symbol=symbol,
                interval=WINDOW_SIZE,
            )
            if self.subscribe_delay_seconds > 0:
                await asyncio.sleep(self.subscribe_delay_seconds)

        def _on_message(data) -> None:
            # SDK может присылать dict или pydantic-модель.
            payload = self._normalize_payload(data)
            if payload is None:
                return

            k = payload.get("k") or {}
            if not k:
                return

            try:
                self._kline_queue.put_nowait(payload)
            except asyncio.QueueFull:
                # Если очередь заполнена, снимаем самый старый элемент и кладем новый.
                try:
                    _ = self._kline_queue.get_nowait()
                    self._kline_queue.task_done()
                    self._kline_queue.put_nowait(payload)
                except asyncio.QueueEmpty:
                    pass

        stream_handle.on("message", _on_message)
        self._symbol_stream_handles[symbol] = stream_handle

    @staticmethod
    def _normalize_payload(data: object) -> dict | None:
        payload = data
        if not isinstance(payload, dict):
            model_dump = getattr(payload, "model_dump", None)
            if callable(model_dump):
                payload = model_dump()
            else:
                to_dict = getattr(payload, "dict", None)
                if callable(to_dict):
                    payload = to_dict()
                else:
                    return None

        # Для combined stream часть SDK может прислать {"stream": "...", "data": {...}}
        if isinstance(payload.get("data"), dict):
            payload = payload["data"]

        if not isinstance(payload, dict):
            return None
        return payload

    async def _sync_symbol_streams(self, ws_streams: object) -> None:
        """
        Приводим набор подписок к набору символов в `self.symbol_titles`.
        """
        current_symbols = set(self.symbol_titles.keys())
        existing_symbols = set(self._symbol_stream_handles.keys())

        removed = existing_symbols - current_symbols
        added = current_symbols - existing_symbols

        # Unsubscribe удалённых
        for symbol in removed:
            handle = self._symbol_stream_handles.pop(symbol, None)
            if handle is None:
                continue
            try:
                await handle.unsubscribe()
            except Exception:
                # если unsubscribe не сработал из-за reconnect'а, это не критично
                pass

        # Subscribe добавленных
        added_sorted = sorted(added)
        if added_sorted:
            tasks = [self._subscribe_symbol(ws_streams, symbol=s) for s in added_sorted]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for symbol, res in zip(added_sorted, results, strict=False):
                if isinstance(res, Exception):
                    self._subscribe_errors += 1
                    self._last_error = str(res)
                    loguru.logger.warning(f"subscribe failed symbol={symbol}: {res}")
                    # транспорт/transport может умереть во время массового subscribe:
                    # делаем reconnect на уровне main loop
                    raise res

    async def _handle_kline_message(self, *, data: dict, window_size: str) -> None:
        """
        Типичное тело binance kline event:
        {
          "e": "kline",
          "s": "BTCUSDT",
          "k": {
            "o": "...", "c": "...", "x": false, "T": closeTime, "i": "15m" ...
          }
        }
        """
        try:
            k = data.get("k") or {}
            if not k:
                return

            symbol = data.get("s") or k.get("s")
            if not symbol:
                return

            if symbol not in self.symbol_titles:
                # могли подписаться на интервал до обновления символов
                return

            o_raw = k.get("o")
            c_raw = k.get("c")
            if o_raw is None or c_raw is None:
                return

            open_price = float(o_raw)
            last_price = float(c_raw)
            if open_price == 0:
                return

            change_pct = (last_price - open_price) / open_price * 100.0
            abs_change_pct = abs(change_pct)

            # dedupe в рамках одной свечи:
            # не шлем повторно в течение текущего candle (идентификатор по времени открытия свечи).
            candle_open_time = k.get("t")
            if candle_open_time is None:
                candle_open_time = k.get("T") or data.get("T")
            dedupe_key = f"{symbol}:{window_size}:{candle_open_time}"

            if self._notified_cache.get(dedupe_key):
                return

            if not self._subs_ref:
                return

            users_to_notify: list[int] = []
            for sub in self._subs_ref:
                if abs_change_pct >= sub.diff_percent:
                    users_to_notify.append(sub.user_id)

            if not users_to_notify:
                return

            # помечаем как уведомленное до отправки, чтобы избежать дублей при ределивери
            self._notified_cache.set(dedupe_key, True, ttl=settings.binance.alert_cache_ttl_seconds)

            cex = "🟧 Binance"
            cex_text = "Binance"
            title = self.symbol_titles.get(symbol, symbol)
            status = "🟢 Pump" if change_pct >= 0 else "🔴 Dump"

            msg_text = (
                f"[{cex}](https://www.binance.com/ru/futures/{symbol}) - {window_size} - "
                f"[{title}](https://www.coinglass.com/tv/{cex_text}_{symbol})\n"
                f"{status}: {change_pct:.2f}% ({open_price} - {last_price})"
            )

            async def _send_one(chat_id: int) -> None:
                async with self._send_semaphore:
                    await self.bot.send_message(
                        text=msg_text,
                        chat_id=chat_id,
                        disable_web_page_preview=True,
                        parse_mode="Markdown",
                    )

            await asyncio.gather(*(_send_one(uid) for uid in set(users_to_notify)))
            loguru.logger.info(
                f"Alert sent: symbol={symbol} change={change_pct:.2f}% users={len(set(users_to_notify))}"
            )
        except Exception as e:
            self._handle_errors += 1
            self._last_error = str(e)
            loguru.logger.error(f"handle_kline_message error: {e}")

    def get_stats(self) -> dict:
        ws_streams = self.client_ws.websocket_streams
        connections = getattr(ws_streams, "connections", None) or []
        reconnect_tasks = getattr(ws_streams, "reconnect_tasks", None) or []
        return {
            "started": self._started,
            "window_size": WINDOW_SIZE,
            "ws_connections": len(connections),
            "ws_reconnect_tasks": len(reconnect_tasks),
            "token_streams_active": len(self._symbol_stream_handles),
            "symbols_loaded": len(self.symbol_titles),
            "active_user_subscriptions": len(self._subs_ref),
            "queue_size": self._kline_queue.qsize(),
            "workers": len(self._kline_workers),
            "connect_retries": self._connect_retries,
            "resubscribe_retries": self._resubscribe_retries,
            "subscribe_errors": self._subscribe_errors,
            "worker_errors": self._worker_errors,
            "handle_errors": self._handle_errors,
            "last_error": self._last_error,
        }

    async def run_forever(self) -> None:
        self._started = True
        if not self._kline_workers:
            self._kline_workers = [
                asyncio.create_task(self._kline_worker())
                for _ in range(self._kline_workers_count)
            ]

        # initial load (символы + пороги)
        try:
            await self._reload_symbols()
        except Exception as e:
            loguru.logger.error(f"Initial symbols load error: {e}")
        try:
            await self._reload_subscriptions()
        except Exception as e:
            loguru.logger.error(f"Initial subscriptions load error: {e}")

        ws_streams = None
        while not self._stop_event.is_set():
            try:
                # create_connection() делается ОДИН раз, чтобы не плодить лишние сокеты
                ws_streams = await self.client_ws.websocket_streams.create_connection()
                await self._sync_symbol_streams(ws_streams)
                break
            except Exception as e:
                self._connect_retries += 1
                self._last_error = str(e)
                loguru.logger.warning(f"WS connect/retry error: {e}")
                self._symbol_stream_handles.clear()
                await asyncio.sleep(self.reconnect_delay_seconds)

        last_symbols_reload = asyncio.get_running_loop().time()
        last_subs_reload = asyncio.get_running_loop().time()

        while not self._stop_event.is_set():
            now = asyncio.get_running_loop().time()

            symbols_changed = False
            subs_changed = False

            if now - last_symbols_reload >= self.symbols_refresh_seconds:
                try:
                    symbols_changed = await self._reload_symbols()
                except Exception as e:
                    loguru.logger.error(f"Symbols reload error: {e}")
                last_symbols_reload = now

            if now - last_subs_reload >= self.subscriptions_refresh_seconds:
                try:
                    subs_changed = await self._reload_subscriptions()
                except Exception as e:
                    loguru.logger.error(f"Subscriptions reload error: {e}")
                last_subs_reload = now

            if symbols_changed:
                if ws_streams is not None:
                    try:
                        await self._sync_symbol_streams(ws_streams)
                    except Exception:
                        # транспорт/transport may die during resubscribe: reconnect and retry
                        try:
                            self._symbol_stream_handles.clear()
                            ws_streams = await self.client_ws.websocket_streams.create_connection()
                            await self._sync_symbol_streams(ws_streams)
                        except Exception as e:
                            self._resubscribe_retries += 1
                            self._last_error = str(e)
                            loguru.logger.warning(f"WS resubscribe retry failed: {e}")

            # subs_changed ни на какие websocket-подписки не влияет: diff_percent обновляется в памяти
            await asyncio.sleep(1)


async def run_binance_kline_alert_engine(bot: Bot) -> None:
    global _ENGINE_INSTANCE
    engine = BinanceKlineAlertEngine(bot)
    _ENGINE_INSTANCE = engine
    await engine.run_forever()


def get_binance_kline_alert_engine_stats() -> dict:
    if _ENGINE_INSTANCE is None:
        return {
            "started": False,
            "window_size": WINDOW_SIZE,
            "ws_connections": 0,
            "ws_reconnect_tasks": 0,
            "token_streams_active": 0,
            "symbols_loaded": 0,
            "active_user_subscriptions": 0,
            "queue_size": 0,
            "workers": 0,
            "connect_retries": 0,
            "resubscribe_retries": 0,
            "subscribe_errors": 0,
            "worker_errors": 0,
            "handle_errors": 0,
            "last_error": "engine not started",
        }
    return _ENGINE_INSTANCE.get_stats()
