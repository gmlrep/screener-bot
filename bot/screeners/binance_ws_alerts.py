import asyncio
import time
from dataclasses import dataclass
from html import escape

import loguru
from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError
from binance_common.configuration import ConfigurationWebSocketStreams
from binance_common.constants import WebsocketMode
from binance_common.utils import get_uuid, ws_streams_placeholder
from binance_common.websocket import RequestStreamHandle, global_stream_connections
from binance_sdk_derivatives_trading_usds_futures import DerivativesTradingUsdsFutures
from binance_sdk_derivatives_trading_usds_futures.websocket_streams.models import (
    KlineCandlestickStreamsResponse,
)

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


def _window_to_seconds(window: str) -> int:
    """'15m' -> 900, '1h' -> 3600 и т.п."""
    if not window:
        return 60
    unit = window[-1].lower()
    try:
        amount = int(window[:-1])
    except ValueError:
        return 60
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return amount * multipliers.get(unit, 60)


def _stream_name(symbol: str) -> str:
    return ws_streams_placeholder(
        "/<symbol>@kline_<interval>".replace("/", "", 1),
        {"symbol": symbol, "interval": WINDOW_SIZE},
    )


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
        self.subscribe_chunk_size = max(1, settings.binance.alert_subscribe_chunk_size)

        # SDK не реконнектит соединение на CLOSE/ERROR, поэтому idle-порог держим
        # независимо от окна свечи (по любому сообщению по движку).
        self.ws_idle_timeout_seconds = max(60, settings.binance.alert_ws_idle_timeout_seconds)
        # TTL dedupe-ключа должен перекрывать длину свечи, иначе возможны дубли.
        self._notified_ttl_seconds = max(
            settings.binance.alert_cache_ttl_seconds,
            _window_to_seconds(WINDOW_SIZE) + 120,
        )

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
        # Событие для немедленного перечитывания символов (например, после /upd_symbols).
        self._symbols_reload_event = asyncio.Event()

        # list of subscriptions for fixed WINDOW_SIZE (отсортирован по diff_percent)
        self._subs_ref: list[KlineSubscription] = []
        self._subs_fingerprint: str = ""

        # symbol -> RequestStreamHandle (нужно, чтобы можно было отписаться при изменении списка символов)
        self._symbol_stream_handles: dict[str, RequestStreamHandle] = {}
        # Символы, по которым подписка не удалась; повторяем только их (P3).
        self._failed_symbols: set[str] = set()
        self._failed_retry_at: float = 0.0

        # (symbol, window_size, candle_open_time) -> True
        self._notified_cache = LRUCache(capacity=20000)

        # ограничитель одновременных send_message (Telegram rate limit)
        self._send_semaphore = asyncio.Semaphore(settings.binance.alert_send_concurrency)

        # Очередь входящих свечей и воркеры обработки.
        self._kline_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=settings.binance.alert_queue_maxsize)
        self._kline_workers: list[asyncio.Task] = []
        self._kline_workers_count = settings.binance.alert_workers_count

        self._stop_event = asyncio.Event()
        self._started = False
        self._last_message_monotonic = time.monotonic()

        # Runtime metrics for admin diagnostics
        self._connect_retries = 0
        self._subscribe_errors = 0
        self._worker_errors = 0
        self._handle_errors = 0
        self._resubscribe_retries = 0
        self._last_error: str | None = None

    async def stop(self) -> None:
        self._stop_event.set()
        for worker in self._kline_workers:
            worker.cancel()
        self._kline_workers.clear()

    def request_symbols_reload(self) -> None:
        """Просит движок немедленно перечитать список символов (P7)."""
        self._symbols_reload_event.set()

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
            except Exception as e:  # noqa: BLE001 - воркер не должен падать от одной ошибки
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

        # window_size фиксирован, поэтому игнорируем значение из БД,
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
        # Сортировка по порогу позволяет в горячем пути выходить досрочно (P6).
        self._subs_ref.sort(key=lambda s: s.diff_percent)

        fingerprint = str(sorted((s.user_id, s.diff_percent) for s in self._subs_ref))
        if fingerprint == self._subs_fingerprint:
            return False

        self._subs_fingerprint = fingerprint
        return True

    @staticmethod
    def _extract_kline(data: object) -> dict | None:
        """Быстро достаёт нужные поля без полного model_dump (P6)."""
        if not isinstance(data, dict):
            k = getattr(data, "k", None)
            if k is None:
                model_dump = getattr(data, "model_dump", None)
                if callable(model_dump):
                    return BinanceKlineAlertEngine._extract_kline(model_dump())
                return None

            symbol = getattr(data, "s", None)
            if symbol is None:
                return None
            return {
                "s": symbol,
                "o": getattr(k, "o", None),
                "c": getattr(k, "c", None),
                "t": getattr(k, "t", None),
                "T": getattr(k, "T", None),
            }

        payload = data["data"] if isinstance(data.get("data"), dict) else data
        k = payload.get("k") or {}
        if not k:
            return None
        return {
            "s": payload.get("s") or k.get("s"),
            "o": k.get("o"),
            "c": k.get("c"),
            "t": k.get("t"),
            "T": k.get("T") or payload.get("T"),
        }

    def _on_ws_message(self, data: object) -> None:
        self._last_message_monotonic = time.monotonic()
        payload = self._extract_kline(data)
        if payload is None:
            return

        try:
            self._kline_queue.put_nowait(payload)
        except asyncio.QueueFull:
            # Если очередь заполнена, снимаем самый старый элемент и кладем новый.
            try:
                _ = self._kline_queue.get_nowait()
                self._kline_queue.task_done()
                self._kline_queue.put_nowait(payload)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    async def _batch_subscribe_symbols(self, ws_streams: object, symbols: list[str]) -> set[str]:
        """Батчевый SUBSCRIBE одним params-массивом вместо sleep(0.5) на символ (P1).

        Возвращает множество символов, для которых подписка не удалась.
        """
        if not symbols:
            return set()

        connections = [
            connection
            for connection in ws_streams.connections
            if getattr(connection, "url_path", None) == "market"
        ]
        if not connections:
            raise RuntimeError("no 'market' websocket connections available")

        failed: set[str] = set()
        for idx, start in enumerate(range(0, len(symbols), self.subscribe_chunk_size)):
            chunk = symbols[start:start + self.subscribe_chunk_size]
            connection = connections[idx % len(connections)]
            streams = [_stream_name(symbol) for symbol in chunk]

            try:
                await ws_streams.send_message(
                    {"method": "SUBSCRIBE", "params": streams, "id": get_uuid()},
                    connection,
                )
            except Exception as e:  # noqa: BLE001 - фиксируем и повторяем только этот чанк
                self._subscribe_errors += 1
                self._last_error = str(e)
                loguru.logger.warning(f"batch subscribe failed symbols={chunk}: {e}")
                failed.update(chunk)
                continue

            for symbol, stream in zip(chunk, streams, strict=False):
                global_stream_connections.stream_connections_map[stream] = connection
                connection.stream_callback_map.setdefault(stream, [])
                connection.response_types[stream] = KlineCandlestickStreamsResponse

                handle = RequestStreamHandle(ws_streams, stream, KlineCandlestickStreamsResponse)
                handle.on("message", self._on_ws_message)
                self._symbol_stream_handles[symbol] = handle

            if self.subscribe_delay_seconds > 0:
                await asyncio.sleep(self.subscribe_delay_seconds)

        return failed

    async def _drop_all_streams(self) -> None:
        """Отписываемся от всех стримов перед reconnect (P4: параллельно)."""
        handles = list(self._symbol_stream_handles.values())
        self._symbol_stream_handles.clear()
        if not handles:
            return

        semaphore = asyncio.Semaphore(16)

        async def _unsubscribe(handle: RequestStreamHandle) -> None:
            async with semaphore:
                try:
                    await handle.unsubscribe()
                except Exception as e:  # noqa: BLE001 - unsubscribe на мёртвом сокете не критичен
                    loguru.logger.debug(f"unsubscribe failed: {e}")

        await asyncio.gather(*(_unsubscribe(handle) for handle in handles), return_exceptions=True)

    async def _sync_symbol_streams(self, ws_streams: object) -> set[str]:
        """
        Приводим набор подписок к набору символов в `self.symbol_titles`.
        Возвращает множество символов, по которым подписка не удалась.
        """
        current_symbols = set(self.symbol_titles.keys())
        existing_symbols = set(self._symbol_stream_handles.keys())

        removed = existing_symbols - current_symbols
        added = current_symbols - existing_symbols

        # Unsubscribe удалённых (параллельно).
        if removed:
            handles = [self._symbol_stream_handles.pop(symbol, None) for symbol in removed]
            handles = [handle for handle in handles if handle is not None]
            semaphore = asyncio.Semaphore(16)

            async def _unsubscribe(handle: RequestStreamHandle) -> None:
                async with semaphore:
                    try:
                        await handle.unsubscribe()
                    except Exception as e:  # noqa: BLE001 - reconnect мог уже убить сокет
                        loguru.logger.debug(f"unsubscribe failed: {e}")

            await asyncio.gather(*(_unsubscribe(handle) for handle in handles), return_exceptions=True)

        return await self._batch_subscribe_symbols(ws_streams, sorted(added))

    async def _handle_kline_message(self, *, data: dict, window_size: str) -> None:
        """
        data — компактный payload {'s','o','c','t','T'}, подготовленный _extract_kline.
        """
        try:
            symbol = data.get("s")
            o_raw = data.get("o")
            c_raw = data.get("c")
            if not symbol or o_raw is None or c_raw is None:
                return

            if symbol not in self.symbol_titles:
                # могли подписаться на интервал до обновления символов
                return

            open_price = float(o_raw)
            last_price = float(c_raw)
            if open_price == 0:
                return

            change_pct = (last_price - open_price) / open_price * 100.0
            abs_change_pct = abs(change_pct)

            # dedupe в рамках одной свечи: не шлем повторно по той же свече.
            candle_open_time = data.get("t")
            if candle_open_time is None:
                candle_open_time = data.get("T")
            dedupe_key = f"{symbol}:{window_size}:{candle_open_time}"

            if self._notified_cache.get(dedupe_key):
                return

            if not self._subs_ref:
                return

            # _subs_ref отсортирован по diff_percent — выходим досрочно (P6).
            users_to_notify: list[int] = []
            for sub in self._subs_ref:
                if abs_change_pct < sub.diff_percent:
                    break
                users_to_notify.append(sub.user_id)

            if not users_to_notify:
                return

            # помечаем как уведомленное до отправки, чтобы избежать дублей при ределивери
            self._notified_cache.set(dedupe_key, True, ttl=self._notified_ttl_seconds)

            title = escape(self.symbol_titles.get(symbol, symbol), quote=False)
            status = "🟢 Pump" if change_pct >= 0 else "🔴 Dump"

            msg_text = (
                f'<a href="https://www.binance.com/ru/futures/{symbol}">🟧 Binance</a> - {window_size} - '
                f'<a href="https://www.coinglass.com/tv/Binance_{symbol}">{title}</a>\n'
                f'{status}: {change_pct:.2f}% ({open_price:.8g} - {last_price:.8g})'
            )

            async def _send_one(chat_id: int) -> None:
                async with self._send_semaphore:
                    await self.bot.send_message(
                        text=msg_text,
                        chat_id=chat_id,
                        disable_web_page_preview=True,
                        parse_mode="HTML",
                    )

            user_ids = sorted(set(users_to_notify))
            results = await asyncio.gather(
                *(_send_one(uid) for uid in user_ids),
                return_exceptions=True,
            )
            for uid, res in zip(user_ids, results, strict=False):
                if isinstance(res, TelegramForbiddenError):
                    # Пользователь заблокировал бота — отключаем подписку, чтобы не долбиться.
                    await KlineAlertSubscriptionService().deactivate_user_subscriptions(user_id=uid)
                    loguru.logger.info(f"User {uid} blocked the bot, subscriptions disabled")
                elif isinstance(res, Exception):
                    self._last_error = str(res)
                    loguru.logger.warning(f"send failed user={uid}: {res}")

            loguru.logger.info(
                f"Alert sent: symbol={symbol} change={change_pct:.2f}% users={len(user_ids)}"
            )
        except Exception as e:  # noqa: BLE001 - обработчик не должен ронять воркер
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
            "failed_symbols": len(self._failed_symbols),
            "subscribe_chunk_size": self.subscribe_chunk_size,
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

    async def _connect_ws(self) -> object | None:
        """Устанавливает соединение и подписывается на все символы. None, если остановлено."""
        while not self._stop_event.is_set():
            try:
                ws_streams = await self.client_ws.websocket_streams.create_connection()
                self._failed_symbols = await self._sync_symbol_streams(ws_streams)
                self._last_message_monotonic = time.monotonic()
                return ws_streams
            except Exception as e:  # noqa: BLE001 - цикл реконнекта
                self._connect_retries += 1
                self._last_error = str(e)
                loguru.logger.warning(f"WS connect/retry error: {e}")
                await self._drop_all_streams()
                await asyncio.sleep(self.reconnect_delay_seconds)
        return None

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
        except Exception as e:  # noqa: BLE001
            loguru.logger.error(f"Initial symbols load error: {e}")
        try:
            await self._reload_subscriptions()
        except Exception as e:  # noqa: BLE001
            loguru.logger.error(f"Initial subscriptions load error: {e}")

        # create_connection() делается ОДИН раз, чтобы не плодить лишние сокеты
        ws_streams = await self._connect_ws()
        if ws_streams is None:
            return

        loop = asyncio.get_running_loop()
        last_symbols_reload = loop.time()
        last_subs_reload = loop.time()

        while not self._stop_event.is_set():
            now = loop.time()

            # Health check: если по WS давно ничего не приходило — переподключаемся (P5).
            if self.symbol_titles and now - self._last_message_monotonic > self.ws_idle_timeout_seconds:
                loguru.logger.warning("WS stream looks idle, reconnecting")
                await self._drop_all_streams()
                ws_streams = await self._connect_ws()
                if ws_streams is None:
                    return
                last_symbols_reload = loop.time()
                last_subs_reload = last_symbols_reload
                continue

            force_symbols_reload = self._symbols_reload_event.is_set()
            if force_symbols_reload:
                self._symbols_reload_event.clear()

            symbols_changed = False
            if force_symbols_reload or now - last_symbols_reload >= self.symbols_refresh_seconds:
                try:
                    symbols_changed = await self._reload_symbols()
                except Exception as e:  # noqa: BLE001
                    loguru.logger.error(f"Symbols reload error: {e}")
                last_symbols_reload = now

            if now - last_subs_reload >= self.subscriptions_refresh_seconds:
                try:
                    await self._reload_subscriptions()
                except Exception as e:  # noqa: BLE001
                    loguru.logger.error(f"Subscriptions reload error: {e}")
                last_subs_reload = now

            # subs_changed ни на какие websocket-подписки не влияет: diff_percent обновляется в памяти
            if symbols_changed and ws_streams is not None:
                try:
                    self._failed_symbols = await self._sync_symbol_streams(ws_streams)
                except Exception as e:  # noqa: BLE001
                    self._resubscribe_retries += 1
                    self._last_error = str(e)
                    loguru.logger.warning(f"WS resubscribe failed, reconnecting: {e}")
                    await self._drop_all_streams()
                    ws_streams = await self._connect_ws()
                    if ws_streams is None:
                        return
                    last_symbols_reload = loop.time()
                    last_subs_reload = last_symbols_reload

            # P3: повторяем только символы, по которым подписка упала, без полного reconnect.
            if self._failed_symbols and ws_streams is not None and now >= self._failed_retry_at:
                try:
                    self._failed_symbols = await self._sync_symbol_streams(ws_streams)
                except Exception as e:  # noqa: BLE001
                    self._resubscribe_retries += 1
                    self._last_error = str(e)
                    loguru.logger.warning(f"WS failed-symbols retry error, reconnecting: {e}")
                    await self._drop_all_streams()
                    ws_streams = await self._connect_ws()
                    if ws_streams is None:
                        return
                self._failed_retry_at = loop.time() + self.reconnect_delay_seconds

            await asyncio.sleep(1)


async def run_binance_kline_alert_engine(bot: Bot) -> None:
    global _ENGINE_INSTANCE
    engine = BinanceKlineAlertEngine(bot)
    _ENGINE_INSTANCE = engine
    await engine.run_forever()


def get_binance_kline_alert_engine() -> "BinanceKlineAlertEngine | None":
    return _ENGINE_INSTANCE


def get_binance_kline_alert_engine_stats() -> dict:
    if _ENGINE_INSTANCE is None:
        return {
            "started": False,
            "window_size": WINDOW_SIZE,
            "ws_connections": 0,
            "ws_reconnect_tasks": 0,
            "token_streams_active": 0,
            "failed_symbols": 0,
            "subscribe_chunk_size": 0,
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
