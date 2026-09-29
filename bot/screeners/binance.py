import asyncio
import time

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

from bot.db.config import settings
from bot.db.services.binance_margin_symbols import BinanceMarginSymbolsService

SECONDS_PER_DAY = 86_400
MILLISECONDS_PER_DAY = SECONDS_PER_DAY * 1000


def _field(item: object, *names: str) -> object | None:
    """Достаёт поле из pydantic-модели или dict (с учётом camelCase/snake_case)."""
    for name in names:
        if isinstance(item, dict):
            if name in item:
                return item[name]
        else:
            value = getattr(item, name, None)
            if value is not None:
                return value
    return None


class BinanceScreener:
    conf = ConfigurationRestAPI(api_key=settings.binance.api_key,
                                api_secret=settings.binance.api_secret)

    client = DerivativesTradingUsdsFutures(config_rest_api=conf)

    def _get_symbols(self) -> dict:
        response = self.client.rest_api.exchange_information()
        data = response.data()

        symbols_data = {}
        for symbol in data.symbols:
            symbols_data[symbol.symbol] = {
                'title': symbol.base_asset,
                'symbol': symbol.symbol,
                'onboard_date': symbol.onboard_date,
            }
        return symbols_data

    def _get_quote_volumes(self) -> dict[str, float]:
        """24h quote volume по всем символам (один запрос, weight=40)."""
        response = self.client.rest_api.ticker24hr_price_change_statistics()
        data = response.data()

        # SDK может вернуть oneof-обёртку или список моделей/словарей.
        if hasattr(data, 'actual_instance'):
            data = data.actual_instance
        items = data if isinstance(data, (list, tuple)) else [data]

        volumes: dict[str, float] = {}
        for item in items:
            if item is None:
                continue
            symbol = _field(item, 'symbol')
            quote_volume = _field(item, 'quote_volume', 'quoteVolume')
            if symbol is None or quote_volume is None:
                continue
            try:
                volumes[str(symbol)] = float(quote_volume)
            except (TypeError, ValueError):
                continue
        return volumes

    def _collect_filtered_symbols(self) -> dict:
        """Возвращает символы, прошедшие фильтры по объёму и истории, для записи в БД."""
        symbols = self._get_symbols()

        min_volume = settings.binance.min_quote_volume
        volumes = self._get_quote_volumes() if min_volume > 0 else {}

        min_history_days = settings.binance.min_history_days
        min_onboard_ms = int(time.time() * 1000) - min_history_days * MILLISECONDS_PER_DAY

        result = {}
        skipped_volume = 0
        skipped_history = 0
        for symbol, info in symbols.items():
            if min_volume > 0 and volumes.get(symbol, 0.0) < min_volume:
                skipped_volume += 1
                continue

            if min_history_days > 0:
                onboard_date = info.get('onboard_date')
                if not onboard_date or onboard_date > min_onboard_ms:
                    skipped_history += 1
                    continue

            result[symbol] = {'title': info['title'], 'symbol': symbol}

        return result, skipped_volume, skipped_history

    async def update_symbols_list(self) -> tuple[int, int, int]:
        """
        Обновляет список символов в БД с учётом фильтров.
        Возвращает (сколько сохранено, отброшено по объёму, отброшено по истории).
        """
        loop = asyncio.get_running_loop()
        symbols_data, skipped_volume, skipped_history = await loop.run_in_executor(
            None, self._collect_filtered_symbols
        )

        service = BinanceMarginSymbolsService()
        await service.sync_binance_margin_symbols(data=symbols_data)
        return len(symbols_data), skipped_volume, skipped_history
