import asyncio

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

from bot.db.config import settings
from bot.db.services.binance_margin_symbols import BinanceMarginSymbolsService


class BinanceScreener:
    conf = ConfigurationRestAPI(api_key=settings.binance.api_key,
                                api_secret=settings.binance.api_secret)

    client = DerivativesTradingUsdsFutures(config_rest_api=conf)

    async def _get_symbols(self) -> dict:
        # SDK-клиент синхронный, поэтому REST-запрос уводим из event loop в executor.
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            self.client.rest_api.exchange_information,
        )
        data = response.data()

        symbols_data = {}
        for symbol in data.symbols:
            symbols_data[symbol.symbol] = {
                'title': symbol.base_asset,
                'symbol': symbol.symbol,
            }
        return symbols_data

    async def update_symbols_list(self) -> None:
        symbols_data = await self._get_symbols()
        service = BinanceMarginSymbolsService()
        await service.sync_binance_margin_symbols(data=symbols_data)
