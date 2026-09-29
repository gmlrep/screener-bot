from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

from bot.db.config import settings
from bot.db.services.binance_margin_symbols import BinanceMarginSymbolsService


class BinanceScreener:
    conf = ConfigurationRestAPI(api_key=settings.binance.api_key,
                                api_secret=settings.binance.api_secret)

    client = DerivativesTradingUsdsFutures(config_rest_api=conf)

    def _get_symbols(self):
        response = self.client.rest_api.exchange_information()
        data = response.data()

        symbols_data = {}

        symbols = data.symbols
        for symbol in symbols:
            data = {
                f'{symbol.symbol}': {
                    'title': symbol.base_asset,
                    'symbol': symbol.symbol,
                }
            }
            symbols_data.update(data)
        return symbols_data

    async def update_symbols_list(self) -> None:
        data = self._get_symbols()
        servive = BinanceMarginSymbolsService()
        await servive.create_binance_margin_symbols(data=data)
