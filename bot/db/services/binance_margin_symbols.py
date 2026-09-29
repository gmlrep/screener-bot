from sqlalchemy import select

from bot.db.database import async_session
from bot.db.models import BinanceMarginSymbols
from bot.db.services.crud import CRUD


class BinanceMarginSymbolsService:

    def __init__(self):
        super().__init__()
        self.user_repo = CRUD(model=BinanceMarginSymbols)

    
    async def create_binance_margin_symbols(self,
                                            data: dict
                                            ) -> None:
        async with async_session() as session:
            for symbol, dt in data.items():
                resp = await session.execute(select(BinanceMarginSymbols.id
                                                    ).filter(BinanceMarginSymbols.symbol == symbol))
                resp = resp.scalar()

                if resp is not None:
                    continue

                symbol_obj = BinanceMarginSymbols(symbol=symbol,
                                                title=dt.get('title'))
                session.add(symbol_obj)
            await session.commit()

    async def get_symbols(self,
                          filter_by: dict | None = None,
                          ) -> dict:
        if filter_by is None:
            filter_by = {}

        async with async_session() as session:
            resp = await session.execute(select(BinanceMarginSymbols).filter_by(**filter_by))
            resp = resp.scalars()
            resp = resp.all()
            symbols = {}

            for symbol in resp:
                data = {
                    f'{symbol.symbol}': {
                        'title': symbol.title,
                        'symbol': symbol.symbol,
                    }
                }
                symbols.update(data)
            return symbols
