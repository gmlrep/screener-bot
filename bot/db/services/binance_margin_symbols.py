from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from bot.db.database import async_session
from bot.db.models import BinanceMarginSymbols
from bot.db.services.crud import CRUD


class BinanceMarginSymbolsService:

    def __init__(self):
        super().__init__()
        self.user_repo = CRUD(model=BinanceMarginSymbols)

    async def sync_binance_margin_symbols(self, data: dict) -> None:
        """
        Полная синхронизация списка символов: upsert новых/изменённых одним
        bulk-запросом и удаление тех, которых больше нет на бирже.
        """
        if not data:
            return

        rows = [
            {
                'symbol': symbol,
                'title': info.get('title') or symbol,
            }
            for symbol, info in data.items()
        ]
        symbols = [row['symbol'] for row in rows]

        async with async_session() as session:
            stmt = sqlite_insert(BinanceMarginSymbols).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'],
                set_={'title': stmt.excluded.title},
            )
            await session.execute(stmt)

            # Удаляем символы, которых больше нет в актуальном списке.
            await session.execute(
                BinanceMarginSymbols.__table__.delete().where(
                    BinanceMarginSymbols.symbol.not_in(symbols)
                )
            )
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
                symbols[symbol.symbol] = {
                    'title': symbol.title,
                    'symbol': symbol.symbol,
                }
            return symbols
