from sqlalchemy import delete, insert, inspect, select, update

from bot.db.database import async_session


class CRUD:
    def __init__(self, model):
        self.model = model

    async def create(self, data: dict):
        async with async_session() as session:
            stmt = insert(self.model).values(**data)
            await session.execute(stmt)
            await session.commit()

    async def read(self, filter_by: dict):
        async with async_session() as session:
            stmt = select(self.model).filter_by(**filter_by)
            data = await session.execute(stmt)
            res = data.scalars().all()
            return [self._model_to_dict(r) for r in res]

    async def update(self, filter_by: dict, values: dict):
        async with async_session() as session:
            stmt = update(self.model).filter_by(**filter_by).values(values)
            await session.execute(stmt)
            await session.commit()

    async def delete(self, filter_by: dict):
        async with async_session() as session:
            stmt = delete(self.model).filter_by(**filter_by)
            await session.execute(stmt)
            await session.commit()

    @staticmethod
    def _model_to_dict(instance,
                       ) -> dict:
        return {c.key: getattr(instance, c.key) for c in inspect(instance).mapper.column_attrs}

    async def find_one(self, filter_by: dict):
        async with async_session() as session:
            stmt = select(self.model).filter_by(**filter_by)
            data = await session.execute(stmt)
            res = data.scalars().first()
            if res is None:
                return None
            return self._model_to_dict(res)
