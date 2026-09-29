from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from bot.db.config import settings

# Гарантируем наличие каталога под файл SQLite (если путь кастомизирован через DB_PATH).
settings.db.path.parent.mkdir(parents=True, exist_ok=True)

async_engine = create_async_engine(settings.db.db_url, echo=settings.db.echo)

# expire_on_commit=False — чтобы ORM-объекты не протухали после commit и не вызывали
# неожиданную ленивую подгрузку (MissingGreenlet) в async-коде.
async_session = async_sessionmaker(async_engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass
