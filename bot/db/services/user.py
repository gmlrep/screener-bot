from sqlalchemy import func, not_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError

from bot.db.database import async_session
from bot.db.models import User
from bot.db.services.crud import CRUD


class UserService:

    def __init__(self):
        super().__init__()
        self.user_repo = CRUD(model=User)

    async def add_user(self, **data) -> bool:
        try:
            await self.user_repo.create(data=data)
            return True
        except IntegrityError:
            return False

    async def find_user_status(self,
                               filter_by: dict,
                               alert_field: str = 'alert_20m',
                               ) -> bool:
        user = await self.user_repo.find_one(filter_by=filter_by)

        if not user:
            return False

        return bool(user.get(alert_field))

    async def find_users_with_alert(self,
                                    alert_field: str = 'alert_20m',
                                    ) -> list[int]:
        users = await self.user_repo.read(filter_by={alert_field: True})
        return [u.get('user_id') for u in users]

    async def update_alert(self,
                           filter_by: dict,
                           alert_field: str = 'alert_20m',
                           ) -> None:
        column = User.__table__.c.get(alert_field)
        if column is None:
            return
        await self.user_repo.update(filter_by=filter_by,
                                    values={alert_field: not_(column)})

    async def count_users(self) -> int:
        async with async_session() as session:
            response = await session.execute(select(func.count()).select_from(User))
            return int(response.scalar_one())

    async def insert_user(self,
                          user_id: int,
                          username: str | None,
                          fullname: str,
                          ) -> None:
        """
        Идемпотентный upsert пользователя по user_id в рамках одной сессии.
        Раньше username мог быть None и нарушал NOT NULL/UNIQUE.
        """
        async with async_session() as session:
            stmt = sqlite_insert(User).values(
                user_id=user_id,
                username=username,
                user_fullname=fullname,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['user_id'],
                set_={
                    'username': username,
                    'user_fullname': fullname,
                },
            )
            await session.execute(stmt)
            await session.commit()
