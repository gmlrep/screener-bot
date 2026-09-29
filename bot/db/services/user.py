from sqlalchemy import not_, select, update
from sqlalchemy.exc import IntegrityError

from bot.db.database import async_session
from bot.db.models import User
from bot.db.services.crud import CRUD


class UserService:

    def __init__(self):
        super().__init__()
        self.user_repo = CRUD(model=User)

    async def add_user(self,
                       **data
                       ) -> bool:
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

        user = dict(user)
        return user.get(alert_field)

    async def find_users_with_alert(self,
                                    alert_field: str = 'alert_20m',
                                    ) -> list[int]:
        users = await self.user_repo.read(filter_by={alert_field: True})
        user_ids = [u.get('user_id') for u in users]
        return user_ids

    async def update_alert(self,
                           filter_by: dict,
                           alert_field: str = 'alert_20m',
                           ) -> None:
        await self.user_repo.update(filter_by=filter_by,
                                    values={alert_field: not_(User.__table__.c.get(alert_field))})

    async def count_users(self) -> int:
        async with async_session() as session:
            response = await session.execute(select(User.username))
            count = len(response.scalars().all())
            return count
    
    async def insert_user(self, user_id: int, username: str, fullname: str) -> None:
        async with async_session() as session:
            resp = await session.execute(select(User.user_id).filter_by(user_id=user_id))
            resp = resp.scalar()
            if resp is None:
                await self.user_repo.create(data={
                    'user_id': user_id,
                    'username': username,
                    'user_fullname': fullname,
                })
            else:
                await self.user_repo.update(filter_by={'user_id': user_id}, values={'username': username})
