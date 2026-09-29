from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from bot.db.database import async_session
from bot.db.models import BinanceKlineAlertSubscription
from bot.db.services.crud import CRUD


class KlineAlertSubscriptionService:
    def __init__(self):
        super().__init__()
        self.repo = CRUD(model=BinanceKlineAlertSubscription)

    async def upsert_subscription(self,
                                  *,
                                  user_id: int,
                                  diff_percent: float,
                                  window_size: str,
                                  ) -> None:
        """
        Атомарный upsert по (user_id, window_size) без гонки read-then-write.
        """
        async with async_session() as session:
            stmt = sqlite_insert(BinanceKlineAlertSubscription).values(
                user_id=user_id,
                diff_percent=diff_percent,
                window_size=window_size,
                active=True,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['user_id', 'window_size'],
                set_={'diff_percent': diff_percent, 'active': True},
            )
            await session.execute(stmt)
            await session.commit()

    async def remove_user_subscriptions(self,
                                        *,
                                        user_id: int,
                                        ) -> None:
        await self.repo.delete(filter_by={'user_id': user_id})

    async def deactivate_user_subscriptions(self,
                                            *,
                                            user_id: int,
                                            ) -> None:
        await self.repo.update(filter_by={'user_id': user_id}, values={'active': False})

    async def get_all_subscriptions(self) -> list[dict]:
        return await self.repo.read(filter_by={})

    async def get_user_subscriptions(self,
                                     *,
                                     user_id: int,
                                     window_size: str | None = None,
                                     ) -> list[dict]:
        filter_by = {'user_id': user_id}
        if window_size is not None:
            filter_by['window_size'] = window_size
        return await self.repo.read(filter_by=filter_by)

    async def update_subscription_diff(self,
                                       *,
                                       subscription_id: int,
                                       user_id: int,
                                       diff_percent: float,
                                       ) -> bool:
        subscription = await self.repo.find_one(
            filter_by={'id': subscription_id, 'user_id': user_id}
        )
        if not subscription:
            return False

        await self.repo.update(
            filter_by={'id': subscription_id, 'user_id': user_id},
            values={'diff_percent': diff_percent},
        )
        return True

    async def toggle_subscription_active(self,
                                         *,
                                         subscription_id: int,
                                         user_id: int,
                                         ) -> bool:
        subscription = await self.repo.find_one(
            filter_by={'id': subscription_id, 'user_id': user_id}
        )
        if not subscription:
            return False

        current_active = bool(subscription.get('active', True))
        await self.repo.update(
            filter_by={'id': subscription_id, 'user_id': user_id},
            values={'active': not current_active},
        )
        return True
