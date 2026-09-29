import datetime

from sqlalchemy import Integer, String, BigInteger, Float, func, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from bot.db.database import Base


class User(Base):
    __tablename__ = 'user'

    id: Mapped[int] = mapped_column(Integer, autoincrement=True, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    username: Mapped[str] = mapped_column(unique=True)
    user_fullname: Mapped[str] = mapped_column()
    create_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    alert_20m: Mapped[bool] = mapped_column(nullable=True, default=False)

    def __repr__(self):
        return f"<User(id={self.id}, user_id={self.user_id!r}, username={self.username!r})>"


class BinanceMarginSymbols(Base):
    __tablename__ = 'binance_margin_symbols'

    id: Mapped[int] = mapped_column(Integer, autoincrement=True, primary_key=True)
    symbol: Mapped[str] = mapped_column(nullable=False, unique=True)
    title: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self):
        return f"<BinanceMarginSymbols(id={self.id}, symbol={self.symbol!r}, title={self.title!r})>"


class BinanceKlineAlertSubscription(Base):
    """
    Подписка пользователя на уведомления по kline:
    - window_size: интервал свечи (например, "15m")
    - diff_percent: порог изменения цены в процентах (сравниваем по abs(change))
    """

    __tablename__ = 'binance_kline_alert_subscriptions'

    id: Mapped[int] = mapped_column(Integer, autoincrement=True, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    window_size: Mapped[str] = mapped_column(String(20), nullable=False)
    diff_percent: Mapped[float] = mapped_column(Float, nullable=False)
    active: Mapped[bool] = mapped_column(nullable=False, default=True)
    create_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    __table_args__ = (
        # Разрешаем одну подписку на интервал на пользователя; diff_percent можно обновлять.
        UniqueConstraint('user_id', 'window_size', name='uq_user_window_size'),
    )

    def __repr__(self):
        return f"<BinanceKlineAlertSubscription(id={self.id}, user_id={self.user_id!r}, window_size={self.window_size!r}, diff_percent={self.diff_percent}%>)"
