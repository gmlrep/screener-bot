from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from bot.db.config import settings
from bot.db.services.kline_alert_subscription import KlineAlertSubscriptionService
from bot.db.services.user import UserService
from bot.keyboards.keyboards import kb_user_alerts, kb_user_settings

user_router = Router()

MAX_DIFF_PERCENT = 1000.0


class UserSettingsState(StatesGroup):
    waiting_new_diff_percent = State()


def _format_subscriptions(subscriptions: list[dict]) -> str:
    if not subscriptions:
        return f"У вас нет подписок на {settings.binance.alert_window_size}."

    rows = []
    for sub in subscriptions:
        active_text = "активна" if sub.get("active", True) else "неактивна"
        rows.append(f"• {sub.get('window_size')}: {sub.get('diff_percent')}% ({active_text})")
    return "Ваши подписки:\n" + "\n".join(rows)


async def _edit_or_reply(message: Message, text: str, markup) -> None:
    try:
        await message.edit_text(text=text, reply_markup=markup)
    except TelegramBadRequest:
        await message.answer(text=text, reply_markup=markup)


def _parse_diff_percent(raw: str) -> float | None:
    try:
        value = float(raw.strip().replace(",", "."))
    except ValueError:
        return None
    if value <= 0 or value > MAX_DIFF_PERCENT:
        return None
    return value


# Обработка команды start
@user_router.message(Command("start"))
async def start_handler(message: Message):
    service = UserService()
    await service.insert_user(user_id=message.from_user.id,
                              username=message.from_user.username,
                              fullname=message.from_user.full_name)
    await message.answer(text='Привет, я могу оповещать об изменениях цены токенов на фьючерсной торговле.',
                         reply_markup=kb_user_settings())


@user_router.message(Command("alert"))
async def subscribe_alert_handler(message: Message):
    """
    Пример:
    /alert 4.5
    где 4.5 - порог abs(price_change_percent).
    """
    if not message.text:
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /alert <diff_percent>, пример: /alert 4.0")
        return

    diff_percent = _parse_diff_percent(parts[1])
    if diff_percent is None:
        await message.answer(f"diff_percent должен быть числом > 0 и <= {MAX_DIFF_PERCENT:g}. Пример: /alert 4.0")
        return

    service = KlineAlertSubscriptionService()
    await service.upsert_subscription(
        user_id=message.from_user.id,
        diff_percent=diff_percent,
        window_size=settings.binance.alert_window_size,
    )

    await message.answer(f"Оповещения включены: abs(change) >= {diff_percent}% на {settings.binance.alert_window_size}")


@user_router.message(Command("alert_off"))
async def unsubscribe_alert_handler(message: Message):
    service = KlineAlertSubscriptionService()
    await service.remove_user_subscriptions(user_id=message.from_user.id)
    await message.answer("Оповещения выключены.")


@user_router.callback_query(F.data == "user_settings")
async def user_settings_handler(callback: CallbackQuery):
    service = KlineAlertSubscriptionService()
    subscriptions = await service.get_user_subscriptions(
        user_id=callback.from_user.id,
        window_size=settings.binance.alert_window_size,
    )
    text = _format_subscriptions(subscriptions)
    await _edit_or_reply(callback.message, text, kb_user_alerts(subscriptions))
    await callback.answer()


@user_router.callback_query(F.data == "add_alert_subscription")
async def add_alert_subscription_handler(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserSettingsState.waiting_new_diff_percent)
    await state.update_data(subscription_id=None)
    await callback.message.answer("Введите процент изменения для новой подписки (например: 4.0)")
    await callback.answer()


@user_router.callback_query(F.data.startswith("edit_alert:"))
async def edit_alert_subscription_handler(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":", maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        await callback.answer("Некорректная подписка", show_alert=True)
        return

    subscription_id = int(parts[1])
    await state.set_state(UserSettingsState.waiting_new_diff_percent)
    await state.update_data(subscription_id=subscription_id)
    await callback.message.answer("Введите новый процент изменения (например: 5.5)")
    await callback.answer()


@user_router.callback_query(F.data.startswith("toggle_alert_active:"))
async def toggle_alert_active_handler(callback: CallbackQuery):
    parts = callback.data.split(":", maxsplit=1)
    if len(parts) != 2 or not parts[1].isdigit():
        await callback.answer("Некорректная подписка", show_alert=True)
        return

    subscription_id = int(parts[1])
    service = KlineAlertSubscriptionService()
    updated = await service.toggle_subscription_active(
        subscription_id=subscription_id,
        user_id=callback.from_user.id,
    )
    if not updated:
        await callback.answer("Подписка не найдена", show_alert=True)
        return

    subscriptions = await service.get_user_subscriptions(
        user_id=callback.from_user.id,
        window_size=settings.binance.alert_window_size,
    )
    text = _format_subscriptions(subscriptions)
    await _edit_or_reply(callback.message, text, kb_user_alerts(subscriptions))
    await callback.answer()


# Регистрируем state-хендлер последним и не перехватываем команды в состоянии ожидания.
@user_router.message(UserSettingsState.waiting_new_diff_percent, ~F.text.startswith("/"))
async def apply_new_diff_percent_handler(message: Message, state: FSMContext):
    if not message.text:
        return

    new_diff = _parse_diff_percent(message.text)
    if new_diff is None:
        await message.answer(f"Нужно число > 0 и <= {MAX_DIFF_PERCENT:g}. Пример: 4.0")
        return

    data = await state.get_data()
    subscription_id = data.get("subscription_id")
    service = KlineAlertSubscriptionService()
    if subscription_id is None:
        await service.upsert_subscription(
            user_id=message.from_user.id,
            diff_percent=new_diff,
            window_size=settings.binance.alert_window_size,
        )
        updated = True
    else:
        updated = await service.update_subscription_diff(
            subscription_id=int(subscription_id),
            user_id=message.from_user.id,
            diff_percent=new_diff,
        )
    await state.clear()

    if not updated:
        await message.answer("Подписка не найдена или недоступна.")
        return

    subscriptions = await service.get_user_subscriptions(
        user_id=message.from_user.id,
        window_size=settings.binance.alert_window_size,
    )
    text = "Подписка обновлена.\n\n" + _format_subscriptions(subscriptions)
    await message.answer(text=text, reply_markup=kb_user_alerts(subscriptions))
