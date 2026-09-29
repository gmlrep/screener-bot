from aiogram import F, Router
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest
from fluent.runtime import FluentLocalization

from bot.db.services.kline_alert_subscription import KlineAlertSubscriptionService
from bot.db.config import settings
from bot.db.services.user import UserService
from bot.keyboards.keyboards import kb_user_settings, kb_user_alerts

user_router = Router()


class UserSettingsState(StatesGroup):
    waiting_new_diff_percent = State()


# Обработка команды start
@user_router.message(Command("start"))
async def start_handler(message: Message):
    service = UserService()
    await service.insert_user(user_id=message.from_user.id,
                      username=message.from_user.username,
                      fullname=message.from_user.full_name)
    await message.answer(text='Привет, я могу оповещать об изменениях цены токенов на фьючерсной торговле.',
                         reply_markup=kb_user_settings())


@user_router.callback_query(F.data == "user_settings")
async def user_settings_handler(callback: CallbackQuery):
    service = KlineAlertSubscriptionService()
    subscriptions = await service.get_user_subscriptions(
        user_id=callback.from_user.id,
        window_size=settings.binance.alert_window_size,
    )

    if not subscriptions:
        text = (
            f"У вас нет подписок на {settings.binance.alert_window_size}.\n"
            f"Нажмите «Добавить подписку»."
        )
    else:
        rows = []
        for sub in subscriptions:
            active_text = "активна" if sub.get("active", True) else "неактивна"
            rows.append(f"• {sub.get('window_size')}: {sub.get('diff_percent')}% ({active_text})")
        text = "Ваши подписки:\n" + "\n".join(rows)

    try:
        await callback.message.edit_text(
            text=text,
            reply_markup=kb_user_alerts(subscriptions),
        )
    except TelegramBadRequest:
        await callback.message.answer(
            text=text,
            reply_markup=kb_user_alerts(subscriptions),
        )
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
    rows = []
    for sub in subscriptions:
        active_text = "активна" if sub.get("active", True) else "неактивна"
        rows.append(f"• {sub.get('window_size')}: {sub.get('diff_percent')}% ({active_text})")
    text = "Ваши подписки:\n" + "\n".join(rows) if rows else f"У вас нет подписок на {settings.binance.alert_window_size}."

    try:
        await callback.message.edit_text(
            text=text,
            reply_markup=kb_user_alerts(subscriptions),
        )
    except TelegramBadRequest:
        await callback.message.answer(text=text, reply_markup=kb_user_alerts(subscriptions))
    await callback.answer()


@user_router.message(UserSettingsState.waiting_new_diff_percent)
async def apply_new_diff_percent_handler(message: Message, state: FSMContext):
    if not message.text:
        return

    try:
        new_diff = float(message.text.strip().replace(",", "."))
    except ValueError:
        await message.answer("Нужно число. Пример: 4.0")
        return

    if new_diff <= 0:
        await message.answer("Процент должен быть > 0")
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
    rows = []
    for sub in subscriptions:
        active_text = "активна" if sub.get("active", True) else "неактивна"
        rows.append(f"• {sub.get('window_size')}: {sub.get('diff_percent')}% ({active_text})")
    text = "Подписка обновлена.\n\nВаши подписки:\n" + "\n".join(rows)
    await message.answer(text=text, reply_markup=kb_user_alerts(subscriptions))


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

    try:
        diff_percent = float(parts[1].replace(",", "."))
    except ValueError:
        await message.answer("diff_percent должен быть числом. Пример: /alert 4.0")
        return

    if diff_percent <= 0:
        await message.answer("diff_percent должен быть > 0")
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
