from aiogram.utils.keyboard import InlineKeyboardBuilder


def kb_user_settings():
    menu = InlineKeyboardBuilder()
    menu.button(text="⚙️ Настройки", callback_data="user_settings")
    menu.adjust(1)
    return menu.as_markup()


def kb_user_alerts(subscriptions: list[dict]):
    menu = InlineKeyboardBuilder()
    for sub in subscriptions:
        sub_id = sub.get("id")
        diff = sub.get("diff_percent")
        window_size = sub.get("window_size")
        is_active = bool(sub.get("active", True))
        active_text = "🟢 Активна" if is_active else "⚪️ Неактивна"
        menu.button(
            text=f"Изменить {window_size}: {diff}%",
            callback_data=f"edit_alert:{sub_id}",
        )
        menu.button(
            text=f"{active_text}",
            callback_data=f"toggle_alert_active:{sub_id}",
        )

    if not subscriptions:
        menu.button(text="➕ Добавить подписку", callback_data="add_alert_subscription")

    menu.adjust(1)
    return menu.as_markup()
