import time
from collections import OrderedDict
from typing import Any


class LRUCache:
    """
    LRU-кэш с ограничением по вместимости (capacity) и индивидуальным ttl (в секундах)
    для каждого элемента.

    Методы:
    - get(key) -> Optional[value]
    - set(key, value, ttl: Optional[float])  # ttl в секундах, None = без истечения
    - remove(key)
    - clear()
    - __contains__, __len__
    """

    def __init__(self,
                 capacity: int = 128
                 ):
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.capacity = capacity
        # OrderedDict: ключ -> (value, expire_at)
        # порядок: от старого (left) к новому (right)
        self._cache: OrderedDict[Any, tuple[Any, float | None]] = OrderedDict()

    def _now(self) -> float:
        return time.monotonic()

    def _is_expired(self,
                    expire_at: float | None
                    ) -> bool:
        return expire_at is not None and self._now() >= expire_at

    def _purge_expired(self) -> None:
        """Удаляет все просроченные элементы.

        Вызывается только при подсчёте длины (не на каждый set), чтобы не сканировать
        весь кэш на каждую свечу.
        """
        now = self._now()
        keys_to_remove = [
            k for k, (_v, expire_at) in self._cache.items()
            if expire_at is not None and now >= expire_at
        ]
        for k in keys_to_remove:
            self._cache.pop(k, None)

    def get(self, key: Any) -> Any:
        """
        Возвращает значение или None, если нет/истёк.
        При успешном получении помечает элемент как недавно использованный.
        """
        item = self._cache.get(key)
        if item is None:
            return None
        value, expire_at = item
        if self._is_expired(expire_at):
            # удаляем и возвращаем None
            self._cache.pop(key, None)
            return None
        # переместить в конец как недавно использованный
        self._cache.pop(key, None)
        self._cache[key] = (value, expire_at)
        return value

    def set(self,
            key: Any,
            value: Any,
            ttl: float | None = None
            ) -> None:
        """
        Устанавливает ключ со значением и ttl (в секундах). ttl=None — бессрочно.
        Если ключ уже существует — обновляет значение и ttl, помечает как недавно использованный.
        Если после вставки превышена capacity — удаляет LRU элементы (с учётом просроченных).
        """
        expire_at = None if ttl is None else self._now() + float(ttl)
        # Если ключ уже в кеше — удалить старую запись чтобы затем добавить в конец
        if key in self._cache:
            self._cache.pop(key)
        self._cache[key] = (value, expire_at)

        # Удаляем элементы, пока превышаем capacity. Просроченные вытесняем в первую очередь.
        while len(self._cache) > self.capacity:
            first_key, (_first_value, first_expire_at) = next(iter(self._cache.items()))
            if self._is_expired(first_expire_at):
                self._cache.pop(first_key, None)
            else:
                self._cache.popitem(last=False)  # удаляет LRU (слева)

    def remove(self,
               key: Any
               ) -> None:
        """Удалить ключ, если есть."""
        self._cache.pop(key, None)

    def clear(self) -> None:
        """Полная очистка кеша."""
        self._cache.clear()

    def __contains__(self,
                     key: Any
                     ) -> bool:
        item = self._cache.get(key)
        if item is None:
            return False
        _, expire_at = item
        if self._is_expired(expire_at):
            self._cache.pop(key, None)
            return False
        return True

    def __len__(self) -> int:
        # Очистим просроченные перед подсчётом
        self._purge_expired()
        return len(self._cache)

    def items(self):
        """Итератор (key, value) для текущих непрошедших элементов (LRU порядок)."""
        # Возвращаем копию значений без expire_at
        now = self._now()
        keys = list(self._cache.keys())
        for k in keys:
            v, expire_at = self._cache.get(k, (None, None))
            if expire_at is not None and now >= expire_at:
                self._cache.pop(k, None)
                continue
            yield k, v


cache = LRUCache()