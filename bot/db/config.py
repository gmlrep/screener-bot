import os
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse, urlunparse

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent  # bot/db
PROJECT_ROOT = BASE_DIR.parents[1]  # repo root

# Сначала ищем .env в корне проекта, затем — стандартный поиск вверх по дереву.
load_dotenv(PROJECT_ROOT / ".env")
load_dotenv()


def _parse_int_list(raw: str | None) -> list[int]:
    if not raw:
        return []

    result: list[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        result.append(int(chunk))
    return result


# aiohttp-socks (используется aiogram) понимает только http/socks4/socks5.
_SUPPORTED_PROXY_SCHEMES = {"http", "socks4", "socks5"}
_TG_PROXY_PREFIXES = ("tg://proxy", "https://t.me/proxy", "http://t.me/proxy", "t.me/proxy")
_TG_SOCKS_PREFIXES = ("tg://socks", "https://t.me/socks", "http://t.me/socks", "t.me/socks")


def normalize_proxy_url(raw: str | None) -> str | None:
    """Приводит PROXY_URL к формату, который понимает aiohttp-socks.

    - t.me/proxy (MTProto) -> понятная ошибка (для Bot API не подходит);
    - t.me/socks -> socks5://host:port;
    - https:// -> http:// (провайдеры обычно так называют обычный CONNECT-прокси);
    - socks5h:// -> socks5://.
    """
    if not raw or not raw.strip():
        return None

    value = raw.strip()
    lowered = value.lower()

    if lowered.startswith(_TG_PROXY_PREFIXES):
        raise ValueError(
            "PROXY_URL задан ссылкой MTProto-прокси Telegram (t.me/proxy). "
            "Bot API работает по HTTPS, а MTProto-прокси для HTTP-клиента не подходит. "
            "Укажите обычный HTTP/SOCKS5-прокси, например: "
            "socks5://user:pass@host:1080 или http://user:pass@host:8080"
        )

    if lowered.startswith(_TG_SOCKS_PREFIXES):
        parsed = urlparse(value if "://" in value else f"https://{value}")
        query = parse_qs(parsed.query)
        server = (query.get("server") or [""])[0]
        port = (query.get("port") or ["1080"])[0]
        if not server:
            raise ValueError("PROXY_URL: в ссылке t.me/socks не указан параметр server")
        user = (query.get("user") or [""])[0]
        password = (query.get("pass") or [""])[0]
        auth = f"{quote(user)}:{quote(password)}@" if user else ""
        return f"socks5://{auth}{server}:{port}"

    if "://" not in value:
        value = f"http://{value}"

    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    if scheme == "https":
        scheme = "http"
    elif scheme == "socks5h":
        scheme = "socks5"

    if scheme not in _SUPPORTED_PROXY_SCHEMES:
        supported = ", ".join(sorted(_SUPPORTED_PROXY_SCHEMES))
        raise ValueError(
            f"PROXY_URL: схема '{parsed.scheme}' не поддерживается. Доступные схемы: {supported}"
        )
    if not parsed.hostname:
        raise ValueError("PROXY_URL: не удалось определить host прокси")

    return urlunparse(parsed._replace(scheme=scheme))


class BotSettings(BaseModel):
    model_config = ConfigDict(validate_default=True)

    token: str = os.getenv("BOT_TOKEN", "")
    admin_list: list[int] = _parse_int_list(os.getenv("ADMIN_LIST_ID"))
    language: str = os.getenv("LANGUAGE") or "ru"
    throttling: int = int(os.getenv("THROTTLING") or 1)

    alert_diff: float = 6.0
    alert_window_size: str = "20m"

    proxy_url: str | None = os.getenv("PROXY_URL") or None

    @field_validator("token")
    @classmethod
    def _validate_token(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("BOT_TOKEN is not set (check your .env)")
        return value

    @field_validator("proxy_url")
    @classmethod
    def _validate_proxy_url(cls, value: str | None) -> str | None:
        return normalize_proxy_url(value)


class BinanceSettings(BaseModel):
    api_key: str | None = os.getenv("BINANCE_API_KEY") or None
    api_secret: str | None = os.getenv("BINANCE_API_SECRET") or None

    ws_stream_url: str = os.getenv("BINANCE_WS_STREAM_URL", "wss://fstream.binance.com/stream")
    ws_pool_size: int = int(os.getenv("BINANCE_WS_POOL_SIZE", "5"))
    ws_reconnect_delay_ms: int = int(os.getenv("BINANCE_WS_RECONNECT_DELAY_MS", "5000"))

    alert_window_size: str = os.getenv("BINANCE_ALERT_WINDOW_SIZE", "15m")
    alert_cache_ttl_seconds: int = int(os.getenv("BINANCE_ALERT_CACHE_TTL_SECONDS", str(15 * 60 + 60)))
    alert_subscriptions_refresh_seconds: int = int(os.getenv("BINANCE_ALERT_SUBSCRIPTIONS_REFRESH_SECONDS", "15"))
    alert_symbols_refresh_seconds: int = int(os.getenv("BINANCE_ALERT_SYMBOLS_REFRESH_SECONDS", "300"))
    alert_reconnect_delay_seconds: int = int(os.getenv("BINANCE_ALERT_RECONNECT_DELAY_SECONDS", "5"))
    # SDK сам не реконнектит на CLOSE/ERROR, поэтому health-check не должен ждать окно свечи.
    alert_ws_idle_timeout_seconds: int = int(os.getenv("BINANCE_ALERT_WS_IDLE_TIMEOUT_SECONDS", "120"))
    alert_subscribe_delay_seconds: float = float(os.getenv("BINANCE_ALERT_SUBSCRIBE_DELAY_SECONDS", "0.0"))
    alert_subscribe_concurrency: int = int(os.getenv("BINANCE_ALERT_SUBSCRIBE_CONCURRENCY", "8"))
    # Сколько стримов кладём в одну SUBSCRIBE-рамку (Binance допускает до 1024 на соединение).
    alert_subscribe_chunk_size: int = int(os.getenv("BINANCE_ALERT_SUBSCRIBE_CHUNK_SIZE", "100"))
    alert_queue_maxsize: int = int(os.getenv("BINANCE_ALERT_QUEUE_MAXSIZE", "5000"))
    alert_workers_count: int = int(os.getenv("BINANCE_ALERT_WORKERS_COUNT", "4"))
    alert_send_concurrency: int = int(os.getenv("BINANCE_ALERT_SEND_CONCURRENCY", "8"))


class DbSettings(BaseModel):
    echo: bool = False
    path: Path = Path(os.getenv("DB_PATH") or (BASE_DIR / "database.db")).expanduser()

    @property
    def db_url(self) -> str:
        return f"sqlite+aiosqlite:///{self.path}"


class Settings(BaseSettings):
    bot: BotSettings = BotSettings()
    db: DbSettings = DbSettings()
    binance: BinanceSettings = BinanceSettings()


settings = Settings()
