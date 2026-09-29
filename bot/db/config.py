import os

from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import BaseModel

BASE_DIR = Path(__file__).parent

load_dotenv()


class BotSettings(BaseModel):
    token: str = os.getenv('BOT_TOKEN')
    admin_list: list[int] = [int(admin) for admin in os.getenv('ADMIN_LIST_ID').split(',')]
    language: str = os.getenv('LANGUAGE')
    throttling: int = int(os.getenv('THROTTLING'))

    alert_diff: float = 6.0
    alert_window_size: str = '20m'

    proxy_url: str = os.getenv('PROXY_URL')


class BinanceSettings(BaseModel):
    api_key: str = os.getenv('BINANCE_API_KEY')
    api_secret: str = os.getenv('BINANCE_API_SECRET')

    ws_stream_url: str = os.getenv('BINANCE_WS_STREAM_URL', 'wss://fstream.binance.com/stream')
    ws_pool_size: int = int(os.getenv('BINANCE_WS_POOL_SIZE', '5'))
    ws_reconnect_delay_ms: int = int(os.getenv('BINANCE_WS_RECONNECT_DELAY_MS', '5000'))

    alert_window_size: str = os.getenv('BINANCE_ALERT_WINDOW_SIZE', '15m')
    alert_cache_ttl_seconds: int = int(os.getenv('BINANCE_ALERT_CACHE_TTL_SECONDS', str(15 * 60 + 60)))
    alert_subscriptions_refresh_seconds: int = int(os.getenv('BINANCE_ALERT_SUBSCRIPTIONS_REFRESH_SECONDS', '15'))
    alert_symbols_refresh_seconds: int = int(os.getenv('BINANCE_ALERT_SYMBOLS_REFRESH_SECONDS', '300'))
    alert_reconnect_delay_seconds: int = int(os.getenv('BINANCE_ALERT_RECONNECT_DELAY_SECONDS', '5'))
    alert_subscribe_delay_seconds: float = float(os.getenv('BINANCE_ALERT_SUBSCRIBE_DELAY_SECONDS', '0.0'))
    alert_subscribe_concurrency: int = int(os.getenv('BINANCE_ALERT_SUBSCRIBE_CONCURRENCY', '8'))
    alert_queue_maxsize: int = int(os.getenv('BINANCE_ALERT_QUEUE_MAXSIZE', '5000'))
    alert_workers_count: int = int(os.getenv('BINANCE_ALERT_WORKERS_COUNT', '4'))
    alert_send_concurrency: int = int(os.getenv('BINANCE_ALERT_SEND_CONCURRENCY', '8'))


class BbSettings(BaseModel):

    @property
    def db_url(self) -> str:
        return f"sqlite+aiosqlite:///{BASE_DIR}/database.db"

    echo: bool = False


class Settings(BaseSettings):
    bot: BotSettings = BotSettings()
    bd: BbSettings = BbSettings()
    binance: BinanceSettings = BinanceSettings()



settings = Settings()
