from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    APP_NAME: str = "WARNSignal"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/warnsignal"
    CORS_ORIGINS: List[str] = ["http://localhost:3000"]
    SEC_USER_AGENT: str = "WARNSignal research@warnsignal.dev"
    SCRAPE_DELAY_SECONDS: float = 2.0
    MATCH_THRESHOLD: int = 85
    BACKTEST_DEFAULT_HOLD_DAYS: int = 30
    BACKTEST_MAX_POSITIONS: int = 20

    model_config = {
        "env_file": ".env",
        "case_sensitive": True,
        "extra": "ignore",
    }


settings = Settings()
