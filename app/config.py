from functools import lru_cache
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # AMS Settings
    AMS_BASE_URL: str = "http://103.255.15.138:5080"
    AMS_USER: str = "wiko@gmail.com"
    AMS_PASSWORD: str = "wiko2026"
    AMS_TIMEOUT_SECONDS: float = 5.0
    AMS_MAX_RETRIES: int = 3

    # Background Poller and Log Ingestor
    POLL_INTERVAL_SECONDS: int = 5
    LOG_RETENTION_DAYS: int = 30
    LOG_RETENTION_LIMIT: int = Field(default=100, ge=50, le=100, multiple_of=50)
    STREAM_METRICS_INTERVAL_SECONDS: int = 30
    STREAM_METRICS_HISTORY_INTERVAL_SECONDS: int = 30
    METRIC_RETENTION_DAYS: int = Field(default=7, ge=1)

    # Database (MySQL only)
    DATABASE_URL: str

    # App Settings
    APP_NAME: str = "CCTV Monitoring Dashboard"
    APP_ENV: str = "development"
    DEBUG: bool = True
    SECRET_KEY: str = "cctv-monitoring-super-secret-key-2026"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    @field_validator("DATABASE_URL")
    @classmethod
    def require_mysql_database(cls, value: str) -> str:
        if not value.startswith("mysql+aiomysql://"):
            raise ValueError("DATABASE_URL must use the MySQL aiomysql driver")
        return value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
