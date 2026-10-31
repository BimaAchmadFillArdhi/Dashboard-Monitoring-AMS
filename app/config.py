import os
from functools import lru_cache
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
    STREAM_METRICS_INTERVAL_SECONDS: int = 5
    METRIC_RETENTION_DAYS: int = 30

    # Database (MySQL Laragon by default)
    DATABASE_URL: str = "mysql+aiomysql://root:@localhost:3306/ams_monitoring"
    SYNC_DATABASE_URL: str = "mysql+pymysql://root:@localhost:3306/ams_monitoring"

    # App Settings
    APP_NAME: str = "CCTV Monitoring Dashboard"
    APP_ENV: str = "development"
    DEBUG: bool = True
    SECRET_KEY: str = "cctv-monitoring-super-secret-key-2026"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
