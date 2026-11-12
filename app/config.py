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

    # Green API WhatsApp notifications
    GREEN_API_ENABLED: bool = False
    GREEN_API_URL: str = "https://api.green-api.com"
    GREEN_API_INSTANCE_ID: str = ""
    GREEN_API_TOKEN: str = ""
    GREEN_API_CHAT_IDS: str = ""
    APP_PUBLIC_URL: str = "http://localhost:8060"
    CCTV_INCIDENT_CHECK_INTERVAL_SECONDS: int = Field(default=60, ge=10)
    CCTV_INCIDENT_REPEAT_INTERVAL_SECONDS: int = Field(default=21600, ge=3600)
    CCTV_INCIDENT_RECENT_ERROR_SECONDS: int = Field(default=300, ge=60)
    CCTV_INCIDENT_MANUAL_COOLDOWN_SECONDS: int = Field(default=300, ge=0)

    # Database (MySQL Laragon by default)
    DATABASE_URL: str = "mysql+aiomysql://root:@localhost:3306/ams_monitoring"

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
