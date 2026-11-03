import sys
from typing import AsyncGenerator

from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool

from app.config import get_settings

settings = get_settings()

if not settings.DATABASE_URL.startswith("mysql+aiomysql://"):
    raise RuntimeError("The application requires a MySQL aiomysql database URL")

Base = declarative_base()
is_testing = "pytest" in sys.modules

engine_options = {"echo": False}
if is_testing:
    engine_options["poolclass"] = NullPool
else:
    engine_options.update(
        pool_size=5,
        max_overflow=5,
        pool_recycle=3600,
        pool_pre_ping=True,
    )

async_engine = create_async_engine(settings.DATABASE_URL, **engine_options)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Create all tables and initialize schema if needed on MySQL."""
    from app.db import models  # noqa: F401

    def ensure_retention_indexes(sync_conn):
        if sync_conn.dialect.name != "mysql":
            raise RuntimeError("The application schema must be initialized on MySQL")

        for table, index_name in (
            (models.LogEntry.__table__, "ix_log_entries_app_stream_ts"),
            (models.AmsSyncLog.__table__, "ix_ams_sync_logs_app_created_at"),
        ):
            existing_indexes = {
                index["name"] for index in inspect(sync_conn).get_indexes(table.name)
            }
            if index_name not in existing_indexes:
                index = next(
                    item for item in table.indexes if item.name == index_name
                )
                index.create(bind=sync_conn)

    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(ensure_retention_indexes)
