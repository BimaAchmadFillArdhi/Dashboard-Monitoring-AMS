import os
import sys
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool

from app.config import get_settings

settings = get_settings()

# Base declarative class
Base = declarative_base()

is_sqlite_async = settings.DATABASE_URL.startswith("sqlite")
is_sqlite_sync = settings.SYNC_DATABASE_URL.startswith("sqlite")
is_testing = "pytest" in sys.modules or os.environ.get("APP_ENV") == "testing"

# Async Engine
if is_sqlite_async:
    async_engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(async_engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=-64000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()
else:
    # MySQL / MariaDB async engine
    if is_testing:
        async_engine = create_async_engine(
            settings.DATABASE_URL,
            echo=False,
            poolclass=NullPool,
        )
    else:
        async_engine = create_async_engine(
            settings.DATABASE_URL,
            echo=False,
            pool_size=15,
            max_overflow=25,
            pool_recycle=3600,
            pool_pre_ping=True,
        )

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
    """Create all tables and initialize schema if needed."""
    from app.db import models  # noqa: F401
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# Sync Engine for bulk operations / background utilities
if is_sqlite_sync:
    sync_engine = create_engine(
        settings.SYNC_DATABASE_URL,
        echo=False,
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(sync_engine, "connect")
    def set_sync_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=-64000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()
else:
    sync_engine = create_engine(
        settings.SYNC_DATABASE_URL,
        echo=False,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        pool_pre_ping=True,
    )

SyncSessionLocal = sessionmaker(bind=sync_engine, expire_on_commit=False)
