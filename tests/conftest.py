import os
import sys
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

# Add project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))


def _test_database_url():
    from app.config import get_settings

    raw_url = os.environ.get("TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("Set TEST_DATABASE_URL to run tests requiring MySQL")

    test_url = make_url(raw_url)
    if test_url.drivername != "mysql+aiomysql":
        pytest.fail("TEST_DATABASE_URL must use the MySQL aiomysql driver")
    if not test_url.database or "test" not in test_url.database.lower():
        pytest.fail("TEST_DATABASE_URL must point to a dedicated test database")

    app_url = make_url(get_settings().DATABASE_URL)
    if (
        test_url.host == app_url.host
        and test_url.port == app_url.port
        and test_url.database == app_url.database
    ):
        pytest.fail("TEST_DATABASE_URL must not point to the application database")

    return test_url.render_as_string(hide_password=False)


@pytest_asyncio.fixture
async def mysql_test_engine():
    from app.db.models import Base

    engine = create_async_engine(_test_database_url(), poolclass=NullPool, echo=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def mysql_test_session(mysql_test_engine):
    connection = await mysql_test_engine.connect()
    transaction = await connection.begin()
    factory = async_sessionmaker(
        bind=connection,
        class_=AsyncSession,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    async with factory() as session:
        yield session

    await transaction.rollback()
    await connection.close()


@pytest_asyncio.fixture
async def test_session(mysql_test_session):
    yield mysql_test_session


@pytest_asyncio.fixture
async def metric_session(mysql_test_session):
    yield mysql_test_session
