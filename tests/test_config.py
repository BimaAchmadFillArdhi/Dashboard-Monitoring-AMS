import pytest
from pydantic import ValidationError

from app.config import Settings
from app.db.session import async_engine


@pytest.mark.parametrize(
    "database_url",
    ["postgresql://localhost/ams_monitoring", "file:///tmp/ams_monitoring"],
)
def test_settings_reject_non_mysql_database_urls(database_url):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, DATABASE_URL=database_url)


def test_application_engine_is_mysql_only():
    assert async_engine.dialect.name == "mysql"
