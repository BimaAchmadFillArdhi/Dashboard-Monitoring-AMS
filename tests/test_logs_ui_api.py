import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.main import app
from app.db.models import Base, LogEntry
from app.db.session import get_db_session


@pytest_asyncio.fixture
async def override_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async def _get_test_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = _get_test_session

    # Seed test data
    async with session_factory() as session:
        session.add_all([
            LogEntry(
                ts=1792900000000,
                level="ERROR",
                logger="HLSMuxer",
                raw_message="cannot write video frame to muxer(cam1.m3u8). Error is Numerical result out of range",
                stream_id="cam1",
                app_name="atcs",
                friendly_title="Gangguan Muxer Video HLS",
                friendly_message="Video dari kamera cam1 tidak bisa disimpan.",
                severity_group="masalah",
                fingerprint="fp1",
            ),
            LogEntry(
                ts=1792900001000,
                level="WARN",
                logger="HLSMuxer",
                raw_message="cannot write video frame to muxer(cam1.m3u8). Error is Numerical result out of range",
                stream_id="cam1",
                app_name="atcs",
                friendly_title="Gangguan Muxer Video HLS",
                friendly_message="Video dari kamera cam1 tidak bisa disimpan.",
                severity_group="masalah",
                fingerprint="fp1",
            ),
            LogEntry(
                ts=1792900002000,
                level="INFO",
                logger="StreamFetcher",
                raw_message="Publishing started for streamId:cam2",
                stream_id="cam2",
                app_name="atcs",
                friendly_title="Stream Kamera Mulai Tayang",
                friendly_message="Kamera cam2 mulai mengirimkan video.",
                severity_group="info",
                fingerprint="fp2",
            ),
        ])
        await session.commit()

    yield

    app.dependency_overrides.clear()


def test_logs_page_render():
    client = TestClient(app)
    resp = client.get("/logs")
    assert resp.status_code == 200
    assert "Histori Log &amp; Arsip" in resp.text or "Histori Log & Arsip" in resp.text
    assert "Live Streaming Log (Real-time)" in resp.text
    assert "Ekspor CSV" in resp.text


def test_api_logs_query_all(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs")
    assert resp.status_code == 200
    data = resp.json()

    assert "entries" in data
    assert len(data["entries"]) == 3
    assert data["counts"]["TOTAL"] == 3
    assert data["counts"]["ERROR"] == 1
    assert data["counts"]["WARN"] == 1
    assert data["counts"]["INFO"] == 1


def test_api_logs_filter_by_level(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs?level=ERROR")
    assert resp.status_code == 200
    data = resp.json()

    assert len(data["entries"]) == 1
    assert data["entries"][0]["level"] == "ERROR"
    assert data["counts"]["TOTAL"] == 3
    assert data["counts"]["ERROR"] == 1


def test_api_logs_filter_by_stream(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs?stream_id=cam2")
    assert resp.status_code == 200
    data = resp.json()

    assert len(data["entries"]) == 1
    assert data["entries"][0]["stream_id"] == "cam2"
    assert data["entries"][0]["level"] == "INFO"


def test_api_logs_search_text(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs?q=Numerical")
    assert resp.status_code == 200
    data = resp.json()

    assert len(data["entries"]) == 2
    for e in data["entries"]:
        assert "Numerical" in e["raw_message"]


def test_api_logs_counts_remain_accurate(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs?level=WARN")
    assert resp.status_code == 200
    data = resp.json()

    assert len(data["entries"]) == 1
    assert data["counts"]["TOTAL"] == 3
    assert data["counts"]["ERROR"] == 1
    assert data["counts"]["WARN"] == 1
    assert data["counts"]["INFO"] == 1


def test_api_logs_dedupe(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs?dedupe=true")
    assert resp.status_code == 200
    data = resp.json()

    # cam1 has 2 logs with fingerprint fp1 -> grouped into 1 entry with repeat_count=2
    assert len(data["entries"]) == 2
    fp1_entry = next(e for e in data["entries"] if e["fingerprint"] == "fp1")
    assert fp1_entry["repeat_count"] == 2
    assert "cam1" in fp1_entry["stream_id"]


def test_api_logs_filters(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs/filters")
    assert resp.status_code == 200
    data = resp.json()
    assert "stream_ids" in data
    assert "app_names" in data
    assert "cam1" in data["stream_ids"]
    assert "atcs" in data["app_names"]


def test_api_logs_csv_export(override_db):
    client = TestClient(app)
    resp = client.get("/api/logs/export.csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "text/csv; charset=utf-8"
    assert "ID,Waktu (ISO),Level" in resp.text
    assert "cam1" in resp.text
    assert "cam2" in resp.text


def test_api_logs_filter_by_app_and_system(override_db):
    client = TestClient(app)

    # Filter by specific app 'atcs'
    resp_atcs = client.get("/api/logs?app_name=atcs")
    assert resp_atcs.status_code == 200
    data_atcs = resp_atcs.json()
    assert len(data_atcs["entries"]) == 3
    for e in data_atcs["entries"]:
        assert e["app_name"] == "atcs"

    # Filter by app that has no entries
    resp_empty = client.get("/api/logs?app_name=cctv-sekolah")
    assert resp_empty.status_code == 200
    assert len(resp_empty.json()["entries"]) == 0

    # Filter by SYSTEM (None app_name)
    resp_sys = client.get("/api/logs?app_name=SYSTEM")
    assert resp_sys.status_code == 200
    assert len(resp_sys.json()["entries"]) == 0
