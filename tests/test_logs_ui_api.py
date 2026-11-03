import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy import delete, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.main import app
from app.db.models import LogEntry
from app.db.session import get_db_session
from app.services.log_service import get_log_service


@pytest_asyncio.fixture
async def override_db(mysql_test_engine):
    log_service = get_log_service()
    log_service.invalidate_stats_cache()
    session_factory = async_sessionmaker(
        bind=mysql_test_engine, class_=AsyncSession, expire_on_commit=False
    )

    async def _get_test_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = _get_test_session

    # Seed test data
    seeded_entries = [
        LogEntry(
                ts=1792900000000,
                level="ERROR",
                logger="HLSMuxer",
                raw_message="cannot write video frame to muxer(cam1.m3u8). Error is Numerical result out of range",
                stream_id="cam1",
                app_name="atcs",
                friendly_title="Gangguan Muxer Video HLS",
                friendly_message="Video dari kamera cam1 tidak bisa disimpan.",
                category="muxing",
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
                category="muxing",
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
                category="stream",
                severity_group="info",
                fingerprint="fp2",
        ),
    ]
    async with session_factory() as session:
        session.add_all(seeded_entries)
        await session.flush()
        seeded_ids = [entry.id for entry in seeded_entries]
        await session.commit()

    yield

    app.dependency_overrides.clear()
    async with session_factory() as session:
        await session.execute(delete(LogEntry).where(LogEntry.id.in_(seeded_ids)))
        await session.commit()
    log_service.invalidate_stats_cache()


def test_logs_page_render():
    client = TestClient(app)
    resp = client.get("/logs")
    assert resp.status_code == 200
    assert "Histori Log &amp; Arsip" in resp.text or "Histori Log & Arsip" in resp.text
    assert "Live Streaming Log" in resp.text
    assert "200 baris terbaru" in resp.text
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
    assert data["category_counts"]["muxing"] == 2
    assert data["category_counts"]["stream"] == 1
    assert "raw_message" not in data["entries"][0]


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
        detail = client.get(f"/api/logs/{e['id']}")
        assert detail.status_code == 200
        assert "Numerical" in detail.json()["raw_message"]


def test_api_log_detail_not_found():
    response = TestClient(app).get("/api/logs/999999999")
    assert response.status_code == 404


def test_api_logs_incremental_updates(override_db):
    client = TestClient(app)
    page = client.get("/api/logs?limit=50").json()
    first_id = min(entry["id"] for entry in page["entries"])
    response = client.get(f"/api/logs/updates?after_id={first_id}")
    assert response.status_code == 200
    data = response.json()
    assert [entry["id"] for entry in data["entries"]] == sorted(
        entry["id"] for entry in data["entries"]
    )
    assert len(data["entries"]) == 2
    assert data["counts"]["TOTAL"] == 3
    assert data["next_after_id"] == data["latest_id"]

    deduped = client.get(
        f"/api/logs/updates?after_id={first_id}&dedupe=true"
    ).json()
    assert {entry["fingerprint"]: entry["repeat_count"] for entry in deduped["entries"]} == {
        "fp1": 2,
        "fp2": 1,
    }


def test_api_logs_dedupe_keyset_pagination(override_db):
    client = TestClient(app)
    first_page = client.get("/api/logs?dedupe=true&limit=1").json()
    assert first_page["has_more"]
    second_page = client.get(
        f"/api/logs?dedupe=true&limit=1&cursor={first_page['next_cursor']}"
    ).json()
    assert len(second_page["entries"]) == 1
    assert second_page["entries"][0]["fingerprint"] != first_page["entries"][0]["fingerprint"]


def test_api_logs_caches_grouped_statistics(override_db, mysql_test_engine):
    statements = []

    def record_statement(conn, cursor, statement, params, context, executemany):
        if "GROUP BY log_entries.level, log_entries.category" in statement:
            statements.append(statement)

    event.listen(mysql_test_engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        client = TestClient(app)
        assert client.get("/api/logs").status_code == 200
        assert client.get("/api/logs").status_code == 200
    finally:
        event.remove(mysql_test_engine.sync_engine, "before_cursor_execute", record_statement)

    assert len(statements) == 1


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
