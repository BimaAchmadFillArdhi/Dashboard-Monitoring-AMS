import time
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.db.models import Base, LogEntry
from app.clients.models import BroadcastStream
from app.services.health_service import HealthService


@pytest_asyncio.fixture
async def test_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_evaluate_health_offline(test_session):
    svc = HealthService()
    offline_stream = BroadcastStream(streamId="cam_offline", status="finished")
    report = await svc.evaluate_stream_health(offline_stream, "atcs", test_session)
    assert report.status == "Offline"
    assert report.tone == "neutral"


@pytest.mark.asyncio
async def test_evaluate_health_healthy(test_session):
    svc = HealthService()
    live_stream = BroadcastStream(streamId="cam_live", status="broadcasting", bitrate=1500)
    report = await svc.evaluate_stream_health(live_stream, "atcs", test_session)
    assert report.status == "Sehat"
    assert report.tone == "success"


@pytest.mark.asyncio
async def test_evaluate_health_problematic(test_session):
    svc = HealthService(warn_threshold_5m=5, error_threshold_5m=2)
    cam_id = "cam_troubled"
    live_stream = BroadcastStream(streamId=cam_id, status="broadcasting", bitrate=1200)

    # Insert 6 recent WARN entries
    now_ms = int(time.time() * 1000)
    entries = [
        LogEntry(
            ts=now_ms - (i * 1000),
            level="WARN",
            logger="HLSMuxer",
            raw_message=f"cannot write video frame to muxer({cam_id}.m3u8). Numerical result out of range",
            stream_id=cam_id,
            friendly_title="Gangguan Muxer Video HLS",
            friendly_message=f"Video dari kamera {cam_id} tidak bisa disimpan.",
            severity_group="masalah",
            fingerprint="fp1",
        )
        for i in range(6)
    ]
    test_session.add_all(entries)
    await test_session.commit()

    report = await svc.evaluate_stream_health(live_stream, "atcs", test_session)
    assert report.status == "Bermasalah"
    assert report.tone == "danger"
    assert report.warns_5m == 6
    assert "Video dari kamera" in report.problem_summary


@pytest.mark.asyncio
async def test_rank_problematic_streams(test_session):
    svc = HealthService(warn_threshold_5m=5)

    s1 = BroadcastStream(streamId="cam_ok", status="broadcasting", bitrate=1000)
    s2 = BroadcastStream(streamId="cam_warn", status="broadcasting", bitrate=1000)
    s3 = BroadcastStream(streamId="cam_error", status="broadcasting", bitrate=1000)

    now_ms = int(time.time() * 1000)

    # s2 has 6 warnings (Bermasalah)
    # s3 has 1 warning (Perhatian)
    logs = [
        LogEntry(
            ts=now_ms - (i * 1000),
            level="WARN",
            raw_message="err",
            stream_id="cam_warn",
            friendly_title="Warn Title",
            friendly_message="Warn Msg",
            severity_group="masalah",
            fingerprint="fp2",
        )
        for i in range(6)
    ]
    logs.append(
        LogEntry(
            ts=now_ms - 2000,
            level="WARN",
            raw_message="err",
            stream_id="cam_error",
            friendly_title="Minor Warn",
            friendly_message="Minor Msg",
            severity_group="perhatian",
            fingerprint="fp3",
        )
    )
    test_session.add_all(logs)
    await test_session.commit()

    ranked = await svc.get_problematic_streams([s1, s2, s3], "atcs", test_session)
    # cam_ok should NOT be in problematic list
    assert len(ranked) == 2
    # cam_warn (Bermasalah) should be first, cam_error (Perhatian) second
    assert ranked[0].stream_id == "cam_warn"
    assert ranked[0].status == "Bermasalah"
    assert ranked[1].stream_id == "cam_error"
    assert ranked[1].status == "Perhatian"
