import json
import time

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api import applications_api
from app.db.models import Base, StreamMetricHistory
from app.services.application_service import ApplicationService


@pytest_asyncio.fixture
async def metric_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_history_returns_recent_metric_samples_in_ascending_time(metric_session):
    service = ApplicationService()
    now_ms = int(time.time() * 1000)
    metric_session.add_all(
        [
            StreamMetricHistory(
                ts=now_ms - offset * 10_000,
                app_name="atcs",
                stream_id="camera-1",
                status="broadcasting",
                bitrate=offset * 1000,
                bitrate_mbps=float(offset),
                speed=1.0,
                total_viewers=offset,
                hls_viewers=offset,
                webrtc_viewers=0,
            )
            for offset in range(700)
        ]
    )
    await metric_session.commit()

    one_hour = await service.get_stream_metric_history(
        metric_session, "atcs", "camera-1", hours=1
    )
    six_hours = await service.get_stream_metric_history(
        metric_session, "atcs", "camera-1", hours=6
    )

    assert 0 < one_hour["points_count"] < 700
    assert six_hours["points_count"] == 600
    assert six_hours["timestamps"][0] < six_hours["timestamps"][-1]
    assert six_hours["bitrate_mbps"][0] == 599.0
    assert six_hours["bitrate_mbps"][-1] == 0.0
    assert six_hours["viewers"][-1] == 0


@pytest.mark.asyncio
async def test_save_stream_metric_persists_broadcast_viewer_fields(metric_session):
    service = ApplicationService()
    saved = await service.save_stream_metric(
        metric_session,
        "atcs",
        "camera-1",
        {
            "status": "broadcasting",
            "bitrate": 2_500_000,
            "bitrate_mbps": 2.5,
            "speed": 0.98,
            "total_viewers": 5,
            "hls_viewers": 3,
            "webrtc_viewers": 2,
            "dash_viewers": 0,
        },
        force=True,
    )

    assert saved is True
    row = (
        await metric_session.execute(
            select(StreamMetricHistory).where(
                StreamMetricHistory.app_name == "atcs",
                StreamMetricHistory.stream_id == "camera-1",
            )
        )
    ).scalar_one()
    assert row.bitrate == 2_500_000
    assert row.bitrate_mbps == 2.5
    assert row.speed == 0.98
    assert row.hls_viewers == 3
    assert row.webrtc_viewers == 2
    assert row.total_viewers == 5


@pytest.mark.asyncio
async def test_live_sse_includes_requested_history(monkeypatch):
    calls = {}
    detail = {
        "bitrate": 1_000_000,
        "bitrate_mbps": 1.0,
        "bitrate_kbps": 1000.0,
        "speed": 1.0,
        "total_viewers": 2,
        "hls_viewers": 2,
        "webrtc_viewers": 0,
        "dash_viewers": 0,
        "status": "broadcasting",
        "health_status": "Sehat",
        "health_tone": "success",
    }
    history = {
        "app_name": "atcs",
        "stream_id": "camera-1",
        "timestamps": ["14:00:00", "14:00:02"],
        "bitrate_mbps": [0.9, 1.0],
        "speed": [0.95, 1.0],
        "speed_filtered": [0.95, 1.0],
        "viewers": [1, 2],
        "points_count": 2,
    }

    async def get_detail(*args):
        return detail

    async def save_metric(*args, **kwargs):
        calls["force"] = kwargs.get("force")
        return True

    async def get_history(*args, **kwargs):
        calls["hours"] = kwargs.get("hours")
        return history

    monkeypatch.setattr(applications_api.app_service, "get_stream_detail", get_detail)
    monkeypatch.setattr(applications_api.app_service, "save_stream_metric", save_metric)
    monkeypatch.setattr(
        applications_api.app_service, "get_stream_metric_history", get_history
    )

    response = await applications_api.sse_stream_drawer(
        "atcs", "camera-1", hours=6, session=None
    )
    iterator = response.body_iterator
    try:
        event = await iterator.__anext__()
    finally:
        await iterator.aclose()

    event_text = event.decode() if isinstance(event, bytes) else event
    payload = json.loads(event_text.removeprefix("data: ").strip())
    assert payload["history"] == history
    assert calls == {"force": True, "hours": 6}
