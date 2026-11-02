import asyncio

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.clients.models import BroadcastStream, SystemResources
from app.db.models import AmsStreamCurrent, Base
from app.services.application_service import ApplicationService
from app.services.dashboard_service import DashboardService, DashboardSnapshot, RingBufferHistory
from app.services.sync_service import SyncService


@pytest_asyncio.fixture
async def test_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )
    async with session_factory() as session:
        yield session

    await engine.dispose()


async def seed_streams(session, app_name, count):
    session.add_all(
        [
            AmsStreamCurrent(
                app_name=app_name,
                stream_id=f"{app_name}-camera-{index:02}",
                name=f"Camera {index:02}",
                status="broadcasting",
                bitrate=1_000_000 + index,
                speed=1.0,
            )
            for index in range(count)
        ]
    )
    await session.commit()


@pytest.mark.asyncio
async def test_failed_ams_request_uses_app_scoped_mysql_snapshot(
    test_session, monkeypatch
):
    import app.services.application_service as application_service_module

    await seed_streams(test_session, "cctv-bantul", 24)
    await seed_streams(test_session, "cctv-sleman", 7)

    class UnavailableAMS:
        async def get_broadcasts_count(self, *args, **kwargs):
            raise RuntimeError("AMS timeout")

        async def get_all_broadcasts(self, *args, **kwargs):
            raise RuntimeError("AMS timeout")

        async def get_applications_info(self, **kwargs):
            raise RuntimeError("AMS timeout")

    monkeypatch.setattr(application_service_module, "ams_client", UnavailableAMS())
    service = ApplicationService()

    overview = await service.get_applications_overview(test_session)
    bantul_summary = next(item for item in overview if item.name == "cctv-bantul")
    streams = await service.get_streams_for_app(
        "cctv-bantul", test_session, offset=0, size=50
    )
    sleman_streams = await service.get_streams_for_app(
        "cctv-sleman", test_session, offset=0, size=50
    )

    assert bantul_summary.live_count == 24
    assert len(streams) == 24
    assert len(sleman_streams) == 7
    assert {item.stream_id for item in streams}.isdisjoint(
        {item.stream_id for item in sleman_streams}
    )


@pytest.mark.asyncio
async def test_empty_ams_page_with_positive_count_uses_cached_streams(
    test_session, monkeypatch
):
    import app.services.application_service as application_service_module

    await seed_streams(test_session, "cctv-bantul", 24)

    class EmptyAMS:
        async def get_broadcasts_count(self, *args, **kwargs):
            return 24

        async def get_all_broadcasts(self, *args, **kwargs):
            return []

    monkeypatch.setattr(application_service_module, "ams_client", EmptyAMS())
    streams = await ApplicationService().get_streams_for_app(
        "cctv-bantul", test_session, offset=0, size=50
    )

    assert len(streams) == 24
    assert "cctv-bantul-camera-00" in {item.stream_id for item in streams}


@pytest.mark.asyncio
async def test_applications_stream_list_matches_active_broadcast_count(
    test_session, monkeypatch
):
    import app.services.application_service as application_service_module

    class MixedStatusAMS:
        async def get_broadcasts_count(self, *args, **kwargs):
            return 3

        async def get_all_broadcasts(self, *args, **kwargs):
            return [
                BroadcastStream(streamId="active-1", status="broadcasting"),
                BroadcastStream(streamId="stopped", status="finished"),
                BroadcastStream(streamId="active-2", status="broadcasting"),
            ]

    monkeypatch.setattr(application_service_module, "ams_client", MixedStatusAMS())
    streams = await ApplicationService().get_streams_for_app(
        "cctv-bantul", test_session, offset=0, size=50
    )

    assert [item.stream_id for item in streams] == ["active-1", "active-2"]


@pytest.mark.asyncio
async def test_dashboard_keeps_last_application_counts_when_ams_is_unavailable():
    service = DashboardService()
    cached = DashboardSnapshot(
        ams_connected=True,
        total_live_streams=24,
        applications=[
            {
                "name": "cctv-bantul",
                "live_count": 24,
                "vod_count": 0,
                "storage_mb": 0.0,
                "status": "Aktif",
            }
        ],
        history=RingBufferHistory(
            timestamps=[],
            cpu=[],
            memory_system=[],
            jvm_heap=[],
            live_streams=[],
            viewers=[],
            db_query_ms=[],
        ),
    )
    service._last_snapshot = cached

    class EmptyApplicationsAMS:
        async def get_system_resources(self):
            return SystemResources()

        async def get_applications_info(self, **kwargs):
            return []

    service.ams_client = EmptyApplicationsAMS()
    empty_snapshot = await service.collect_snapshot()
    assert empty_snapshot.total_live_streams == 24
    assert empty_snapshot.applications == cached.applications

    class UnavailableAMS:
        async def get_system_resources(self):
            return None

    service.ams_client = UnavailableAMS()
    snapshot = await service.collect_snapshot()

    assert snapshot.ams_connected is False
    assert snapshot.total_live_streams == 24
    assert snapshot.applications == cached.applications


@pytest.mark.asyncio
async def test_sync_rejects_incomplete_ams_snapshot_without_overwriting_cache(
    test_session,
):
    service = SyncService()
    existing = AmsStreamCurrent(
        app_name="cctv-bantul",
        stream_id="camera-00",
        name="Last valid name",
        status="broadcasting",
    )
    test_session.add(existing)
    await test_session.commit()

    class IncompleteAMS:
        async def get_applications(self, **kwargs):
            return ["cctv-bantul"]

        async def get_broadcasts_count(self, *args, **kwargs):
            return 2

        async def get_all_broadcasts(self, *args, **kwargs):
            return [BroadcastStream(streamId="camera-00", name="Partial response")]

    service.ams_client = IncompleteAMS()
    result = await service.sync_all_applications_streams(test_session)

    await test_session.refresh(existing)
    assert result["status"] == "WARNING"
    assert result["apps_failed"] == ["cctv-bantul"]
    assert existing.name == "Last valid name"


@pytest.mark.asyncio
async def test_concurrent_manual_and_scheduled_syncs_are_serialized():
    service = SyncService()
    active = 0
    max_active = 0

    async def sync_once(session):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return {"status": "SUCCESS"}

    service._sync_all_applications_streams = sync_once
    await asyncio.gather(
        service.sync_all_applications_streams(None),
        service.sync_all_applications_streams(None),
    )

    assert max_active == 1
