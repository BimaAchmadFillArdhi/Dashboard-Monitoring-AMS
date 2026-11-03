import asyncio

import pytest
from sqlalchemy import select

from app.clients.ams_client import AMSAPIError
from app.clients.models import BroadcastStream, SystemResources
from app.db.models import AmsStreamCurrent, AmsStreamStatusHistory, StreamMetricHistory
from app.services.application_service import ApplicationService
from app.services.dashboard_service import DashboardService, DashboardSnapshot, RingBufferHistory
from app.services.sync_service import SyncService


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
            raise AMSAPIError("AMS timeout")

        async def get_broadcasts_list(self, *args, **kwargs):
            raise AMSAPIError("AMS timeout")

        async def get_applications_info(self, **kwargs):
            raise AMSAPIError("AMS timeout")

    monkeypatch.setattr(application_service_module, "ams_client", UnavailableAMS())
    service = ApplicationService()

    overview = await service.get_applications_overview(test_session)
    bantul_summary = next(item for item in overview if item.name == "cctv-bantul")
    streams, stream_total, live_count, offline_count = await service.get_streams_for_app(
        "cctv-bantul", test_session, offset=0, size=50
    )
    sleman_streams, sleman_total, _, _ = await service.get_streams_for_app(
        "cctv-sleman", test_session, offset=0, size=50
    )

    assert bantul_summary.live_count == 24
    assert stream_total == 24
    assert live_count == 24
    assert offline_count == 0
    assert sleman_total == 7
    assert len(streams) == 24
    assert len(sleman_streams) == 7
    assert {item.stream_id for item in streams}.isdisjoint(
        {item.stream_id for item in sleman_streams}
    )


@pytest.mark.asyncio
async def test_stream_page_uses_mysql_snapshot_without_ams_requests(test_session, monkeypatch):
    await seed_streams(test_session, "cctv-bantul", 24)

    class UnusedAMS:
        async def get_broadcasts_count(self, *args, **kwargs):
            raise AssertionError("Stream pagination must not call AMS")

        async def get_broadcasts_list(self, *args, **kwargs):
            raise AssertionError("Stream pagination must not call AMS")

    import app.services.application_service as application_service_module

    monkeypatch.setattr(application_service_module, "ams_client", UnusedAMS())
    streams, total, live, offline = await ApplicationService().get_streams_for_app(
        "cctv-bantul", test_session, offset=0, size=50
    )

    assert total == 24
    assert live == 24
    assert offline == 0
    assert len(streams) == 24
    assert "cctv-bantul-camera-00" in {item.stream_id for item in streams}


@pytest.mark.asyncio
async def test_applications_stream_list_includes_live_and_offline_mysql_records(test_session):
    test_session.add_all([
        AmsStreamCurrent(app_name="cctv-bantul", stream_id="active-1", status="broadcasting"),
        AmsStreamCurrent(app_name="cctv-bantul", stream_id="stopped", status="finished"),
        AmsStreamCurrent(app_name="cctv-bantul", stream_id="active-2", status="broadcasting"),
    ])
    await test_session.commit()
    streams, total, live, offline = await ApplicationService().get_streams_for_app(
        "cctv-bantul", test_session, offset=0, size=50
    )

    assert total == 3
    assert live == 2
    assert offline == 1
    assert {item.stream_id for item in streams} == {"active-1", "stopped", "active-2"}
    assert next(item for item in streams if item.stream_id == "stopped").health_status == "Offline"


@pytest.mark.asyncio
async def test_applications_stream_page_returns_requested_mysql_offset(test_session):
    await seed_streams(test_session, "test-app", 150)
    streams, total, live, offline = await ApplicationService().get_streams_for_app(
        "test-app", test_session, offset=0, size=50
    )
    next_page, next_total, next_live, next_offline = await ApplicationService().get_streams_for_app(
        "test-app", test_session, offset=50, size=50
    )

    assert total == next_total == 150
    assert live == next_live == 150
    assert offline == next_offline == 0
    assert len(streams) == 50
    assert len(next_page) == 50
    assert {item.stream_id for item in streams}.isdisjoint(
        {item.stream_id for item in next_page}
    )
    matching_page, matching_total, matching_live, matching_offline = await ApplicationService().get_streams_for_app(
        "test-app", test_session, offset=25, size=25, search_q="camera-1"
    )
    assert matching_total == 60
    assert matching_live == 60
    assert matching_offline == 0
    assert len(matching_page) == 25


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

    class FailedApplicationsAMS:
        async def get_system_resources(self):
            return SystemResources()

        async def get_applications_info(self, **kwargs):
            raise AMSAPIError("applications-info HTTP 500")

    service.ams_client = FailedApplicationsAMS()
    service._snapshot_updated_at = 0.0
    failed_counts_snapshot = await service.collect_snapshot()
    assert failed_counts_snapshot.ams_connected is True
    assert failed_counts_snapshot.total_live_streams == 24
    assert failed_counts_snapshot.applications == cached.applications

    class UnavailableAMS:
        async def get_system_resources(self):
            return None

    service.ams_client = UnavailableAMS()
    service._snapshot_updated_at = 0.0
    snapshot = await service.collect_snapshot()

    assert snapshot.ams_connected is False
    assert snapshot.total_live_streams == 24
    assert snapshot.applications == cached.applications


@pytest.mark.asyncio
async def test_dashboard_snapshot_cache_shares_ams_polling(monkeypatch):
    service = DashboardService()
    calls = 0

    class AvailableAMS:
        async def get_system_resources(self):
            nonlocal calls
            calls += 1
            return SystemResources()

        async def get_applications_info(self, **kwargs):
            return []

    service.ams_client = AvailableAMS()

    first = await service.collect_snapshot()
    second = await service.collect_snapshot()

    assert first.ams_connected is True
    assert second == first
    assert calls == 1


@pytest.mark.asyncio
async def test_stream_history_sampling_is_throttled_between_full_syncs(test_session):
    service = SyncService()
    service.settings.STREAM_METRICS_HISTORY_INTERVAL_SECONDS = 3600

    class AMS:
        async def get_applications(self, **kwargs):
            return ["test-app"]

        async def get_broadcasts_count(self, *args, **kwargs):
            return 1

        async def get_all_broadcasts(self, *args, **kwargs):
            return [BroadcastStream(streamId="camera-1", status="broadcasting")]

    service.ams_client = AMS()

    await service.sync_all_applications_streams(test_session)
    await service.sync_all_applications_streams(test_session)

    metrics = (
        await test_session.execute(select(StreamMetricHistory))
    ).scalars().all()
    assert len(metrics) == 1


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
async def test_sync_reconciles_stale_current_streams_only_after_complete_snapshot(
    test_session,
):
    service = SyncService()
    existing = AmsStreamCurrent(
        app_name="cctv-bantul",
        stream_id="removed-camera",
        name="Removed camera",
        status="broadcasting",
    )
    test_session.add(existing)
    await test_session.commit()

    class CompleteAMS:
        async def get_applications(self, **kwargs):
            return ["cctv-bantul"]

        async def get_broadcasts_count(self, *args, **kwargs):
            return 1

        async def get_all_broadcasts(self, *args, **kwargs):
            return [BroadcastStream(streamId="current-camera", status="finished")]

    service.ams_client = CompleteAMS()
    result = await service.sync_all_applications_streams(test_session)

    assert result["status"] == "SUCCESS"
    records = (
        await test_session.execute(
            select(AmsStreamCurrent).where(
                AmsStreamCurrent.app_name == "cctv-bantul"
            )
        )
    ).scalars().all()
    assert [record.stream_id for record in records] == ["current-camera"]
    history = (
        await test_session.execute(
            select(AmsStreamStatusHistory).where(
                AmsStreamStatusHistory.app_name == "cctv-bantul",
                AmsStreamStatusHistory.stream_id == "removed-camera",
            )
        )
    ).scalars().all()
    assert history[-1].new_status == "removed"


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
