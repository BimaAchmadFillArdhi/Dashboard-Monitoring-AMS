import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select, func

from app.main import app
from app.db.session import init_db, AsyncSessionLocal
from app.db.models import (
    AmsStreamCurrent,
    AmsSyncLog,
    AmsStreamStatusHistory,
    ServerMetricHistory,
    StreamMetricHistory,
)
from app.clients.ams_client import get_ams_client
from app.services.sync_service import get_sync_service


@pytest.mark.asyncio
async def test_mysql_initialization():
    """Ensure database schema is created properly in MySQL."""
    await init_db()
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(func.count()).select_from(AmsStreamCurrent))
        count = res.scalar()
        assert count is not None
        assert count >= 0


@pytest.mark.asyncio
async def test_ams_pagination_fetches_all_streams():
    """Ensure pagination retrieves all 104+ streams from atcs-kota without being cut off."""
    client = get_ams_client()
    streams = await client.get_all_broadcasts("atcs-kota", batch_size=50)
    assert len(streams) >= 100, f"Expected at least 100 streams, got {len(streams)}"


@pytest.mark.asyncio
async def test_sync_service_execution():
    """Test full synchronization into MySQL tables."""
    sync_service = get_sync_service()
    async with AsyncSessionLocal() as session:
        result = await sync_service.sync_all_applications_streams(session)
        assert result["status"] == "SUCCESS"
        assert result["total_synced"] >= 104

        # Verify ams_streams_current
        res_current = await session.execute(select(func.count()).select_from(AmsStreamCurrent))
        assert res_current.scalar() >= 104

        # Verify ams_sync_logs
        res_logs = await session.execute(select(func.count()).select_from(AmsSyncLog))
        assert res_logs.scalar() > 0

        # Verify ams_stream_status_history
        res_status = await session.execute(select(func.count()).select_from(AmsStreamStatusHistory))
        assert res_status.scalar() > 0


@pytest.mark.asyncio
async def test_dashboard_history_filters_api():
    """Test dashboard history endpoints with 1h, 6h, 24h, all_history, and custom date range."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1 hour
        r_1h = await ac.get("/api/dashboard/history?hours=1")
        assert r_1h.status_code == 200
        assert "timestamps" in r_1h.json()

        # 6 hours
        r_6h = await ac.get("/api/dashboard/history?hours=6")
        assert r_6h.status_code == 200

        # 24 hours
        r_24h = await ac.get("/api/dashboard/history?hours=24")
        assert r_24h.status_code == 200

        # All history
        r_all = await ac.get("/api/dashboard/history?all_history=true")
        assert r_all.status_code == 200

        # Custom date range
        r_custom = await ac.get("/api/dashboard/history?start_date=2026-01-01&end_date=2026-12-31")
        assert r_custom.status_code == 200

        # Sync logs endpoint
        r_sync_logs = await ac.get("/api/dashboard/sync-logs?limit=10")
        assert r_sync_logs.status_code == 200
        assert isinstance(r_sync_logs.json(), list)

        # Stream status history endpoint
        r_stat_hist = await ac.get("/api/dashboard/stream-status-history?limit=10")
        assert r_stat_hist.status_code == 200
        assert isinstance(r_stat_hist.json(), list)
