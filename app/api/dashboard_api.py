import json
import asyncio
from datetime import datetime
from typing import Set, Optional
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.services.dashboard_service import get_dashboard_service, DashboardSnapshot
from app.services.application_service import get_application_service
from app.services.sync_service import get_sync_service

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard API"])
dashboard_service = get_dashboard_service()
app_service = get_application_service()
sync_service = get_sync_service()


def parse_date_to_ms(val: Optional[str], default_end: bool = False) -> Optional[int]:
    """Parse date string (YYYY-MM-DD, ISO, or epoch ms) to epoch milliseconds."""
    if not val:
        return None
    val = val.strip()
    try:
        if val.isdigit():
            return int(val)
        if len(val) == 10:  # e.g. "2026-10-30"
            dt = datetime.strptime(val, "%Y-%m-%d")
            if default_end:
                dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
            return int(dt.timestamp() * 1000)
        # Handles ISO format like 2026-10-30T09:00:00
        clean_val = val.replace("Z", "")
        dt = datetime.fromisoformat(clean_val)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


class DashboardBroadcaster:
    def __init__(self):
        self._subscribers: Set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=10)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self._subscribers.discard(q)

    def broadcast(self, snapshot_dict: dict):
        for q in list(self._subscribers):
            try:
                q.put_nowait(snapshot_dict)
            except asyncio.QueueFull:
                pass


dashboard_broadcaster = DashboardBroadcaster()


@router.get("/metrics")
async def get_dashboard_metrics():
    """Fetch current system metrics snapshot with ring buffer history."""
    snapshot = await dashboard_service.collect_snapshot()
    return snapshot


@router.get("/history")
async def get_dashboard_history(
    hours: Optional[int] = Query(None, description="Rentang riwayat dalam jam (1, 6, 24)"),
    all_history: bool = Query(False, description="Tampilkan seluruh riwayat"),
    start_date: Optional[str] = Query(None, description="Tanggal mulai (YYYY-MM-DD atau ISO)"),
    end_date: Optional[str] = Query(None, description="Tanggal selesai (YYYY-MM-DD atau ISO)"),
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch historical system metrics (CPU, RAM, JVM, Disk, Streams, Viewers) with custom time filters."""
    start_ts = parse_date_to_ms(start_date, default_end=False)
    end_ts = parse_date_to_ms(end_date, default_end=True)

    return await dashboard_service.get_historical_metrics(
        session=session,
        hours=hours or 1,
        all_history=all_history,
        start_ts=start_ts,
        end_ts=end_ts,
    )


@router.get("/system-streams-history")
async def get_system_streams_history(
    hours: Optional[int] = Query(None, description="Rentang riwayat dalam jam"),
    all_history: bool = Query(False, description="Tampilkan seluruh riwayat"),
    start_date: Optional[str] = Query(None, description="Tanggal mulai (YYYY-MM-DD atau ISO)"),
    end_date: Optional[str] = Query(None, description="Tanggal selesai (YYYY-MM-DD atau ISO)"),
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch system-wide aggregated stream metrics history with custom time filters."""
    start_ts = parse_date_to_ms(start_date, default_end=False)
    end_ts = parse_date_to_ms(end_date, default_end=True)

    return await app_service.get_system_stream_history(
        session=session,
        hours=hours or 1,
        all_history=all_history,
        start_ts=start_ts,
        end_ts=end_ts,
    )


@router.get("/sync-logs")
async def get_sync_logs(
    limit: int = Query(50, ge=1, le=200),
    app_name: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch audit logs of periodic/manual API data synchronizations."""
    return await sync_service.get_sync_logs(session=session, limit=limit, app_name=app_name)


@router.get("/stream-status-history")
async def get_stream_status_history(
    limit: int = Query(50, ge=1, le=200),
    app_name: Optional[str] = Query(None),
    stream_id: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch audit history of stream status transitions (e.g. broadcasting <-> finished)."""
    return await sync_service.get_status_history(
        session=session, limit=limit, app_name=app_name, stream_id=stream_id
    )


@router.post("/trigger-sync")
async def trigger_manual_sync(
    session: AsyncSession = Depends(get_db_session),
):
    """Manually trigger full synchronization of all AMS applications and streams."""
    result = await sync_service.sync_all_applications_streams(session)
    return result


@router.get("/server-info")
async def get_server_info():
    """Fetch server environment and system details (OS, CPU Cores, Java, AMS version)."""
    snapshot = await dashboard_service.collect_snapshot()
    return {
        "ams_connected": snapshot.ams_connected,
        "version_name": snapshot.version_name,
        "version_type": snapshot.version_type,
        "instance_id": snapshot.instance_id,
        "os_name": snapshot.os_name,
        "os_arch": snapshot.os_arch,
        "java_version": snapshot.java_version,
        "processor_count": snapshot.processor_count,
        "uptime": snapshot.uptime_formatted,
        "server_time": snapshot.server_time,
    }


@router.get("/sse")
async def sse_dashboard_stream():
    """Server-Sent Events endpoint pushing real-time system metrics every 3 seconds."""
    q = dashboard_broadcaster.subscribe()

    async def event_generator():
        try:
            # Send initial snapshot immediately upon connection
            initial = await dashboard_service.collect_snapshot()
            yield f"data: {json.dumps(initial.model_dump())}\n\n"

            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=10.0)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    # Send periodic keep-alive
                    yield ": ping\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            dashboard_broadcaster.unsubscribe(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
