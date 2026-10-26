import json
import asyncio
from typing import Optional, List, Set
from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.services.log_service import get_log_service, LogItemDTO
from app.services.health_service import HealthService
from app.clients.ams_client import get_ams_client

router = APIRouter(prefix="/api/logs", tags=["Logs API"])
log_service = get_log_service()
health_service = HealthService()
ams_client = get_ams_client()

# SSE Broadcaster for real-time live log tailing
class LogBroadcaster:
    def __init__(self):
        self._subscribers: Set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=100)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        self._subscribers.discard(q)

    def broadcast(self, log_dict: dict):
        for q in list(self._subscribers):
            try:
                q.put_nowait(log_dict)
            except asyncio.QueueFull:
                pass


log_broadcaster = LogBroadcaster()


@router.get("")
async def get_logs(
    date: Optional[str] = Query(None, description="Tanggal YYYY-MM-DD"),
    level: Optional[str] = Query(None, description="ERROR, WARN, INFO, DEBUG, ALL"),
    stream_id: Optional[str] = Query(None, description="Filter stream ID atau ALL"),
    app_name: Optional[str] = Query(None, description="Filter app name atau ALL"),
    q: Optional[str] = Query(None, description="Pencarian kata kunci"),
    fingerprint: Optional[str] = Query(None, description="Filter kejadian grup fingerprint"),
    dedupe: bool = Query(False, description="Gabungkan log berulang"),
    cursor: Optional[int] = Query(None, description="ID log terakhir untuk paginasi"),
    limit: int = Query(50, ge=1, le=100),
    session: AsyncSession = Depends(get_db_session),
):
    """Query logs with filters, date selection, deduplication, and level counts."""
    return await log_service.query_logs(
        session=session,
        date_str=date,
        level=level,
        stream_id=stream_id,
        app_name=app_name,
        search_q=q,
        fingerprint=fingerprint,
        dedupe=dedupe,
        cursor_id=cursor,
        limit=limit,
    )


@router.get("/export.csv")
async def export_logs_csv(
    date: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    stream_id: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db_session),
):
    """Download filtered logs as CSV."""
    csv_content = await log_service.export_logs_csv(
        session=session,
        date_str=date,
        level=level,
        stream_id=stream_id,
        search_q=q,
    )
    filename = f"ams_logs_{date or 'all'}.csv"
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/problematic-cctv")
async def get_problematic_cctv(
    session: AsyncSession = Depends(get_db_session),
):
    """Get list of CCTV streams ranked by issue severity."""
    # Fetch streams from atcs and atcs-kota
    all_streams = []
    for app in ["atcs", "atcs-kota", "cctv-sekolah"]:
        try:
            streams = await ams_client.get_broadcasts_list(app, offset=0, size=100)
            for s in streams:
                all_streams.append((s, app))
        except Exception:
            pass

    problematic = []
    for s, app in all_streams:
        rep = await health_service.evaluate_stream_health(s, app, session)
        if rep.status in ("Bermasalah", "Perhatian"):
            problematic.append(rep)

    problematic.sort(
        key=lambda x: (0 if x.status == "Bermasalah" else 1, -x.total_events, -x.last_event_ts)
    )
    return problematic


@router.get("/live-sse")
async def sse_live_logs():
    """Server-Sent Events (SSE) endpoint for real-time live log tailing."""
    q = log_broadcaster.subscribe()

    async def event_generator():
        try:
            # Send initial keepalive / connection packet
            yield f"data: {json.dumps({'type': 'connected', 'message': 'SSE live stream terhubung'})}\n\n"
            while True:
                # Wait for new log event from broadcaster with timeout for heartbeat
                try:
                    log_item = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps({'type': 'log', 'data': log_item})}\n\n"
                except asyncio.TimeoutError:
                    # Heartbeat comment to keep connection alive
                    yield ": ping\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            log_broadcaster.unsubscribe(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
