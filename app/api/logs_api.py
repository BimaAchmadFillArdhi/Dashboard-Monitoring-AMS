import json
import asyncio
import time
from typing import Optional, Set
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.services.log_service import get_log_service
from app.services.health_service import HealthService
from app.clients.models import BroadcastStream
from app.db.models import AmsStreamCurrent

router = APIRouter(prefix="/api/logs", tags=["Logs API"])
log_service = get_log_service()
health_service = HealthService()
_problematic_cache: list[dict] = []
_problematic_cache_at = 0.0
_problematic_cache_lock = asyncio.Lock()
PROBLEMATIC_CACHE_SECONDS = 15


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
                while not q.empty():
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                q.put_nowait({"_refresh": True})


log_broadcaster = LogBroadcaster()


@router.get("")
async def get_logs(
    date: Optional[str] = Query(None, description="Tanggal YYYY-MM-DD atau ALL"),
    level: Optional[str] = Query(None, description="ERROR, WARN, INFO, DEBUG, ALL"),
    stream_id: Optional[str] = Query(None, description="Filter stream ID atau ALL"),
    app_name: Optional[str] = Query(None, description="Filter app name atau ALL"),
    q: Optional[str] = Query(None, description="Pencarian kata kunci"),
    fingerprint: Optional[str] = Query(None, description="Filter kejadian grup fingerprint"),
    dedupe: bool = Query(False, description="Gabungkan log berulang"),
    cursor: Optional[int] = Query(None, ge=1, description="ID log terakhir untuk paginasi"),
    limit: int = Query(50, ge=1, le=100),
    session: AsyncSession = Depends(get_db_session),
):
    """Return one bounded page of log summaries and cached aggregate counts."""
    result = await log_service.query_logs(
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
    return result.model_dump(exclude_none=True)


@router.get("/filters")
async def get_log_filters(session: AsyncSession = Depends(get_db_session)):
    """Fetch available stream IDs and applications for search filter dropdowns."""
    return await log_service.get_filter_options(session)


@router.get("/updates")
async def get_log_updates(
    after_id: int = Query(0, ge=0),
    date: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    stream_id: Optional[str] = Query(None),
    app_name: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    fingerprint: Optional[str] = Query(None),
    dedupe: bool = Query(False),
    limit: int = Query(50, ge=1, le=100),
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch only log IDs created after the client's last observed record."""
    result = await log_service.query_logs(
        session=session,
        date_str=date,
        level=level,
        stream_id=stream_id,
        app_name=app_name,
        search_q=q,
        fingerprint=fingerprint,
        dedupe=dedupe,
        after_id=after_id,
        limit=limit,
    )
    return result.model_dump(exclude_none=True)


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
    """Rank problematic CCTV using the periodically synchronized MySQL snapshot."""
    global _problematic_cache_at, _problematic_cache
    if time.monotonic() - _problematic_cache_at < PROBLEMATIC_CACHE_SECONDS:
        return _problematic_cache

    async with _problematic_cache_lock:
        if time.monotonic() - _problematic_cache_at < PROBLEMATIC_CACHE_SECONDS:
            return _problematic_cache

        result = await session.execute(
            select(
                AmsStreamCurrent.app_name,
                AmsStreamCurrent.stream_id,
                AmsStreamCurrent.status,
                AmsStreamCurrent.bitrate,
            ).where(
                AmsStreamCurrent.app_name.in_(
                    ["atcs", "atcs-kota", "cctv-sekolah"]
                )
            )
        )
        streams = [
            (
                BroadcastStream(
                    streamId=row.stream_id,
                    status=row.status,
                    bitrate=row.bitrate,
                ),
                row.app_name,
            )
            for row in result
        ]
        reports = await health_service.evaluate_streams_health(streams, session)
        problematic = [
            report
            for report in reports
            if report.status in ("Bermasalah", "Perhatian")
        ]
        problematic.sort(
            key=lambda report: (
                0 if report.status == "Bermasalah" else 1,
                -report.total_events,
                -report.last_event_ts,
            )
        )
        _problematic_cache = [report.model_dump() for report in problematic]
        _problematic_cache_at = time.monotonic()
        return _problematic_cache


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
                    if log_item.get("_refresh"):
                        yield f"data: {json.dumps({'type': 'refresh'})}\n\n"
                        continue
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


@router.get("/{log_id}")
async def get_log_detail(
    log_id: int,
    session: AsyncSession = Depends(get_db_session),
):
    """Load technical log content only when the user expands a row."""
    detail = await log_service.get_log_detail(session, log_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Log tidak ditemukan")
    return detail
