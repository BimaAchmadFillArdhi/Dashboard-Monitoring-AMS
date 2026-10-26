import json
import asyncio
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.services.application_service import get_application_service

router = APIRouter(prefix="/api/applications", tags=["Applications API"])
app_service = get_application_service()


@router.get("")
async def get_applications(session: AsyncSession = Depends(get_db_session)):
    """Fetch all applications with stream counts and problematic badges."""
    return await app_service.get_applications_overview(session)


@router.get("/{app_name}/streams")
async def get_app_streams(
    app_name: str,
    offset: int = Query(0, ge=0),
    size: int = Query(50, ge=1, le=100),
    q: Optional[str] = Query(None, description="Pencarian nama atau streamId"),
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch streams for an application with operational health status."""
    return await app_service.get_streams_for_app(
        app_name=app_name,
        session=session,
        offset=offset,
        size=size,
        search_q=q,
    )


@router.get("/{app_name}/streams/{stream_id}")
async def get_stream_detail(
    app_name: str,
    stream_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """Fetch detailed information for a single stream (read-only, masked RTSP credentials)."""
    detail = await app_service.get_stream_detail(app_name, stream_id, session)
    if not detail:
        raise HTTPException(status_code=404, detail="Stream tidak ditemukan")
    return detail


@router.get("/{app_name}/streams/{stream_id}/live-sse")
async def sse_stream_drawer(
    app_name: str,
    stream_id: str,
    session: AsyncSession = Depends(get_db_session),
):
    """SSE endpoint to stream live bitrate, speed, and viewer count while drawer is open."""
    async def event_generator():
        try:
            while True:
                detail = await app_service.get_stream_detail(app_name, stream_id, session)
                if detail:
                    payload = {
                        "bitrate": detail["bitrate"],
                        "speed": detail["speed"],
                        "total_viewers": detail["total_viewers"],
                        "hls_viewers": detail["hls_viewers"],
                        "webrtc_viewers": detail["webrtc_viewers"],
                        "status": detail["status"],
                        "health_status": detail["health_status"],
                        "health_tone": detail["health_tone"],
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
