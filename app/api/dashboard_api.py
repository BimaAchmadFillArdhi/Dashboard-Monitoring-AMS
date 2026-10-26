import json
import asyncio
from typing import Set
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.services.dashboard_service import get_dashboard_service, DashboardSnapshot

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard API"])
dashboard_service = get_dashboard_service()


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
