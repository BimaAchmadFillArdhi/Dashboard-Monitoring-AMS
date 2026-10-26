import re
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.ams_client import get_ams_client
from app.clients.models import BroadcastStream, mask_rtsp_url
from app.services.health_service import HealthService, StreamHealthReport

ams_client = get_ams_client()
health_service = HealthService()


class StreamTableRowDTO(BaseModel):
    stream_id: str
    name: str
    status: str
    health_status: str       # Sehat, Perhatian, Bermasalah, Offline
    health_tone: str         # success, warning, danger, neutral
    problem_summary: str
    stream_type: str
    masked_url: str
    bitrate: int
    speed: float
    total_viewers: int
    resolution_str: str
    created_date_formatted: str
    mp4_enabled: bool


class AppSummaryDTO(BaseModel):
    name: str
    live_count: int
    vod_count: int
    storage_mb: float
    problematic_count: int


class ApplicationService:
    async def get_applications_overview(self, session: AsyncSession) -> List[AppSummaryDTO]:
        """Fetch all applications with live count, storage, and problematic CCTV count."""
        apps_info = await ams_client.get_applications_info()
        result = []

        for app in apps_info:
            prob_count = 0
            try:
                # Fetch streams to evaluate health
                streams = await ams_client.get_broadcasts_list(app.name, offset=0, size=100)
                for s in streams:
                    rep = await health_service.evaluate_stream_health(s, app.name, session)
                    if rep.status in ("Bermasalah", "Perhatian"):
                        prob_count += 1
            except Exception:
                pass

            result.append(AppSummaryDTO(
                name=app.name,
                live_count=app.liveStreamCount,
                vod_count=app.vodCount,
                storage_mb=round(app.storage / (1024**2), 1),
                problematic_count=prob_count,
            ))

        return result

    async def get_streams_for_app(
        self,
        app_name: str,
        session: AsyncSession,
        offset: int = 0,
        size: int = 50,
        search_q: Optional[str] = None,
    ) -> List[StreamTableRowDTO]:
        """Fetch streams for an app and evaluate their operational health."""
        raw_streams = await ams_client.get_broadcasts_list(app_name, offset=offset, size=size)
        rows = []

        for s in raw_streams:
            if search_q:
                q = search_q.lower()
                name = (s.name or "").lower()
                sid = (s.streamId or "").lower()
                if q not in name and q not in sid:
                    continue

            # Evaluate health
            rep = await health_service.evaluate_stream_health(s, app_name, session)

            # Date formatted
            from datetime import datetime
            date_str = "-"
            if s.date and s.date > 0:
                try:
                    date_str = datetime.fromtimestamp(s.date / 1000).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pass

            rows.append(StreamTableRowDTO(
                stream_id=s.streamId,
                name=s.name or s.streamId,
                status=s.status,
                health_status=rep.status,
                health_tone=rep.tone,
                problem_summary=rep.problem_summary,
                stream_type=s.type,
                masked_url=s.masked_url,
                bitrate=s.bitrate,
                speed=s.speed,
                total_viewers=s.total_viewers,
                resolution_str=s.resolution_str,
                created_date_formatted=date_str,
                mp4_enabled=bool(s.mp4Enabled),
            ))

        return rows

    async def get_stream_detail(
        self, app_name: str, stream_id: str, session: AsyncSession
    ) -> Optional[Dict[str, Any]]:
        """Fetch detailed information for a single stream (read-only)."""
        s = await ams_client.get_broadcast(app_name, stream_id)
        if not s:
            return None

        rep = await health_service.evaluate_stream_health(s, app_name, session)

        from datetime import datetime
        date_str = "-"
        if s.date and s.date > 0:
            try:
                date_str = datetime.fromtimestamp(s.date / 1000).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

        return {
            "stream_id": s.streamId,
            "name": s.name or s.streamId,
            "status": s.status,
            "health_status": rep.status,
            "health_tone": rep.tone,
            "problem_summary": rep.problem_summary,
            "action_hint": rep.action_hint,
            "stream_type": s.type,
            "masked_url": s.masked_url,
            "origin_address": s.originAdress or "-",
            "bitrate": s.bitrate,
            "speed": s.speed,
            "total_viewers": s.total_viewers,
            "hls_viewers": s.hlsViewerCount,
            "webrtc_viewers": s.webRTCViewerCount,
            "dash_viewers": s.dashViewerCount,
            "resolution": s.resolution_str,
            "created_at": date_str,
            "mp4_enabled": bool(s.mp4Enabled),
            "webm_enabled": bool(s.webMEnabled),
            "auto_start_stop": s.autoStartStopEnabled,
        }


_application_service: Optional[ApplicationService] = None


def get_application_service() -> ApplicationService:
    global _application_service
    if _application_service is None:
        _application_service = ApplicationService()
    return _application_service
