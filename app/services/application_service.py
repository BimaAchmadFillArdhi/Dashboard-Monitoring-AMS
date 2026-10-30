import re
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from sqlalchemy import select, delete, desc, asc, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.ams_client import get_ams_client
from app.clients.models import BroadcastStream, VoDItem, mask_rtsp_url
from app.db.models import StreamMetricHistory
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


class VoDTableRowDTO(BaseModel):
    vod_id: str
    stream_name: str
    stream_id: str
    file_path: str
    vod_name: str
    file_size_mb: float
    duration_formatted: str
    creation_date_formatted: str
    vod_type: str


class AppSummaryDTO(BaseModel):
    name: str
    live_count: int
    vod_count: int
    storage_mb: float
    problematic_count: int


class ApplicationService:
    def __init__(self):
        self._last_saved_ts: Dict[str, float] = {}
    async def get_applications_overview(self, session: AsyncSession) -> List[AppSummaryDTO]:
        """Fetch all applications with live count, storage, and problematic CCTV count."""
        apps_info = await ams_client.get_applications_info()
        result = []

        for app in apps_info:
            prob_count = 0
            try:
                # Fetch streams to evaluate health
                streams = await ams_client.get_all_broadcasts(app.name, batch_size=50)
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

    async def get_vods_for_app(
        self,
        app_name: str,
        offset: int = 0,
        size: int = 50,
        search_q: Optional[str] = None,
    ) -> List[VoDTableRowDTO]:
        """Fetch VoD recordings for an application."""
        raw_vods = await ams_client.get_vod_list(app_name, offset=offset, size=size)
        rows = []

        for v in raw_vods:
            if search_q:
                q = search_q.lower()
                vn = (v.vodName or "").lower()
                sn = (v.streamName or "").lower()
                sid = (v.streamId or "").lower()
                if q not in vn and q not in sn and q not in sid:
                    continue

            from datetime import datetime
            date_str = "-"
            if v.date and v.date > 0:
                try:
                    date_str = datetime.fromtimestamp(v.date / 1000).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pass

            rows.append(VoDTableRowDTO(
                vod_id=v.vodId or v.vodName or "-",
                stream_name=v.streamName or v.vodName or "-",
                stream_id=v.streamId or "-",
                file_path=v.filePath or "-",
                vod_name=v.vodName or "-",
                file_size_mb=v.file_size_mb,
                duration_formatted=v.duration_formatted,
                creation_date_formatted=date_str,
                vod_type=v.type or "userVoD",
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

        date_str = "-"
        if s.date and s.date > 0:
            try:
                date_str = datetime.fromtimestamp(s.date / 1000).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

        bitrate_bps = s.bitrate or 0
        bitrate_mbps = round(bitrate_bps / 1_000_000.0, 2)
        bitrate_kbps = round(bitrate_bps / 1000.0, 1)
        speed_val = round(s.speed, 2) if s.speed is not None else 0.0

        detail = {
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
            "bitrate": bitrate_bps,
            "bitrate_mbps": bitrate_mbps,
            "bitrate_kbps": bitrate_kbps,
            "speed": speed_val,
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
        return detail

    async def save_stream_metric(
        self,
        session: AsyncSession,
        app_name: str,
        stream_id: str,
        detail: Dict[str, Any],
        force: bool = False,
    ) -> bool:
        """Persist stream metric snapshot to SQLite if interval has passed."""
        key = f"{app_name}:{stream_id}"
        now = time.monotonic()
        last_ts = self._last_saved_ts.get(key, 0.0)
        # Throttle to at most once per 4 seconds per stream unless forced
        if not force and (now - last_ts < 4.0):
            return False

        try:
            ts_ms = int(time.time() * 1000)
            rec = StreamMetricHistory(
                ts=ts_ms,
                app_name=app_name,
                stream_id=stream_id,
                status=detail.get("status", "finished"),
                bitrate=detail.get("bitrate", 0),
                bitrate_mbps=detail.get("bitrate_mbps", 0.0),
                speed=detail.get("speed", 0.0),
                total_viewers=detail.get("total_viewers", 0),
                hls_viewers=detail.get("hls_viewers", 0),
                webrtc_viewers=detail.get("webrtc_viewers", 0),
                dash_viewers=detail.get("dash_viewers", 0),
            )
            session.add(rec)
            await session.commit()
            self._last_saved_ts[key] = now
            return True
        except Exception:
            await session.rollback()
            return False

    async def get_stream_metric_history(
        self,
        session: AsyncSession,
        app_name: str,
        stream_id: str,
        hours: int = 1,
    ) -> Dict[str, Any]:
        """Query stream metric history for chart display with speed filtering and ideal reference line."""
        cutoff_ms = int((time.time() - (hours * 3600)) * 1000)
        stmt = (
            select(StreamMetricHistory)
            .where(
                StreamMetricHistory.app_name == app_name,
                StreamMetricHistory.stream_id == stream_id,
                StreamMetricHistory.ts >= cutoff_ms,
            )
            .order_by(StreamMetricHistory.ts.asc())
            .limit(600)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()

        timestamps = []
        bitrate_mbps = []
        speed_raw = []
        speed_filtered = []
        viewers = []

        for r in records:
            dt = datetime.fromtimestamp(r.ts / 1000)
            fmt = "%H:%M:%S" if hours <= 2 else "%d/%m %H:%M"
            timestamps.append(dt.strftime(fmt))
            bitrate_mbps.append(round(r.bitrate_mbps, 2))
            
            raw_s = round(r.speed, 2)
            speed_raw.append(raw_s)
            
            # Sanitization of speed anomalies from RTSP timestamp jitter / rollover
            # Normal speed is ~1.0x. Values < 0 indicate DTS rollover/negative step.
            # Values > 3.0 indicate massive catch-up packet bursts.
            if raw_s < 0.0:
                s_filt = 0.0
            elif raw_s > 3.0:
                s_filt = 3.0
            else:
                s_filt = raw_s
            speed_filtered.append(s_filt)
            viewers.append(r.total_viewers)

        # If no records exist yet in history, fetch current stream detail to bootstrap chart
        if not timestamps:
            detail = await self.get_stream_detail(app_name, stream_id, session)
            if detail:
                dt_now = datetime.now().strftime("%H:%M:%S")
                timestamps.append(dt_now)
                b_val = detail.get("bitrate_mbps", 0.0)
                bitrate_mbps.append(b_val)
                raw_s = detail.get("speed", 0.0)
                speed_raw.append(raw_s)
                speed_filtered.append(max(0.0, min(raw_s, 3.0)) if raw_s >= 0 else 0.0)
                viewers.append(detail.get("total_viewers", 0))
                # Persist this bootstrap point
                await self.save_stream_metric(session, app_name, stream_id, detail, force=True)

        ideal_speed = [1.0] * len(timestamps)

        return {
            "stream_id": stream_id,
            "app_name": app_name,
            "timestamps": timestamps,
            "bitrate_mbps": bitrate_mbps,
            "speed": speed_raw,
            "speed_filtered": speed_filtered,
            "ideal_speed": ideal_speed,
            "viewers": viewers,
            "points_count": len(timestamps),
        }

    async def collect_all_streams_metrics(self, session: AsyncSession) -> Dict[str, Any]:
        """Poll all streams from AMS using pagination and sync to MySQL via sync_service."""
        from app.services.sync_service import get_sync_service
        sync_svc = get_sync_service()
        res = await sync_svc.sync_all_applications_streams(session)
        return res

    async def get_system_stream_history(
        self,
        session: AsyncSession,
        hours: Optional[int] = 1,
        all_history: bool = False,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Aggregate total system bitrate, active/inactive streams, and total viewers over time with time filter support."""
        time_span_hours = 1.0

        stmt = select(
            StreamMetricHistory.ts,
            func.sum(
                case((StreamMetricHistory.status == "broadcasting", StreamMetricHistory.bitrate_mbps), else_=0.0)
            ).label("total_bitrate"),
            func.sum(
                case((StreamMetricHistory.status == "broadcasting", 1), else_=0)
            ).label("active_streams"),
            func.sum(
                case((StreamMetricHistory.status != "broadcasting", 1), else_=0)
            ).label("inactive_streams"),
            func.sum(
                case((StreamMetricHistory.status == "broadcasting", StreamMetricHistory.total_viewers), else_=0)
            ).label("total_viewers"),
        ).group_by(StreamMetricHistory.ts).order_by(asc(StreamMetricHistory.ts))

        if all_history:
            time_span_hours = 9999.0
        elif start_ts is not None and end_ts is not None:
            stmt = stmt.where(
                StreamMetricHistory.ts >= start_ts,
                StreamMetricHistory.ts <= end_ts,
            )
            time_span_hours = max(1.0, (end_ts - start_ts) / 3600000.0)
        else:
            h = hours if hours is not None and hours > 0 else 1
            cutoff_ms = int((time.time() - (h * 3600)) * 1000)
            stmt = stmt.where(StreamMetricHistory.ts >= cutoff_ms)
            time_span_hours = float(h)

        res = await session.execute(stmt)
        rows = list(res.all())

        # Downsample if rows > 120
        total_rows = len(rows)
        target_points = 120
        if total_rows > target_points:
            step = total_rows / target_points
            sampled_indices = [int(i * step) for i in range(target_points)]
            if (total_rows - 1) not in sampled_indices:
                sampled_indices[-1] = total_rows - 1
            rows = [rows[i] for i in sampled_indices]

        timestamps = []
        total_bitrate = []
        active_counts = []
        inactive_counts = []
        viewer_counts = []

        if time_span_hours <= 6:
            fmt = "%H:%M"
        elif time_span_hours <= 48:
            fmt = "%d/%m %H:%M"
        else:
            fmt = "%d/%m/%y %H:%M"

        for r in rows:
            ts_val, b_sum, act, inact, v_sum = r
            dt = datetime.fromtimestamp(ts_val / 1000)
            timestamps.append(dt.strftime(fmt))
            total_bitrate.append(round(float(b_sum or 0.0), 2))
            active_counts.append(int(act or 0))
            inactive_counts.append(int(inact or 0))
            viewer_counts.append(int(v_sum or 0))

        return {
            "timestamps": timestamps,
            "total_bitrate_mbps": total_bitrate,
            "active_streams": active_counts,
            "inactive_streams": inactive_counts,
            "total_viewers": viewer_counts,
            "points_count": len(timestamps),
        }

    async def cleanup_old_stream_metrics(self, session: AsyncSession, retention_days: int = 7) -> int:
        """Purge stream metric history older than retention policy."""
        cutoff_ms = int((time.time() - (retention_days * 86400)) * 1000)
        stmt = delete(StreamMetricHistory).where(StreamMetricHistory.ts < cutoff_ms)
        res = await session.execute(stmt)
        await session.commit()
        return res.rowcount



_application_service: Optional[ApplicationService] = None


def get_application_service() -> ApplicationService:
    global _application_service
    if _application_service is None:
        _application_service = ApplicationService()
    return _application_service
