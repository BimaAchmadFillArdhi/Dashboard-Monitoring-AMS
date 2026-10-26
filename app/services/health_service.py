import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import LogEntry
from app.clients.models import BroadcastStream


class StreamHealthReport(BaseModel):
    stream_id: str
    app_name: str
    status: str  # Sehat, Perhatian, Bermasalah, Offline
    tone: str    # success, warning, danger, neutral
    problem_summary: str
    action_hint: Optional[str] = None
    errors_5m: int = 0
    warns_5m: int = 0
    errors_60m: int = 0
    warns_60m: int = 0
    total_events: int = 0
    last_event_ts: int = 0
    last_event_time: str = "-"
    bitrate: int = 0
    filter_url: str = ""


class HealthService:
    def __init__(self, warn_threshold_5m: int = 15, error_threshold_5m: int = 2):
        self.warn_threshold_5m = warn_threshold_5m
        self.error_threshold_5m = error_threshold_5m

    async def evaluate_stream_health(
        self, stream: BroadcastStream, app_name: str, session: AsyncSession
    ) -> StreamHealthReport:
        now_ms = int(time.time() * 1000)
        t_5m_ago = now_ms - (5 * 60 * 1000)
        t_60m_ago = now_ms - (60 * 60 * 1000)

        stream_id = stream.streamId

        # Query counts for WARN and ERROR in 5m and 60m windows
        stmt = (
            select(
                LogEntry.level,
                func.count(LogEntry.id).label("cnt"),
                func.max(LogEntry.ts).label("max_ts"),
            )
            .where(
                LogEntry.stream_id == stream_id,
                LogEntry.ts >= t_60m_ago,
                LogEntry.level.in_(["ERROR", "WARN"]),
            )
            .group_by(LogEntry.level)
        )
        res = await session.execute(stmt)
        rows = res.all()

        warns_60m = 0
        errors_60m = 0
        last_ts = 0

        for r in rows:
            if r.level == "WARN":
                warns_60m = r.cnt
            elif r.level == "ERROR":
                errors_60m = r.cnt
            if r.max_ts and r.max_ts > last_ts:
                last_ts = r.max_ts

        # Query counts in 5m window specifically
        stmt_5m = (
            select(
                LogEntry.level,
                func.count(LogEntry.id).label("cnt"),
            )
            .where(
                LogEntry.stream_id == stream_id,
                LogEntry.ts >= t_5m_ago,
                LogEntry.level.in_(["ERROR", "WARN"]),
            )
            .group_by(LogEntry.level)
        )
        res_5m = await session.execute(stmt_5m)
        rows_5m = res_5m.all()

        warns_5m = 0
        errors_5m = 0
        for r in rows_5m:
            if r.level == "WARN":
                warns_5m = r.cnt
            elif r.level == "ERROR":
                errors_5m = r.cnt

        # Find latest friendly explanation if there are issues
        latest_problem = "Transmisi kamera berjalan normal tanpa hambatan."
        latest_action = None
        if errors_60m > 0 or warns_60m > 0:
            stmt_latest = (
                select(LogEntry.friendly_title, LogEntry.friendly_message, LogEntry.action_hint)
                .where(
                    LogEntry.stream_id == stream_id,
                    LogEntry.level.in_(["ERROR", "WARN"]),
                )
                .order_by(LogEntry.ts.desc())
                .limit(1)
            )
            latest_res = await session.execute(stmt_latest)
            latest_log = latest_res.first()
            if latest_log:
                latest_problem = latest_log.friendly_message or latest_log.friendly_title
                latest_action = latest_log.action_hint

        # Determine health status
        if stream.status != "broadcasting":
            status = "Offline"
            tone = "neutral"
            problem = "Kamera tidak aktif atau aliran RTSP terputus."
            action = "Periksa power PoE kamera dan koneksi jaringan ke server."
        elif errors_5m >= self.error_threshold_5m or warns_5m >= self.warn_threshold_5m or (stream.bitrate == 0 and warns_5m > 0):
            status = "Bermasalah"
            tone = "danger"
            problem = latest_problem
            action = latest_action or "Lakukan pengecekan kestabilan jaringan kamera dan restart stream."
        elif errors_60m > 0 or warns_5m > 0 or warns_60m >= 5:
            status = "Perhatian"
            tone = "warning"
            problem = latest_problem
            action = latest_action or "Amati koneksi beberapa menit ke depan."
        else:
            status = "Sehat"
            tone = "success"
            problem = "Koneksi stream stabil dan transmisi video normal."
            action = None

        last_time_str = "-"
        if last_ts > 0:
            dt = datetime.fromtimestamp(last_ts / 1000)
            last_time_str = dt.strftime("%H:%M:%S")

        return StreamHealthReport(
            stream_id=stream_id,
            app_name=app_name,
            status=status,
            tone=tone,
            problem_summary=problem,
            action_hint=action,
            errors_5m=errors_5m,
            warns_5m=warns_5m,
            errors_60m=errors_60m,
            warns_60m=warns_60m,
            total_events=errors_60m + warns_60m,
            last_event_ts=last_ts,
            last_event_time=last_time_str,
            bitrate=stream.bitrate,
            filter_url=f"/logs?stream_id={stream_id}",
        )

    async def get_problematic_streams(
        self, streams: List[BroadcastStream], app_name: str, session: AsyncSession
    ) -> List[StreamHealthReport]:
        """Return list of streams that are in 'Bermasalah' or 'Perhatian' state, ranked by severity."""
        reports = []
        for s in streams:
            rep = await self.evaluate_stream_health(s, app_name, session)
            reports.append(rep)

        # Filter only problematic or attention-needed streams
        problematic = [r for r in reports if r.status in ("Bermasalah", "Perhatian")]
        # Sort by status severity ('Bermasalah' first), then by event count descending
        problematic.sort(
            key=lambda x: (0 if x.status == "Bermasalah" else 1, -x.total_events, -x.last_event_ts)
        )
        return problematic
