import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from sqlalchemy import select, func, and_, case, or_
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

    def _build_report(
        self,
        stream: BroadcastStream,
        app_name: str,
        warns_5m: int,
        errors_5m: int,
        warns_60m: int,
        errors_60m: int,
        last_ts: int,
        latest_log: Optional[Dict[str, Any]],
    ) -> StreamHealthReport:
        latest_problem = "Transmisi kamera berjalan normal tanpa hambatan."
        latest_action = None
        if latest_log:
            latest_problem = (
                latest_log["friendly_message"]
                or latest_log["friendly_title"]
                or latest_problem
            )
            latest_action = latest_log["action_hint"]

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
            last_time_str = datetime.fromtimestamp(last_ts / 1000).strftime("%H:%M:%S")

        return StreamHealthReport(
            stream_id=stream.streamId,
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
            filter_url=f"/logs?stream_id={stream.streamId}",
        )

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
                LogEntry.app_name == app_name,
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
                LogEntry.app_name == app_name,
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

        latest_log = None
        if errors_60m > 0 or warns_60m > 0:
            stmt_latest = (
                select(LogEntry.friendly_title, LogEntry.friendly_message, LogEntry.action_hint)
                .where(
                    LogEntry.app_name == app_name,
                    LogEntry.stream_id == stream_id,
                    LogEntry.level.in_(["ERROR", "WARN"]),
                )
                .order_by(LogEntry.ts.desc())
                .limit(1)
            )
            latest_res = await session.execute(stmt_latest)
            latest_log = latest_res.first()

        return self._build_report(
            stream,
            app_name,
            warns_5m,
            errors_5m,
            warns_60m,
            errors_60m,
            last_ts,
            {
                "friendly_title": latest_log.friendly_title,
                "friendly_message": latest_log.friendly_message,
                "action_hint": latest_log.action_hint,
            }
            if latest_log
            else None,
        )

    async def evaluate_streams_health(
        self,
        streams: List[tuple[BroadcastStream, str]],
        session: AsyncSession,
    ) -> List[StreamHealthReport]:
        if not streams:
            return []

        now_ms = int(time.time() * 1000)
        t_5m_ago = now_ms - (5 * 60 * 1000)
        t_60m_ago = now_ms - (60 * 60 * 1000)
        pairs = {(app_name, stream.streamId) for stream, app_name in streams}
        pair_filter = or_(
            *[
                and_(LogEntry.app_name == app_name, LogEntry.stream_id == stream_id)
                for app_name, stream_id in pairs
            ]
        )
        metrics: Dict[tuple[str, str], Dict[str, int]] = {}
        stmt = (
            select(
                LogEntry.app_name,
                LogEntry.stream_id,
                LogEntry.level,
                func.count(LogEntry.id).label("count_60m"),
                func.max(LogEntry.ts).label("last_ts"),
                func.sum(case((LogEntry.ts >= t_5m_ago, 1), else_=0)).label("count_5m"),
            )
            .where(
                pair_filter,
                LogEntry.ts >= t_60m_ago,
                LogEntry.level.in_(["ERROR", "WARN"]),
            )
            .group_by(LogEntry.app_name, LogEntry.stream_id, LogEntry.level)
        )
        result = await session.execute(stmt)
        latest_ts: Dict[tuple[str, str], int] = {}
        for row in result:
            key = (row.app_name, row.stream_id)
            level_metrics = metrics.setdefault(
                key,
                {"ERROR_5m": 0, "WARN_5m": 0, "ERROR_60m": 0, "WARN_60m": 0},
            )
            level_metrics[f"{row.level}_5m"] = row.count_5m or 0
            level_metrics[f"{row.level}_60m"] = row.count_60m
            latest_ts[key] = max(latest_ts.get(key, 0), row.last_ts or 0)

        active_issue_pairs = [
            key
            for key, counts in metrics.items()
            if counts["ERROR_60m"] or counts["WARN_60m"]
        ]
        latest_logs: Dict[tuple[str, str], Dict[str, Any]] = {}
        if active_issue_pairs:
            issue_filter = or_(
                *[
                    and_(LogEntry.app_name == app_name, LogEntry.stream_id == stream_id)
                    for app_name, stream_id in active_issue_pairs
                ]
            )
            latest_timestamps = (
                select(
                    LogEntry.app_name,
                    LogEntry.stream_id,
                    func.max(LogEntry.ts).label("latest_ts"),
                )
                .where(issue_filter, LogEntry.level.in_(["ERROR", "WARN"]))
                .group_by(LogEntry.app_name, LogEntry.stream_id)
                .subquery()
            )
            latest_result = await session.execute(
                select(
                    LogEntry.id,
                    LogEntry.app_name,
                    LogEntry.stream_id,
                    LogEntry.friendly_title,
                    LogEntry.friendly_message,
                    LogEntry.action_hint,
                )
                .join(
                    latest_timestamps,
                    and_(
                        LogEntry.app_name == latest_timestamps.c.app_name,
                        LogEntry.stream_id == latest_timestamps.c.stream_id,
                        LogEntry.ts == latest_timestamps.c.latest_ts,
                    ),
                )
                .order_by(LogEntry.id.desc())
            )
            for row in latest_result:
                latest_logs.setdefault(
                    (row.app_name, row.stream_id),
                    {
                        "friendly_title": row.friendly_title,
                        "friendly_message": row.friendly_message,
                        "action_hint": row.action_hint,
                    },
                )

        reports = []
        for stream, app_name in streams:
            key = (app_name, stream.streamId)
            counts = metrics.get(
                key,
                {"ERROR_5m": 0, "WARN_5m": 0, "ERROR_60m": 0, "WARN_60m": 0},
            )
            reports.append(
                self._build_report(
                    stream,
                    app_name,
                    counts["WARN_5m"],
                    counts["ERROR_5m"],
                    counts["WARN_60m"],
                    counts["ERROR_60m"],
                    latest_ts.get(key, 0),
                    latest_logs.get(key),
                )
            )
        return reports

    async def get_problematic_streams(
        self, streams: List[BroadcastStream], app_name: str, session: AsyncSession
    ) -> List[StreamHealthReport]:
        """Return list of streams that are in 'Bermasalah' or 'Perhatian' state, ranked by severity."""
        reports = await self.evaluate_streams_health(
            [(stream, app_name) for stream in streams],
            session,
        )

        # Filter only problematic or attention-needed streams
        problematic = [r for r in reports if r.status in ("Bermasalah", "Perhatian")]
        # Sort by status severity ('Bermasalah' first), then by event count descending
        problematic.sort(
            key=lambda x: (0 if x.status == "Bermasalah" else 1, -x.total_events, -x.last_event_ts)
        )
        return problematic
