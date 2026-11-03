import time
import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from sqlalchemy import select, asc, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.ams_client import AMSAPIError, get_ams_client
from app.clients.models import BroadcastStream
from app.db.models import AmsStreamCurrent, StreamMetricHistory
from app.services.health_service import HealthService
from app.services.log_retention import delete_expired_rows

ams_client = get_ams_client()
health_service = HealthService()
logger = logging.getLogger("ams.application_service")


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
    stream_count: Optional[int]
    live_count: Optional[int]
    vod_count: int
    storage_mb: float
    problematic_count: int


class ApplicationService:
    def __init__(self):
        self._last_saved_ts: Dict[str, float] = {}
        self._last_application_overview: Optional[List[AppSummaryDTO]] = None
        self._overview_updated_at = 0.0
        self._overview_failure_until = 0.0
        self._overview_lock = asyncio.Lock()
        self._overview_cache_seconds = 5.0
        self._overview_failure_cache_seconds = 30.0

    async def _get_cached_application_overview(
        self, session: AsyncSession
    ) -> List[AppSummaryDTO]:
        result = await session.execute(
            select(AmsStreamCurrent).order_by(
                AmsStreamCurrent.app_name, AmsStreamCurrent.stream_id
            )
        )
        streams = result.scalars().all()
        grouped: Dict[str, AppSummaryDTO] = {}
        for stream in streams:
            summary = grouped.setdefault(
                stream.app_name,
                AppSummaryDTO(
                    name=stream.app_name,
                    stream_count=0,
                    live_count=0,
                    vod_count=0,
                    storage_mb=0.0,
                    problematic_count=0,
                ),
            )
            summary.stream_count += 1
            if stream.status == "broadcasting":
                summary.live_count += 1
        return list(grouped.values())

    async def get_applications_overview(self, session: AsyncSession) -> List[AppSummaryDTO]:
        now = time.monotonic()
        if (
            self._last_application_overview is not None
            and (
                now - self._overview_updated_at < self._overview_cache_seconds
                or now < self._overview_failure_until
            )
        ):
            return self._last_application_overview

        async with self._overview_lock:
            now = time.monotonic()
            if (
                self._last_application_overview is not None
                and (
                    now - self._overview_updated_at < self._overview_cache_seconds
                    or now < self._overview_failure_until
                )
            ):
                return self._last_application_overview

            result = await self._refresh_applications_overview(session)
            self._last_application_overview = result
            self._overview_updated_at = time.monotonic()
            return result

    async def _get_stale_applications_overview(
        self, session: AsyncSession
    ) -> List[AppSummaryDTO]:
        self._overview_failure_until = (
            time.monotonic() + self._overview_failure_cache_seconds
        )
        if session is not None:
            mysql_snapshot = await self._get_cached_application_overview(session)
            if mysql_snapshot:
                return mysql_snapshot
        return list(self._last_application_overview or [])

    async def _refresh_applications_overview(
        self, session: AsyncSession
    ) -> List[AppSummaryDTO]:
        """Fetch AMS application stream counts, preserving the last valid snapshot."""
        try:
            apps_info = await ams_client.get_applications_info(strict=True)
            if not apps_info:
                logger.warning(
                    "[ApplicationService] AMS returned no application metadata; "
                    "serving the latest MySQL stream snapshot"
                )
                return await self._get_stale_applications_overview(session)
        except AMSAPIError as err:
            logger.warning(
                "[ApplicationService] Unable to refresh application metadata; "
                "serving the latest MySQL stream snapshot: %s",
                err,
            )
            return await self._get_stale_applications_overview(session)

        self._overview_failure_until = 0.0
        result = []
        cached_by_name = {}
        if session is not None:
            cached_by_name = {
                item.name: item
                for item in await self._get_cached_application_overview(session)
            }
        previous_by_name = {
            item.name: item for item in self._last_application_overview or []
        }
        problematic_by_app: Dict[str, int] = {}
        if session is not None:
            active_result = await session.execute(
                select(AmsStreamCurrent).where(
                    AmsStreamCurrent.status == "broadcasting"
                )
            )
            active_records = active_result.scalars().all()
            active_streams = [
                (
                    BroadcastStream(
                        streamId=record.stream_id,
                        name=record.name or record.stream_id,
                        status=record.status,
                        type=record.stream_type or "streamSource",
                        streamUrl=record.stream_url,
                        date=record.created_date or 0,
                        speed=record.speed,
                        bitrate=record.bitrate,
                        width=record.width,
                        height=record.height,
                        hlsViewerCount=record.hls_viewers,
                        webRTCViewerCount=record.webrtc_viewers,
                        dashViewerCount=record.dash_viewers,
                    ),
                    record.app_name,
                )
                for record in active_records
            ]
            reports = await health_service.evaluate_streams_health(
                active_streams,
                session,
            )
            for report in reports:
                if report.status in ("Bermasalah", "Perhatian"):
                    problematic_by_app[report.app_name] = (
                        problematic_by_app.get(report.app_name, 0) + 1
                    )

        for app in apps_info:
            live_count = app.liveStreamCount
            prob_count = problematic_by_app.get(app.name, 0)
            try:
                stream_count = await ams_client.get_broadcasts_count(
                    app.name, strict=True
                )
            except AMSAPIError as err:
                previous = previous_by_name.get(app.name) or cached_by_name.get(
                    app.name
                )
                logger.warning(
                    "[ApplicationService] Unable to refresh total stream count for %s; "
                    "retaining the AMS live count: %s",
                    app.name,
                    err,
                )
                if previous is not None:
                    stream_count = previous.stream_count
                else:
                    stream_count = None

            result.append(AppSummaryDTO(
                name=app.name,
                stream_count=stream_count,
                live_count=live_count,
                vod_count=app.vodCount,
                storage_mb=round(app.storage / (1024**2), 1),
                problematic_count=prob_count,
            ))

        self._last_application_overview = result
        return result

    async def get_streams_for_app(
        self,
        app_name: str,
        session: AsyncSession,
        offset: int = 0,
        size: int = 50,
        search_q: Optional[str] = None,
    ) -> tuple[List[StreamTableRowDTO], int, int, int]:
        """Return one page of all AMS streams from the latest complete MySQL snapshot."""
        filters = [AmsStreamCurrent.app_name == app_name]
        if search_q and search_q.strip():
            query = search_q.strip()
            filters.append(
                AmsStreamCurrent.name.contains(query, autoescape=True)
                | AmsStreamCurrent.stream_id.contains(query, autoescape=True)
            )

        counts = (
            await session.execute(
                select(
                    func.count().label("total"),
                    func.coalesce(
                        func.sum(
                            case(
                                (AmsStreamCurrent.status == "broadcasting", 1),
                                else_=0,
                            )
                        ),
                        0,
                    ).label("live"),
                )
                .select_from(AmsStreamCurrent)
                .where(*filters)
            )
        ).one()
        total = counts.total
        live_count = counts.live
        offline_count = total - live_count
        result = await session.execute(
            select(AmsStreamCurrent)
            .where(*filters)
            .order_by(AmsStreamCurrent.stream_id)
            .offset(offset)
            .limit(size)
        )
        records = result.scalars().all()
        raw_streams = [
            BroadcastStream(
                streamId=record.stream_id,
                name=record.name or record.stream_id,
                status=record.status,
                type=record.stream_type or "streamSource",
                streamUrl=record.stream_url,
                date=record.created_date or 0,
                speed=record.speed,
                bitrate=record.bitrate,
                width=record.width,
                height=record.height,
                hlsViewerCount=record.hls_viewers,
                webRTCViewerCount=record.webrtc_viewers,
                dashViewerCount=record.dash_viewers,
                mp4Enabled=0,
            )
            for record in records
        ]
        health_reports = await health_service.evaluate_streams_health(
            [(stream, app_name) for stream in raw_streams],
            session,
        )
        health_by_id = {report.stream_id: report for report in health_reports}

        rows = []
        for stream in raw_streams:
            report = health_by_id[stream.streamId]
            date_str = "-"
            if stream.date > 0:
                try:
                    date_str = datetime.fromtimestamp(
                        stream.date / 1000
                    ).strftime("%Y-%m-%d %H:%M")
                except (OverflowError, OSError, ValueError):
                    logger.warning(
                        "Invalid creation timestamp for stream %s",
                        stream.streamId,
                    )

            rows.append(
                StreamTableRowDTO(
                    stream_id=stream.streamId,
                    name=stream.name or stream.streamId,
                    status=stream.status,
                    health_status=report.status,
                    health_tone=report.tone,
                    problem_summary=report.problem_summary,
                    stream_type=stream.type,
                    masked_url=stream.masked_url,
                    bitrate=stream.bitrate,
                    speed=stream.speed,
                    total_viewers=stream.total_viewers,
                    resolution_str=stream.resolution_str,
                    created_date_formatted=date_str,
                    mp4_enabled=bool(stream.mp4Enabled),
                )
            )

        return rows, total, live_count, offline_count

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
        """Persist stream metric snapshot to the configured database."""
        key = f"{app_name}:{stream_id}"
        now = time.monotonic()
        last_ts = self._last_saved_ts.get(key, 0.0)
        # The collector and drawer SSE both sample at two-second intervals.
        if not force and (now - last_ts < 2.0):
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
            logger.exception(
                "[ApplicationService] Failed to save metrics for %s/%s",
                app_name,
                stream_id,
            )
            return False

    async def get_stream_metric_history(
        self,
        session: AsyncSession,
        app_name: str,
        stream_id: str,
        hours: int = 1,
    ) -> Dict[str, Any]:
        """Query the latest stream metric history for chart display."""
        cutoff_ms = int((time.time() - (hours * 3600)) * 1000)
        stmt = (
            select(StreamMetricHistory)
            .where(
                StreamMetricHistory.app_name == app_name,
                StreamMetricHistory.stream_id == stream_id,
                StreamMetricHistory.ts >= cutoff_ms,
            )
            .order_by(StreamMetricHistory.ts.desc(), StreamMetricHistory.id.desc())
            .limit(600)
        )
        res = await session.execute(stmt)
        records = list(reversed(res.scalars().all()))

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
        if retention_days <= 0:
            return 0
        cutoff_ms = int((time.time() - (retention_days * 86400)) * 1000)
        deleted = await delete_expired_rows(
            session,
            StreamMetricHistory,
            StreamMetricHistory.ts,
            cutoff_ms,
        )
        await session.commit()
        return deleted



_application_service: Optional[ApplicationService] = None


def get_application_service() -> ApplicationService:
    global _application_service
    if _application_service is None:
        _application_service = ApplicationService()
    return _application_service
