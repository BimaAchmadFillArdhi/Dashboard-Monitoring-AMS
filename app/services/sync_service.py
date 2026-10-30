import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import select, desc, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.ams_client import get_ams_client
from app.clients.models import mask_rtsp_url
from app.db.models import (
    AmsSyncLog,
    AmsStreamCurrent,
    AmsStreamStatusHistory,
    StreamMetricHistory,
    ServerMetricHistory,
)
from app.services.dashboard_service import get_dashboard_service

logger = logging.getLogger("ams.sync_service")


class SyncService:
    def __init__(self):
        self.ams_client = get_ams_client()
        self.dashboard_service = get_dashboard_service()

    async def sync_all_applications_streams(self, session: AsyncSession) -> Dict[str, Any]:
        """
        Synchronize all streams from all applications using full pagination.
        Saves current state, detects status transitions, logs historical telemetry,
        and records sync audit logs to MySQL.
        """
        sync_start_t = time.monotonic()
        total_items_synced = 0
        apps_synced = []
        now_dt = datetime.utcnow()
        now_ms = int(time.time() * 1000)

        try:
            apps = await self.ams_client.get_applications()
        except Exception as e:
            logger.error(f"[SyncService] Failed to retrieve applications from AMS: {e}")
            err_log = AmsSyncLog(
                app_name="ALL",
                sync_type="streams",
                status="FAILED",
                items_synced=0,
                duration_ms=int((time.monotonic() - sync_start_t) * 1000),
                error_message=f"Failed to fetch application list: {str(e)}",
                created_at=now_dt,
            )
            session.add(err_log)
            await session.commit()
            return {"status": "FAILED", "error": str(e), "total_synced": 0}

        for app_name in apps:
            app_start_t = time.monotonic()
            try:
                # 1. Fetch all broadcasts using pagination loop
                streams = await self.ams_client.get_all_broadcasts(app_name, batch_size=50)

                # 2. Query existing streams for this app to detect status changes
                stmt = select(AmsStreamCurrent).where(AmsStreamCurrent.app_name == app_name)
                res = await session.execute(stmt)
                existing_records = {r.stream_id: r for r in res.scalars().all()}

                for s in streams:
                    sid = s.streamId
                    if not sid:
                        continue

                    new_status = s.status or "finished"
                    existing_entry = existing_records.get(sid)

                    # 3. Detect status transition
                    if existing_entry is not None:
                        if existing_entry.status != new_status:
                            # Status changed! Record into status history
                            status_hist = AmsStreamStatusHistory(
                                app_name=app_name,
                                stream_id=sid,
                                old_status=existing_entry.status,
                                new_status=new_status,
                                details=f"Perubahan status dari {existing_entry.status} ke {new_status}",
                                ts=now_ms,
                                created_at=now_dt,
                            )
                            session.add(status_hist)

                        # Update existing entry fields
                        existing_entry.name = s.name or sid
                        existing_entry.status = new_status
                        existing_entry.stream_type = s.type
                        existing_entry.stream_url = s.streamUrl
                        existing_entry.masked_url = mask_rtsp_url(s.streamUrl)
                        existing_entry.bitrate = s.bitrate or 0
                        existing_entry.speed = round(s.speed, 2) if s.speed is not None else 0.0
                        existing_entry.total_viewers = s.total_viewers
                        existing_entry.hls_viewers = s.hlsViewerCount or 0
                        existing_entry.webrtc_viewers = s.webRTCViewerCount or 0
                        existing_entry.dash_viewers = s.dashViewerCount or 0
                        existing_entry.width = s.width or 0
                        existing_entry.height = s.height or 0
                        existing_entry.created_date = s.date
                        existing_entry.last_seen = now_dt
                    else:
                        # First time seeing this stream: create current record & initial status log
                        current_entry = AmsStreamCurrent(
                            app_name=app_name,
                            stream_id=sid,
                            name=s.name or sid,
                            status=new_status,
                            stream_type=s.type,
                            stream_url=s.streamUrl,
                            masked_url=mask_rtsp_url(s.streamUrl),
                            bitrate=s.bitrate or 0,
                            speed=round(s.speed, 2) if s.speed is not None else 0.0,
                            total_viewers=s.total_viewers,
                            hls_viewers=s.hlsViewerCount or 0,
                            webrtc_viewers=s.webRTCViewerCount or 0,
                            dash_viewers=s.dashViewerCount or 0,
                            width=s.width or 0,
                            height=s.height or 0,
                            created_date=s.date,
                            last_seen=now_dt,
                        )
                        session.add(current_entry)

                        initial_status_hist = AmsStreamStatusHistory(
                            app_name=app_name,
                            stream_id=sid,
                            old_status=None,
                            new_status=new_status,
                            details="Stream terdaftar pada sistem (initial discovery)",
                            ts=now_ms,
                            created_at=now_dt,
                        )
                        session.add(initial_status_hist)

                    # 4. Save telemetry point for time-series charts
                    metric = StreamMetricHistory(
                        ts=now_ms,
                        app_name=app_name,
                        stream_id=sid,
                        status=new_status,
                        bitrate=s.bitrate or 0,
                        bitrate_mbps=round((s.bitrate or 0) / 1_000_000.0, 2),
                        speed=round(s.speed, 2) if s.speed is not None else 0.0,
                        total_viewers=s.total_viewers,
                        hls_viewers=s.hlsViewerCount or 0,
                        webrtc_viewers=s.webRTCViewerCount or 0,
                        dash_viewers=s.dashViewerCount or 0,
                        created_at=now_dt,
                    )
                    session.add(metric)

                app_dur_ms = int((time.monotonic() - app_start_t) * 1000)
                app_log = AmsSyncLog(
                    app_name=app_name,
                    sync_type="streams",
                    status="SUCCESS",
                    items_synced=len(streams),
                    duration_ms=app_dur_ms,
                    error_message=None,
                    created_at=now_dt,
                )
                session.add(app_log)
                total_items_synced += len(streams)
                apps_synced.append(app_name)

            except Exception as app_err:
                app_dur_ms = int((time.monotonic() - app_start_t) * 1000)
                logger.warning(f"[SyncService] Error syncing streams for {app_name}: {app_err}")
                err_log = AmsSyncLog(
                    app_name=app_name,
                    sync_type="streams",
                    status="FAILED",
                    items_synced=0,
                    duration_ms=app_dur_ms,
                    error_message=str(app_err),
                    created_at=now_dt,
                )
                session.add(err_log)

        # Commit all updates and logs
        await session.commit()

        total_dur_ms = int((time.monotonic() - sync_start_t) * 1000)
        return {
            "status": "SUCCESS",
            "total_synced": total_items_synced,
            "apps_synced": apps_synced,
            "duration_ms": total_dur_ms,
        }

    async def get_sync_logs(
        self, session: AsyncSession, limit: int = 50, app_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve recent API synchronization logs."""
        query = select(AmsSyncLog).order_by(desc(AmsSyncLog.created_at)).limit(limit)
        if app_name and app_name != "ALL":
            query = query.where(AmsSyncLog.app_name == app_name)
        res = await session.execute(query)
        logs = res.scalars().all()
        return [
            {
                "id": log.id,
                "app_name": log.app_name,
                "sync_type": log.sync_type,
                "status": log.status,
                "items_synced": log.items_synced,
                "duration_ms": log.duration_ms,
                "error_message": log.error_message,
                "created_at": log.created_at.strftime("%Y-%m-%d %H:%M:%S") if log.created_at else "-",
            }
            for log in logs
        ]

    async def get_status_history(
        self,
        session: AsyncSession,
        limit: int = 50,
        app_name: Optional[str] = None,
        stream_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve stream status transition audit logs."""
        query = select(AmsStreamStatusHistory).order_by(desc(AmsStreamStatusHistory.ts)).limit(limit)
        filters = []
        if app_name and app_name != "ALL":
            filters.append(AmsStreamStatusHistory.app_name == app_name)
        if stream_id:
            filters.append(AmsStreamStatusHistory.stream_id == stream_id)
        if filters:
            query = query.where(and_(*filters))

        res = await session.execute(query)
        rows = res.scalars().all()
        return [
            {
                "id": r.id,
                "app_name": r.app_name,
                "stream_id": r.stream_id,
                "old_status": r.old_status or "Baru",
                "new_status": r.new_status,
                "details": r.details or "-",
                "timestamp": datetime.fromtimestamp(r.ts / 1000).strftime("%Y-%m-%d %H:%M:%S") if r.ts else "-",
            }
            for r in rows
        ]


_sync_service: Optional[SyncService] = None


def get_sync_service() -> SyncService:
    global _sync_service
    if _sync_service is None:
        _sync_service = SyncService()
    return _sync_service
