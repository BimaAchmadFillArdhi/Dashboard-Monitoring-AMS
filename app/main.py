import asyncio
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, desc

from app.config import get_settings
from app.db.session import init_db, AsyncSessionLocal
from app.db.models import LogEntry
from app.web.routes import router as web_router
from app.api.logs_api import router as logs_api_router, log_broadcaster
from app.api.dashboard_api import router as dashboard_api_router, dashboard_broadcaster
from app.api.applications_api import router as applications_api_router
from app.services.dashboard_service import get_dashboard_service
from app.services.application_service import get_application_service
from app.clients.ams_client import get_ams_client
from app.ingest.log_ingestor import LogIngestor

logger = logging.getLogger("ams.main")
settings = get_settings()

bg_log_task: asyncio.Task | None = None
bg_dashboard_task: asyncio.Task | None = None
bg_stream_metrics_task: asyncio.Task | None = None
dashboard_svc = get_dashboard_service()
app_svc = get_application_service()
ams_cli = get_ams_client()


async def background_log_poller():
    """Background task polling AMS server logs periodically and broadcasting to SSE."""
    await asyncio.sleep(2.0)
    ingestor = LogIngestor()
    last_cleanup_time = 0.0

    logger.info(f"[BackgroundPoller] Started log poller (interval={settings.POLL_INTERVAL_SECONDS}s)")

    while True:
        try:
            async with AsyncSessionLocal() as session:
                # Catch up on log chunks (up to 5 chunks per polling cycle)
                total_new_logs = 0
                for _ in range(5):
                    count = await ingestor.poll_server_logs(session, log_type="server")
                    total_new_logs += count
                    if count == 0:
                        break

                if total_new_logs > 0:
                    stmt = select(LogEntry).order_by(desc(LogEntry.id)).limit(min(total_new_logs, 50))
                    res = await session.execute(stmt)
                    new_logs = res.scalars().all()
                    for log in reversed(new_logs):
                        dt = datetime.fromtimestamp(log.ts / 1000)
                        log_broadcaster.broadcast({
                            "id": log.id,
                            "ts": log.ts,
                            "time_formatted": dt.strftime("%H:%M:%S"),
                            "level": log.level,
                            "stream_id": log.stream_id,
                            "app_name": log.app_name,
                            "friendly_title": log.friendly_title,
                            "friendly_message": log.friendly_message,
                            "action_hint": log.action_hint,
                            "severity_group": log.severity_group,
                            "raw_message": log.raw_message,
                            "fingerprint": log.fingerprint,
                        })

                now = asyncio.get_event_loop().time()
                if now - last_cleanup_time > 86400:
                    await ingestor.cleanup_old_logs(session)
                    last_cleanup_time = now

        except asyncio.CancelledError:
            break
        except Exception as err:
            logger.error(f"[BackgroundPoller] Error: {err}")

        await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)


async def background_dashboard_ticker():
    """Periodically fetch dashboard metrics, broadcast via SSE every 3 seconds, and persist metrics history."""
    await asyncio.sleep(1.0)
    logger.info("[DashboardTicker] Started metrics broadcast loop (3s interval)")
    ticks = 0
    last_metric_cleanup = 0.0

    while True:
        try:
            snapshot = await dashboard_svc.collect_snapshot()
            dashboard_broadcaster.broadcast(snapshot.model_dump())

            ticks += 1
            # Every 10 ticks (~30s), persist snapshot to database for historical charts
            if ticks >= 10:
                ticks = 0
                async with AsyncSessionLocal() as session:
                    await dashboard_svc.save_snapshot_to_history(session, snapshot)

                    # Snapshot active streams for historical per-stream charts
                    try:
                        apps = await ams_cli.get_applications()
                        for app_name in apps:
                            streams = await ams_cli.get_broadcasts_list(app_name, offset=0, size=50)
                            for s in streams:
                                if s.status == "broadcasting":
                                    detail = {
                                        "status": s.status,
                                        "bitrate": s.bitrate or 0,
                                        "bitrate_mbps": round((s.bitrate or 0) / 1_000_000.0, 2),
                                        "speed": round(s.speed, 2) if s.speed is not None else 0.0,
                                        "total_viewers": s.total_viewers,
                                        "hls_viewers": s.hlsViewerCount,
                                        "webrtc_viewers": s.webRTCViewerCount,
                                        "dash_viewers": s.dashViewerCount,
                                    }
                                    await app_svc.save_stream_metric(session, app_name, s.streamId, detail)
                    except Exception as stream_err:
                        logger.warning(f"[DashboardTicker] Error snapshotting active streams: {stream_err}")

                    now = asyncio.get_event_loop().time()
                    if now - last_metric_cleanup > 86400:
                        await dashboard_svc.cleanup_old_metrics(session, retention_days=7)
                        await app_svc.cleanup_old_stream_metrics(session, retention_days=7)
                        last_metric_cleanup = now

        except asyncio.CancelledError:
            break
        except Exception as err:
            logger.error(f"[DashboardTicker] Error: {err}")

        await asyncio.sleep(3.0)


async def background_stream_metrics_collector():
    """Background task polling all AMS stream metrics every 5s and persisting to SQLite."""
    await asyncio.sleep(2.0)
    interval = settings.STREAM_METRICS_INTERVAL_SECONDS
    logger.info(f"[StreamCollector] Started background stream metrics collector loop ({interval}s interval)")

    while True:
        try:
            async with AsyncSessionLocal() as session:
                await app_svc.collect_all_streams_metrics(session)
        except asyncio.CancelledError:
            break
        except Exception as err:
            logger.warning(f"[StreamCollector] Error collecting stream metrics: {err}")

        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize database schema
    await init_db()
    print(f"[{settings.APP_NAME}] Database initialized with SQLite WAL.")

    # Warm up in-memory dashboard metric buffer from database
    try:
        async with AsyncSessionLocal() as session:
            await dashboard_svc.warm_up_history_from_db(session)
            print(f"[{settings.APP_NAME}] Dashboard metrics buffer warmed up from database.")
    except Exception as e:
        logger.warning(f"Failed to warm up dashboard metrics from DB: {e}")

    # Start background tasks
    global bg_log_task, bg_dashboard_task, bg_stream_metrics_task
    bg_log_task = asyncio.create_task(background_log_poller())
    bg_dashboard_task = asyncio.create_task(background_dashboard_ticker())
    bg_stream_metrics_task = asyncio.create_task(background_stream_metrics_collector())

    yield

    # Shutdown
    if bg_log_task:
        bg_log_task.cancel()
    if bg_dashboard_task:
        bg_dashboard_task.cancel()
    if bg_stream_metrics_task:
        bg_stream_metrics_task.cancel()

    await asyncio.gather(bg_log_task, bg_dashboard_task, bg_stream_metrics_task, return_exceptions=True)


app = FastAPI(
    title=settings.APP_NAME,
    debug=settings.DEBUG,
    lifespan=lifespan
)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Mount routes
app.include_router(web_router)
app.include_router(logs_api_router)
app.include_router(dashboard_api_router)
app.include_router(applications_api_router)
