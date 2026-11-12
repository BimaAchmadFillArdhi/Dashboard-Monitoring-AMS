import asyncio
import logging
import re
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Any, Callable, Coroutine
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, desc, text
from sqlalchemy.engine import make_url

from app.config import get_settings
from app.db.session import init_db, AsyncSessionLocal, async_engine
from app.db.models import LogEntry
from app.web.routes import router as web_router
from app.api.logs_api import (
    router as logs_api_router,
    log_broadcaster,
    whatsapp_bulk_tasks,
)
from app.api.dashboard_api import router as dashboard_api_router, dashboard_broadcaster
from app.api.applications_api import router as applications_api_router
from app.services.dashboard_service import get_dashboard_service
from app.services.application_service import get_application_service
from app.services.log_service import get_log_service
from app.services.sync_service import get_sync_service
from app.clients.ams_client import get_ams_client
from app.ingest.log_ingestor import LogIngestor
from app.services.whatsapp_notifier import WhatsAppNotifier
from app.services.cctv_incident_service import (
    CCTV_INCIDENT_LOCK_NAME,
    get_cctv_incident_service,
)

logger = logging.getLogger("ams.main")
settings = get_settings()
database_name = make_url(settings.DATABASE_URL).database or "default"
log_ingest_lock_name = (
    "ams_log_ingest_"
    + re.sub(r"[^A-Za-z0-9_]", "_", database_name)[:48]
)
bg_log_task: asyncio.Task | None = None
bg_dashboard_task: asyncio.Task | None = None
bg_stream_metrics_task: asyncio.Task | None = None
bg_cctv_incident_task: asyncio.Task | None = None
dashboard_svc = get_dashboard_service()
app_svc = get_application_service()
sync_svc = get_sync_service()
whatsapp_notifier = WhatsAppNotifier()


async def background_log_poller():
    """Background task polling AMS server logs periodically and broadcasting to SSE."""
    await asyncio.sleep(2.0)
    ingestor = LogIngestor()
    last_cleanup_time = 0.0

    logger.info(f"[BackgroundPoller] Started log poller (interval={settings.POLL_INTERVAL_SECONDS}s)")

    while True:
        try:
            async with async_engine.connect() as lock_connection:
                acquired = await lock_connection.scalar(
                    text("SELECT GET_LOCK(:lock_name, 0)"),
                    {"lock_name": log_ingest_lock_name},
                )
                if acquired == 1:
                    try:
                        async with AsyncSessionLocal() as session:
                            total_new_logs = 0
                            for _ in range(5):
                                count = await ingestor.poll_server_logs(
                                    session, log_type="server"
                                )
                                total_new_logs += count
                                if count == 0:
                                    break

                            if total_new_logs > 0:
                                stmt = (
                                    select(
                                        LogEntry.id,
                                        LogEntry.ts,
                                        LogEntry.level,
                                        LogEntry.logger,
                                        LogEntry.stream_id,
                                        LogEntry.app_name,
                                        LogEntry.friendly_title,
                                        LogEntry.friendly_message,
                                        LogEntry.category,
                                        LogEntry.action_hint,
                                        LogEntry.severity_group,
                                        LogEntry.fingerprint,
                                    )
                                    .order_by(desc(LogEntry.id))
                                    .limit(min(total_new_logs, 50))
                                )
                                res = await session.execute(stmt)
                                new_logs = res.mappings().all()
                                for log in reversed(new_logs):
                                    dt = datetime.fromtimestamp(log["ts"] / 1000)
                                    log_broadcaster.broadcast({
                                        "id": log["id"],
                                        "ts": log["ts"],
                                        "time_formatted": dt.strftime("%H:%M:%S"),
                                        "level": log["level"],
                                        "logger": log["logger"],
                                        "stream_id": log["stream_id"],
                                        "app_name": log["app_name"],
                                        "friendly_title": log["friendly_title"],
                                        "friendly_message": log["friendly_message"],
                                        "category": log["category"],
                                        "action_hint": log["action_hint"],
                                        "severity_group": log["severity_group"],
                                        "fingerprint": log["fingerprint"],
                                    })

                            now = asyncio.get_event_loop().time()
                            if now - last_cleanup_time > 86400:
                                await ingestor.cleanup_old_logs(session)
                                await sync_svc.cleanup_old_logs(session)
                                last_cleanup_time = now
                    finally:
                        released = await lock_connection.scalar(
                            text("SELECT RELEASE_LOCK(:lock_name)"),
                            {"lock_name": log_ingest_lock_name},
                        )
                        if released != 1:
                            logger.error(
                                "[BackgroundPoller] MySQL ingestion lock was not released"
                            )

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
    metric_retention_days = min(settings.METRIC_RETENTION_DAYS, 7)

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

                    now = asyncio.get_event_loop().time()
                    if now - last_metric_cleanup > 86400:
                        await dashboard_svc.cleanup_old_metrics(
                            session, retention_days=metric_retention_days
                        )
                        await app_svc.cleanup_old_stream_metrics(
                            session, retention_days=metric_retention_days
                        )
                        last_metric_cleanup = now

        except asyncio.CancelledError:
            break
        except Exception as err:
            logger.error(f"[DashboardTicker] Error: {err}")

        await asyncio.sleep(3.0)


async def background_stream_metrics_collector():
    """Background task polling all AMS stream metrics and persisting to the configured database."""
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


async def background_cctv_incident_monitor():
    """Track persistent CCTV incidents from MySQL snapshots and send due reports."""
    interval = settings.CCTV_INCIDENT_CHECK_INTERVAL_SECONDS
    await asyncio.sleep(min(5, interval))
    logger.info(
        "[CCTVIncidents] Monitor started (interval=%ss, reminder=%ss)",
        interval,
        settings.CCTV_INCIDENT_REPEAT_INTERVAL_SECONDS,
    )
    incident_service = get_cctv_incident_service()
    while True:
        try:
            async with async_engine.connect() as lock_connection:
                acquired = await lock_connection.scalar(
                    text("SELECT GET_LOCK(:lock_name, 0)"),
                    {"lock_name": CCTV_INCIDENT_LOCK_NAME},
                )
                if acquired == 1:
                    try:
                        async with AsyncSessionLocal() as session:
                            result = await incident_service.evaluate_and_notify(
                                session,
                                whatsapp_notifier,
                            )
                        if result.get("notified"):
                            logger.info(
                                "[CCTVIncidents] Batch %s completed: "
                                "status=%s sent=%s failed=%s",
                                result.get("batch_id"),
                                result.get("status"),
                                result.get("sent"),
                                result.get("failed"),
                            )
                    finally:
                        released = await lock_connection.scalar(
                            text("SELECT RELEASE_LOCK(:lock_name)"),
                            {"lock_name": CCTV_INCIDENT_LOCK_NAME},
                        )
                        if released != 1:
                            logger.error(
                                "[CCTVIncidents] MySQL notification lock "
                                "was not released"
                            )
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("[CCTVIncidents] Incident monitor cycle failed")

        await asyncio.sleep(interval)


def _ensure_background_task(
    task: asyncio.Task | None,
    worker: Callable[[], Coroutine[Any, Any, None]],
    task_name: str,
) -> asyncio.Task[None]:
    if task is not None and not task.done():
        return task
    return asyncio.create_task(worker(), name=task_name)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Startup: Initialize database schema and warm caches before starting workers.
        await init_db()
        print(f"[{settings.APP_NAME}] MySQL database schema initialized successfully.")

        async with AsyncSessionLocal() as session:
            try:
                await get_log_service().query_logs(session=session, limit=1)
                logger.info("[Startup] Log statistics cache warmed.")
            except Exception as err:
                logger.warning(
                    "[Startup] Could not warm the log statistics cache: %s",
                    err,
                )

        async with AsyncSessionLocal() as session:
            try:
                await dashboard_svc.warm_up_history_from_db(session)
                print(
                    f"[{settings.APP_NAME}] Dashboard metrics buffer warmed up "
                    "from database."
                )
            except Exception as err:
                logger.warning(
                    "[Startup] Could not warm dashboard metrics from MySQL: %s",
                    err,
                )

        # Reuse active tasks if lifespan startup is invoked more than once.
        global bg_log_task, bg_dashboard_task, bg_stream_metrics_task
        global bg_cctv_incident_task
        bg_log_task = _ensure_background_task(
            bg_log_task, background_log_poller, "ams-log-poller"
        )
        bg_dashboard_task = _ensure_background_task(
            bg_dashboard_task, background_dashboard_ticker, "ams-dashboard-ticker"
        )
        bg_stream_metrics_task = _ensure_background_task(
            bg_stream_metrics_task,
            background_stream_metrics_collector,
            "ams-stream-metrics-collector",
        )
        bg_cctv_incident_task = _ensure_background_task(
            bg_cctv_incident_task,
            background_cctv_incident_monitor,
            "ams-cctv-incident-monitor",
        )

        yield
    finally:
        tasks = (
            bg_log_task,
            bg_dashboard_task,
            bg_stream_metrics_task,
            bg_cctv_incident_task,
        )
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()

        if tasks:
            await asyncio.gather(
                *(task for task in tasks if task is not None),
                return_exceptions=True,
            )

        if whatsapp_bulk_tasks:
            await asyncio.gather(*tuple(whatsapp_bulk_tasks), return_exceptions=True)

        bg_log_task = None
        bg_dashboard_task = None
        bg_stream_metrics_task = None
        bg_cctv_incident_task = None
        try:
            await get_ams_client().close()
        finally:
            await async_engine.dispose()


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
