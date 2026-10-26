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
from app.ingest.log_ingestor import LogIngestor

logger = logging.getLogger("ams.main")
settings = get_settings()

bg_log_task: asyncio.Task | None = None
bg_dashboard_task: asyncio.Task | None = None
dashboard_svc = get_dashboard_service()


async def background_log_poller():
    """Background task polling AMS server logs periodically and broadcasting to SSE."""
    await asyncio.sleep(2.0)
    ingestor = LogIngestor()
    last_cleanup_time = 0.0

    logger.info(f"[BackgroundPoller] Started log poller (interval={settings.POLL_INTERVAL_SECONDS}s)")

    while True:
        try:
            async with AsyncSessionLocal() as session:
                count = await ingestor.poll_server_logs(session, log_type="server")
                if count > 0:
                    stmt = select(LogEntry).order_by(desc(LogEntry.id)).limit(min(count, 50))
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
    """Periodically fetch dashboard metrics and broadcast via SSE every 3 seconds."""
    await asyncio.sleep(1.0)
    logger.info("[DashboardTicker] Started metrics broadcast loop (3s interval)")

    while True:
        try:
            snapshot = await dashboard_svc.collect_snapshot()
            dashboard_broadcaster.broadcast(snapshot.model_dump())
        except asyncio.CancelledError:
            break
        except Exception as err:
            logger.error(f"[DashboardTicker] Error: {err}")

        await asyncio.sleep(3.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize database schema
    await init_db()
    print(f"[{settings.APP_NAME}] Database initialized with SQLite WAL.")

    # Start background tasks
    global bg_log_task, bg_dashboard_task
    bg_log_task = asyncio.create_task(background_log_poller())
    bg_dashboard_task = asyncio.create_task(background_dashboard_ticker())

    yield

    # Shutdown
    if bg_log_task:
        bg_log_task.cancel()
    if bg_dashboard_task:
        bg_dashboard_task.cancel()

    await asyncio.gather(bg_log_task, bg_dashboard_task, return_exceptions=True)

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
