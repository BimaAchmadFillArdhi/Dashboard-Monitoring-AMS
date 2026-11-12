import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app.main as main


class _SessionContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@pytest.mark.asyncio
async def test_lifespan_stops_workers_and_disposes_resources_in_order(monkeypatch):
    stopped_workers = []
    started_workers = []

    async def worker(name):
        started_workers.append(name)
        try:
            await asyncio.Event().wait()
        finally:
            stopped_workers.append(name)

    async def dispose_engine():
        assert len(stopped_workers) == 4

    async def close_ams():
        assert len(stopped_workers) == 4

    monkeypatch.setattr(main, "init_db", AsyncMock())
    monkeypatch.setattr(main, "AsyncSessionLocal", _SessionContext)
    monkeypatch.setattr(
        main,
        "get_log_service",
        lambda: SimpleNamespace(query_logs=AsyncMock()),
    )
    monkeypatch.setattr(
        main.dashboard_svc,
        "warm_up_history_from_db",
        AsyncMock(),
    )
    engine = SimpleNamespace(dispose=AsyncMock(side_effect=dispose_engine))
    monkeypatch.setattr(main, "async_engine", engine)
    ams_client = SimpleNamespace(close=AsyncMock(side_effect=close_ams))
    monkeypatch.setattr(main, "get_ams_client", lambda: ams_client)
    monkeypatch.setattr(
        main,
        "background_log_poller",
        lambda: worker("log-poller"),
    )
    monkeypatch.setattr(
        main,
        "background_dashboard_ticker",
        lambda: worker("dashboard-ticker"),
    )
    monkeypatch.setattr(
        main,
        "background_stream_metrics_collector",
        lambda: worker("stream-metrics"),
    )
    monkeypatch.setattr(
        main,
        "background_cctv_incident_monitor",
        lambda: worker("cctv-incident-monitor"),
    )

    async with main.lifespan(main.app):
        await asyncio.sleep(0)
        assert set(started_workers) == {
            "dashboard-ticker",
            "log-poller",
            "stream-metrics",
            "cctv-incident-monitor",
        }
        existing_task = main.bg_log_task
        duplicate_worker = AsyncMock()
        assert (
            main._ensure_background_task(
                existing_task,
                duplicate_worker,
                "duplicate-log-poller",
            )
            is existing_task
        )

    assert duplicate_worker.await_count == 0
    assert sorted(stopped_workers) == sorted(started_workers)
    ams_client.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()
    assert main.bg_log_task is None
    assert main.bg_dashboard_task is None
    assert main.bg_stream_metrics_task is None
    assert main.bg_cctv_incident_task is None


@pytest.mark.asyncio
async def test_lifespan_disposes_engine_when_startup_fails(monkeypatch):
    close_ams = AsyncMock()
    dispose_engine = AsyncMock()
    monkeypatch.setattr(
        main,
        "init_db",
        AsyncMock(side_effect=RuntimeError("MySQL startup failed")),
    )
    monkeypatch.setattr(
        main,
        "async_engine",
        SimpleNamespace(dispose=dispose_engine),
    )
    monkeypatch.setattr(
        main,
        "get_ams_client",
        lambda: SimpleNamespace(close=close_ams),
    )

    with pytest.raises(RuntimeError, match="MySQL startup failed"):
        async with main.lifespan(main.app):
            pytest.fail("Lifespan must not yield after startup failure")

    close_ams.assert_awaited_once()
    dispose_engine.assert_awaited_once()


@pytest.mark.asyncio
async def test_cctv_incident_monitor_uses_shared_notification_service(monkeypatch):
    class LockConnection(_SessionContext):
        def __init__(self):
            self.lock_results = iter((1, 1))

        async def scalar(self, _statement, _parameters=None):
            return next(self.lock_results)

    class Engine:
        def connect(self):
            return LockConnection()

    service = SimpleNamespace(
        evaluate_and_notify=AsyncMock(return_value={"notified": False})
    )
    monkeypatch.setattr(main, "async_engine", Engine())
    monkeypatch.setattr(main, "AsyncSessionLocal", _SessionContext)
    monkeypatch.setattr(main, "get_cctv_incident_service", lambda: service)
    monkeypatch.setattr(
        main.settings,
        "CCTV_INCIDENT_CHECK_INTERVAL_SECONDS",
        0,
    )

    sleep_calls = 0

    async def stop_after_one_cycle(_delay):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(main.asyncio, "sleep", stop_after_one_cycle)

    with pytest.raises(asyncio.CancelledError):
        await main.background_cctv_incident_monitor()

    service.evaluate_and_notify.assert_awaited_once()
    _, notifier = service.evaluate_and_notify.await_args.args
    assert notifier is main.whatsapp_notifier
