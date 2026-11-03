import asyncio
import json
import httpx
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from pathlib import Path

from app.clients.models import (
    AMSVersion,
    SystemStatus,
    CpuUsage,
    SystemMemoryInfo,
    JvmMemoryUsage,
    FileSystemInfo,
    SystemResources,
    ApplicationItem,
    BroadcastStream,
    BroadcastStatistics,
    VoDItem,
    LogFileChunk,
    mask_rtsp_url,
)
from app.clients.ams_client import (
    AMSAPIError,
    CircuitBreaker,
    CircuitBreakerState,
    AMSClient,
)

SAMPLES_DIR = Path(__file__).resolve().parent.parent / "docs" / "ams_samples"


def test_mask_rtsp_url():
    raw_rtsp = "rtsp://admin:superSecret123@192.168.1.100:554/live/ch0"
    masked = mask_rtsp_url(raw_rtsp)
    assert masked == "rtsp://admin:****@192.168.1.100:554/live/ch0"
    assert "superSecret123" not in masked

    raw_no_auth = "rtsp://cctv.jogjakota.go.id:1935/atcs/stream1"
    assert mask_rtsp_url(raw_no_auth) == raw_no_auth
    assert mask_rtsp_url(None) == ""


def test_models_parse_from_real_samples():
    # 1. Version
    with open(SAMPLES_DIR / "version.json", "r", encoding="utf-8") as f:
        version_data = json.load(f)
    v = AMSVersion(**version_data)
    assert v.versionName == "3.1.0"
    assert v.versionType == "Community Edition"

    # 2. System Resources
    with open(SAMPLES_DIR / "system_resources.json", "r", encoding="utf-8") as f:
        sys_data = json.load(f)
    sr = SystemResources(**sys_data)
    assert sr.cpuUsage is not None
    assert sr.cpuUsage.systemCPULoad >= 0
    assert sr.jvmMemoryUsage is not None
    assert sr.jvmMemoryUsage.maxMemory > 0
    assert sr.systemMemoryInfo is not None
    assert sr.fileSystemInfo is not None
    assert sr.server_timing is not None
    assert "d" in sr.server_timing.uptime_formatted or "h" in sr.server_timing.uptime_formatted

    # 3. Applications Info
    with open(SAMPLES_DIR / "applications_info.json", "r", encoding="utf-8") as f:
        apps_data = json.load(f)
    apps = [ApplicationItem(**item) for item in apps_data]
    assert len(apps) > 0
    assert any(a.name == "atcs" for a in apps)

    # 4. Broadcasts List & Single Stream
    with open(SAMPLES_DIR / "broadcasts_list_atcs.json", "r", encoding="utf-8") as f:
        streams_data = json.load(f)
    streams = [BroadcastStream(**item) for item in streams_data]
    assert len(streams) > 0
    s0 = streams[0]
    assert s0.streamId != ""
    assert s0.type == "streamSource"
    assert s0.status in ("broadcasting", "finished", "created")
    assert s0.originAddress
    assert s0.originAdress == s0.originAddress
    assert s0.date > 0
    assert "absoluteStartTimeMs" in (s0.model_extra or {})

    # 5. Broadcast Statistics
    with open(SAMPLES_DIR / "broadcast_total_statistics_atcs.json", "r", encoding="utf-8") as f:
        stat_data = json.load(f)
    stats = BroadcastStatistics(**stat_data)
    assert stats.activeLiveStreamCount >= 0

    # 6. Log File Chunk
    with open(SAMPLES_DIR / "log_file_sample.json", "r", encoding="utf-8") as f:
        log_data = json.load(f)
    log_chunk = LogFileChunk(**log_data)
    assert log_chunk.logFileSize > 0
    assert len(log_chunk.logContent) > 0

    # 7. SystemStatus & VoDItem
    stat = SystemStatus(osName="Linux", osArch="amd64", javaVersion="21", processorCount=24)
    assert stat.osName == "Linux"
    assert stat.processorCount == 24

    vod = VoDItem(vodId="v1", vodName="test.mp4", duration=65000, fileSize=10485760)
    assert vod.file_size_mb == 10.0
    assert vod.duration_formatted == "1m 5s"


def test_circuit_breaker_transitions():
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.2)
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.can_attempt() is True

    # 2 failures -> still CLOSED
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED

    # 3rd failure -> trips to OPEN
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.can_attempt() is False

    # Wait for recovery timeout
    import time
    time.sleep(0.25)
    assert cb.can_attempt() is True
    assert cb.state == CircuitBreakerState.HALF_OPEN

    # Success restores to CLOSED
    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.failure_count == 0


@pytest.mark.asyncio
async def test_get_all_broadcasts_paginates_without_count_endpoint(monkeypatch):
    client = AMSClient()
    all_streams = [
        BroadcastStream(streamId=f"stream-{index}", status="broadcasting" if index % 2 else "finished")
        for index in range(239)
    ]
    requested_offsets = []

    async def get_page(app_name, offset, size, *, strict=False):
        requested_offsets.append(offset)
        return all_streams[offset:offset + size]

    monkeypatch.setattr(client, "get_broadcasts_list", get_page)
    monkeypatch.setattr(
        client,
        "get_broadcasts_count",
        AsyncMock(side_effect=AssertionError("count endpoint must not determine completeness")),
    )

    streams = await client.get_all_broadcasts("atcs-kota", batch_size=50)

    assert len(streams) == 239
    assert sum(stream.status == "broadcasting" for stream in streams) == 119
    assert requested_offsets == [0, 50, 100, 150, 200]


@pytest.mark.asyncio
async def test_broadcast_api_failures_raise_instead_of_returning_empty_data(monkeypatch):
    client = AMSClient()
    monkeypatch.setattr(client, "request", AsyncMock(return_value=None))
    monkeypatch.setattr(client, "proxy_request", AsyncMock(return_value=None))

    with pytest.raises(AMSAPIError):
        await client.get_applications_info()
    with pytest.raises(AMSAPIError):
        await client.get_broadcasts_count("atcs")
    with pytest.raises(AMSAPIError):
        await client.get_broadcasts_list("atcs")

    monkeypatch.setattr(client, "request", AsyncMock(return_value=SimpleNamespace(json=lambda: {})))
    monkeypatch.setattr(client, "proxy_request", AsyncMock(return_value=SimpleNamespace(json=lambda: {})))

    with pytest.raises(AMSAPIError):
        await client.get_applications_info()
    with pytest.raises(AMSAPIError):
        await client.get_broadcasts_count("atcs")
    with pytest.raises(AMSAPIError):
        await client.get_broadcasts_list("atcs")


@pytest.mark.asyncio
async def test_get_retries_transient_http_500_with_exponential_backoff(monkeypatch):
    client = AMSClient()
    client._authenticated = True
    response_500 = httpx.Response(
        500,
        request=httpx.Request("GET", "http://ams/rest/v2/applications-info"),
    )
    response_ok = httpx.Response(
        200,
        json=[],
        request=httpx.Request("GET", "http://ams/rest/v2/applications-info"),
    )
    mock_http_client = SimpleNamespace(
        request=AsyncMock(side_effect=[response_500, response_500, response_ok])
    )
    monkeypatch.setattr(
        client, "_get_client", AsyncMock(return_value=mock_http_client)
    )
    sleep = AsyncMock()
    monkeypatch.setattr("app.clients.ams_client.asyncio.sleep", sleep)

    response = await client.request("GET", "/rest/v2/applications-info")

    assert response is response_ok
    assert mock_http_client.request.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [0.5, 1.0]


@pytest.mark.asyncio
async def test_applications_info_failure_enters_cooldown_and_success_is_cached(monkeypatch):
    client = AMSClient()
    request = AsyncMock(return_value=None)
    monkeypatch.setattr(client, "request", request)

    with pytest.raises(AMSAPIError, match="request failed"):
        await client.get_applications_info(strict=True)
    with pytest.raises(AMSAPIError, match="cooldown"):
        await client.get_applications_info(strict=True)

    request.assert_awaited_once_with("GET", "/rest/v2/applications-info")

    applications = [
        ApplicationItem(name="test-app", liveStreamCount=3, vodCount=0, storage=0)
    ]
    response = SimpleNamespace(
        json=lambda: [item.model_dump(by_alias=True) for item in applications]
    )
    request.reset_mock(return_value=True)
    request.return_value = response
    client._applications_info_retry_after = 0.0

    assert await client.get_applications_info() == applications
    assert await client.get_applications_info() == applications
    request.assert_awaited_once_with("GET", "/rest/v2/applications-info")


@pytest.mark.asyncio
async def test_close_releases_http_client_and_resets_loop_scoped_state():
    client = AMSClient()
    http_client = SimpleNamespace(is_closed=False, aclose=AsyncMock())
    client._client = http_client
    client._client_loop = asyncio.get_running_loop()
    client._authenticated = True
    client._applications_info = []
    client._applications_info_cached_at = 10.0
    client._applications_info_retry_after = 20.0

    await client.close()

    http_client.aclose.assert_awaited_once()
    assert client._client is None
    assert client._client_loop is None
    assert client._authenticated is False
    assert client._applications_info is None
    assert client._applications_info_cached_at == 0.0
    assert client._applications_info_retry_after == 0.0


@pytest.mark.asyncio
async def test_get_all_broadcasts_paginates_until_short_page(monkeypatch):
    client = AMSClient()
    pages = {
        0: [BroadcastStream(streamId="cam-1"), BroadcastStream(streamId="cam-2")],
        2: [BroadcastStream(streamId="cam-3"), BroadcastStream(streamId="cam-4")],
        4: [BroadcastStream(streamId="cam-5")],
    }
    calls = []

    async def get_page(app_name, offset=0, size=50, *, strict=False):
        calls.append((app_name, offset, size, strict))
        return pages.get(offset, [])

    monkeypatch.setattr(client, "get_broadcasts_list", get_page)

    streams = await client.get_all_broadcasts("atcs", batch_size=2)

    assert [stream.streamId for stream in streams] == [
        "cam-1",
        "cam-2",
        "cam-3",
        "cam-4",
        "cam-5",
    ]
    assert calls == [
        ("atcs", 0, 2, True),
        ("atcs", 2, 2, True),
        ("atcs", 4, 2, True),
    ]


@pytest.mark.asyncio
async def test_get_all_broadcasts_continues_after_short_page_when_count_requires_more(
    monkeypatch,
):
    client = AMSClient()
    pages = {
        0: [BroadcastStream(streamId="cam-1"), BroadcastStream(streamId="cam-2")],
        2: [BroadcastStream(streamId="cam-3"), BroadcastStream(streamId="cam-4")],
        4: [BroadcastStream(streamId="cam-5")],
    }
    calls = []

    async def get_page(app_name, offset=0, size=50, *, strict=False):
        calls.append((offset, size))
        return pages.get(offset, [])

    monkeypatch.setattr(client, "get_broadcasts_list", get_page)

    streams = await client.get_all_broadcasts(
        "atcs",
        batch_size=3,
        expected_count=5,
    )

    assert [stream.streamId for stream in streams] == [
        "cam-1",
        "cam-2",
        "cam-3",
        "cam-4",
        "cam-5",
    ]
    assert calls == [(0, 3), (2, 3), (4, 1)]


@pytest.mark.asyncio
async def test_get_all_broadcasts_rejects_empty_page_before_expected_count(monkeypatch):
    client = AMSClient()

    async def get_page(app_name, offset=0, size=50, *, strict=False):
        return [BroadcastStream(streamId="cam-1")] if offset == 0 else []

    monkeypatch.setattr(client, "get_broadcasts_list", get_page)

    with pytest.raises(AMSAPIError, match="expected 3"):
        await client.get_all_broadcasts(
            "atcs",
            batch_size=2,
            expected_count=3,
        )


@pytest.mark.asyncio
async def test_get_all_broadcasts_reads_empty_page_at_exact_boundary(monkeypatch):
    client = AMSClient()
    calls = []

    async def get_page(app_name, offset=0, size=50, *, strict=False):
        calls.append(offset)
        if offset < 4:
            return [
                BroadcastStream(streamId=f"cam-{offset + 1}"),
                BroadcastStream(streamId=f"cam-{offset + 2}"),
            ]
        return []

    monkeypatch.setattr(client, "get_broadcasts_list", get_page)

    streams = await client.get_all_broadcasts("atcs", batch_size=2, max_pages=3)

    assert len(streams) == 4
    assert calls == [0, 2, 4]


@pytest.mark.asyncio
async def test_get_all_broadcasts_raises_instead_of_silently_truncating(monkeypatch):
    client = AMSClient()

    async def get_full_page(app_name, offset=0, size=50, *, strict=False):
        return [BroadcastStream(streamId=f"cam-{offset + i}") for i in range(size)]

    monkeypatch.setattr(client, "get_broadcasts_list", get_full_page)

    with pytest.raises(RuntimeError, match="max_pages=1"):
        await client.get_all_broadcasts("atcs", batch_size=2, max_pages=1)


@pytest.mark.asyncio
async def test_live_ams_client_smoke():
    client = AMSClient()
    try:
        # Auth
        auth_ok = await client.authenticate()
        assert auth_ok is True
        assert client.is_connected is True

        # Version
        version = await client.get_version()
        assert version is not None
        assert version.versionName == "3.1.0"

        # System Status
        sys_stat = await client.get_system_status()
        assert sys_stat is not None
        assert sys_stat.osName != ""

        # CPU Status
        cpu = await client.get_cpu_status()
        assert cpu is not None
        assert cpu.systemCPULoad is not None

        # System Memory Status
        sys_mem = await client.get_system_memory_status()
        assert sys_mem is not None
        assert sys_mem.totalMemory > 0

        # JVM Memory Status
        jvm = await client.get_jvm_memory_status()
        assert jvm is not None
        assert jvm.maxMemory > 0

        # File System Status
        fs = await client.get_file_system_status()
        assert fs is not None
        assert fs.totalSpace > 0

        # System Resources
        res = await client.get_system_resources()
        assert res is not None
        assert res.cpuUsage is not None

        # Applications
        apps = await client.get_applications()
        assert "atcs" in apps
        assert "atcs-kota" in apps

        # Broadcasts
        count = await client.get_broadcasts_count("atcs")
        assert count > 0

        streams = await client.get_broadcasts_list("atcs", offset=0, size=5)
        assert len(streams) > 0
        assert streams[0].streamId != ""

        all_broadcasts = await client.get_all_broadcasts("atcs", batch_size=50)
        assert len(all_broadcasts) == count
        assert len({stream.streamId for stream in all_broadcasts}) == count

        # VoD List
        vods = await client.get_vod_list("atcs", offset=0, size=5)
        assert isinstance(vods, list)

        # Log Chunk & Log Tail
        log_chunk = await client.get_log_chunk(offset=0, size=2048, log_type="server")
        assert log_chunk is not None
        assert log_chunk.logFileSize > 0

        log_tail = await client.get_log_tail(size=1000, log_type="server")
        assert log_tail is not None
        assert len(log_tail.logContent) > 0
    finally:
        await client.close()
