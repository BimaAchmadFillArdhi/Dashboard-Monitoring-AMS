import json
import pytest
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
from app.clients.ams_client import CircuitBreaker, CircuitBreakerState, AMSClient

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
