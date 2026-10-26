import time
import asyncio
import collections
from datetime import datetime
from typing import Optional, List, Dict, Any, Deque
from pydantic import BaseModel

from app.clients.ams_client import get_ams_client
from app.clients.models import SystemResources, ApplicationItem


class MetricPoint(BaseModel):
    time: str
    value: float


class RingBufferHistory(BaseModel):
    timestamps: List[str]
    cpu: List[float]
    memory_system: List[float]
    jvm_heap: List[float]
    live_streams: List[int]
    viewers: List[int]
    db_query_ms: List[float]


class DashboardSnapshot(BaseModel):
    ams_connected: bool
    version_name: str = "3.1.0"
    version_type: str = "Community Edition"
    uptime_formatted: str = "-"
    uptime_seconds: int = 0
    server_time: str = ""
    
    # CPU
    cpu_percent: float = 0.0
    cpu_load_avg: float = 0.0
    
    # Memory
    system_memory_used_gb: float = 0.0
    system_memory_total_gb: float = 0.0
    system_memory_percent: float = 0.0
    jvm_heap_used_mb: float = 0.0
    jvm_heap_max_mb: float = 0.0
    jvm_heap_percent: float = 0.0
    
    # Disk
    disk_used_gb: float = 0.0
    disk_total_gb: float = 0.0
    disk_free_gb: float = 0.0
    disk_percent: float = 0.0
    
    # Streams & Viewers
    total_live_streams: int = 0
    total_viewers: int = 0
    webrtc_viewers: int = 0
    hls_viewers: int = 0
    dash_viewers: int = 0
    
    # Internal Performance
    db_query_avg_ms: float = 0.0
    
    # Applications Summary
    applications: List[Dict[str, Any]] = []
    
    # Historical Ring Buffer (last ~10 minutes)
    history: RingBufferHistory


class DashboardService:
    def __init__(self, buffer_maxlen: int = 60):
        self.ams_client = get_ams_client()
        self.buffer_maxlen = buffer_maxlen
        
        # In-memory ring buffer (rolling deque)
        self._timestamps: Deque[str] = collections.deque(maxlen=buffer_maxlen)
        self._cpu_hist: Deque[float] = collections.deque(maxlen=buffer_maxlen)
        self._mem_hist: Deque[float] = collections.deque(maxlen=buffer_maxlen)
        self._jvm_hist: Deque[float] = collections.deque(maxlen=buffer_maxlen)
        self._streams_hist: Deque[int] = collections.deque(maxlen=buffer_maxlen)
        self._viewers_hist: Deque[int] = collections.deque(maxlen=buffer_maxlen)
        self._db_hist: Deque[float] = collections.deque(maxlen=buffer_maxlen)
        
        self._last_snapshot: Optional[DashboardSnapshot] = None
        self._lock = asyncio.Lock()

    def get_history(self) -> RingBufferHistory:
        return RingBufferHistory(
            timestamps=list(self._timestamps),
            cpu=list(self._cpu_hist),
            memory_system=list(self._mem_hist),
            jvm_heap=list(self._jvm_hist),
            live_streams=list(self._streams_hist),
            viewers=list(self._viewers_hist),
            db_query_ms=list(self._db_hist),
        )

    async def collect_snapshot(self) -> DashboardSnapshot:
        """Fetch fresh system metrics and update in-memory ring buffer."""
        async with self._lock:
            resources: Optional[SystemResources] = None
            apps_info: List[ApplicationItem] = []
            connected = False

            try:
                resources = await self.ams_client.get_system_resources()
                apps_info = await self.ams_client.get_applications_info()
                connected = resources is not None
            except Exception:
                connected = False

            now = datetime.now()
            time_label = now.strftime("%H:%M:%S")

            if not connected or resources is None:
                # Return cached or default snapshot with ams_connected=False
                history = self.get_history()
                return DashboardSnapshot(
                    ams_connected=False,
                    server_time=time_label,
                    history=history,
                    applications=[]
                )

            # 1. Parse CPU
            cpu_pct = float(resources.cpuUsage.systemCPULoad) if resources.cpuUsage else 0.0
            cpu_load = float(resources.cpuUsage.systemLoadAverageLastMinute) if resources.cpuUsage else 0.0

            # 2. Parse Memory
            sys_mem = resources.systemMemoryInfo
            sys_used_gb = round(sys_mem.inUseMemory / (1024**3), 2) if sys_mem else 0.0
            sys_tot_gb = round(sys_mem.totalMemory / (1024**3), 2) if sys_mem else 0.0
            sys_pct = sys_mem.usage_percent if sys_mem else 0.0

            jvm_mem = resources.jvmMemoryUsage
            jvm_used_mb = round(jvm_mem.inUseMemory / (1024**2), 1) if jvm_mem else 0.0
            jvm_max_mb = round(jvm_mem.maxMemory / (1024**2), 1) if jvm_mem else 0.0
            jvm_pct = jvm_mem.usage_percent if jvm_mem else 0.0

            # 3. Parse Disk
            fs = resources.fileSystemInfo
            disk_used_gb = round(fs.inUseSpace / (1024**3), 1) if fs else 0.0
            disk_tot_gb = round(fs.totalSpace / (1024**3), 1) if fs else 0.0
            disk_free_gb = round(fs.usableSpace / (1024**3), 1) if fs else 0.0
            disk_pct = fs.usage_percent if fs else 0.0

            # 4. Parse Streams and Viewers
            # Total live streams across apps
            total_live = 0
            for app in apps_info:
                total_live += app.liveStreamCount

            hls_v = max(0, resources.localHLSViewers or 0)
            webrtc_v = max(0, resources.localWebRTCViewers or 0)
            dash_v = max(0, resources.localDASHViewers or 0)
            total_v = hls_v + webrtc_v + dash_v

            # 5. DB Query & Uptime
            db_ms = float(resources.dbAverageQueryTimeMs or 0.0)
            uptime_str = resources.server_timing.uptime_formatted if resources.server_timing else "-"
            uptime_sec = (resources.server_timing.up_time // 1000) if resources.server_timing else 0

            # 6. Applications summary table data
            apps_list = []
            for app in apps_info:
                storage_mb = round(app.storage / (1024**2), 1)
                apps_list.append({
                    "name": app.name,
                    "live_count": app.liveStreamCount,
                    "vod_count": app.vodCount,
                    "storage_mb": storage_mb,
                    "status": "Aktif" if app.liveStreamCount > 0 else "Siap",
                })

            # Update rolling ring buffer
            self._timestamps.append(time_label)
            self._cpu_hist.append(cpu_pct)
            self._mem_hist.append(sys_pct)
            self._jvm_hist.append(jvm_used_mb)
            self._streams_hist.append(total_live)
            self._viewers_hist.append(total_v)
            self._db_hist.append(db_ms)

            snapshot = DashboardSnapshot(
                ams_connected=True,
                version_name=resources.softwareVersion.versionName if resources.softwareVersion else "3.1.0",
                version_type=resources.softwareVersion.versionType if resources.softwareVersion else "Community Edition",
                uptime_formatted=uptime_str,
                uptime_seconds=uptime_sec,
                server_time=time_label,
                cpu_percent=cpu_pct,
                cpu_load_avg=cpu_load,
                system_memory_used_gb=sys_used_gb,
                system_memory_total_gb=sys_tot_gb,
                system_memory_percent=sys_pct,
                jvm_heap_used_mb=jvm_used_mb,
                jvm_heap_max_mb=jvm_max_mb,
                jvm_heap_percent=jvm_pct,
                disk_used_gb=disk_used_gb,
                disk_total_gb=disk_tot_gb,
                disk_free_gb=disk_free_gb,
                disk_percent=disk_pct,
                total_live_streams=total_live,
                total_viewers=total_v,
                webrtc_viewers=webrtc_v,
                hls_viewers=hls_v,
                dash_viewers=dash_v,
                db_query_avg_ms=db_ms,
                applications=apps_list,
                history=self.get_history(),
            )
            self._last_snapshot = snapshot
            return snapshot


# Global singleton
_dashboard_service: Optional[DashboardService] = None


def get_dashboard_service() -> DashboardService:
    global _dashboard_service
    if _dashboard_service is None:
        _dashboard_service = DashboardService()
    return _dashboard_service
