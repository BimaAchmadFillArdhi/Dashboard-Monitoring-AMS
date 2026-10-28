import re
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


class AMSVersion(BaseModel):
    model_config = ConfigDict(extra="ignore")
    versionName: str = "3.1.0"
    versionType: str = "Community Edition"
    buildNumber: Optional[str] = None


class SystemStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    osName: Optional[str] = "Linux"
    osArch: Optional[str] = "amd64"
    javaVersion: Optional[str] = "21"
    processorCount: Optional[int] = 1


class VoDItem(BaseModel):
    model_config = ConfigDict(extra="ignore")
    vodId: Optional[str] = None
    streamName: Optional[str] = None
    streamId: Optional[str] = None
    filePath: Optional[str] = None
    vodName: Optional[str] = None
    date: Optional[int] = 0
    duration: Optional[int] = 0
    fileSize: Optional[int] = 0
    type: Optional[str] = "userVoD"

    @property
    def file_size_mb(self) -> float:
        if self.fileSize and self.fileSize > 0:
            return round(self.fileSize / (1024 * 1024), 2)
        return 0.0

    @property
    def duration_formatted(self) -> str:
        if not self.duration or self.duration <= 0:
            return "0s"
        total_sec = self.duration // 1000
        mins = total_sec // 60
        secs = total_sec % 60
        if mins > 0:
            return f"{mins}m {secs}s"
        return f"{secs}s"


class CpuUsage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    systemCPULoad: Optional[float] = 0.0
    processCPULoad: Optional[float] = 0.0
    systemLoadAverageLastMinute: Optional[float] = 0.0
    processCPUTime: Optional[int] = 0


class JvmMemoryUsage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    maxMemory: int = 0
    totalMemory: int = 0
    freeMemory: int = 0
    inUseMemory: int = 0

    @property
    def usage_percent(self) -> float:
        if self.maxMemory <= 0:
            return 0.0
        return round((self.inUseMemory / self.maxMemory) * 100, 1)


class SystemMemoryInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")
    totalMemory: int = 0
    freeMemory: int = 0
    inUseMemory: int = 0
    availableMemory: Optional[int] = 0
    totalSwapSpace: Optional[int] = 0
    freeSwapSpace: Optional[int] = 0

    @property
    def usage_percent(self) -> float:
        if self.totalMemory <= 0:
            return 0.0
        return round((self.inUseMemory / self.totalMemory) * 100, 1)


class FileSystemInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")
    usableSpace: int = 0
    totalSpace: int = 0
    freeSpace: int = 0
    inUseSpace: int = 0

    @property
    def usage_percent(self) -> float:
        if self.totalSpace <= 0:
            return 0.0
        return round((self.inUseSpace / self.totalSpace) * 100, 1)


class ServerTiming(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    up_time: int = Field(default=0, alias="up-time")
    start_time: int = Field(default=0, alias="start-time")

    @property
    def uptime_formatted(self) -> str:
        seconds = self.up_time // 1000
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        else:
            return f"{minutes}m {secs}s"


class SystemResources(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    instanceId: Optional[str] = None
    cpuUsage: Optional[CpuUsage] = None
    jvmMemoryUsage: Optional[JvmMemoryUsage] = None
    systemMemoryInfo: Optional[SystemMemoryInfo] = None
    fileSystemInfo: Optional[FileSystemInfo] = None
    softwareVersion: Optional[AMSVersion] = None
    systemStatus: Optional[SystemStatus] = None
    server_timing: Optional[ServerTiming] = Field(default=None, alias="server-timing")
    dbAverageQueryTimeMs: Optional[float] = 0.0
    localLiveStreams: Optional[int] = 0
    totalLiveStreamSize: Optional[int] = 0
    localWebRTCViewers: Optional[int] = 0
    localHLSViewers: Optional[int] = 0
    localDASHViewers: Optional[int] = 0


class ApplicationItem(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    liveStreamCount: int = 0
    vodCount: int = 0
    storage: int = 0


def mask_rtsp_url(url: Optional[str]) -> str:
    """Mask credentials in RTSP/RTMP/HTTP URLs (e.g. rtsp://user:pass@host -> rtsp://user:****@host)."""
    if not url:
        return ""
    return re.sub(r'://([^:]+):([^@]+)@', r'://\1:****@', url)


class BroadcastStream(BaseModel):
    model_config = ConfigDict(extra="ignore")
    streamId: str
    name: Optional[str] = None
    status: str = "finished"  # broadcasting, created, finished
    type: str = "streamSource"
    streamUrl: Optional[str] = None
    originAdress: Optional[str] = None
    date: int = 0
    speed: float = 0.0
    bitrate: int = 0
    width: int = 0
    height: int = 0
    hlsViewerCount: int = 0
    webRTCViewerCount: int = 0
    dashViewerCount: int = 0
    mp4Enabled: int = 0
    webMEnabled: int = 0
    autoStartStopEnabled: bool = False

    @property
    def masked_url(self) -> str:
        return mask_rtsp_url(self.streamUrl)

    @property
    def total_viewers(self) -> int:
        return max(0, self.hlsViewerCount) + max(0, self.webRTCViewerCount) + max(0, self.dashViewerCount)

    @property
    def resolution_str(self) -> str:
        if self.width > 0 and self.height > 0:
            return f"{self.width}x{self.height}"
        return "N/A"

    @property
    def is_healthy(self) -> bool:
        return self.status == "broadcasting"


class BroadcastStatistics(BaseModel):
    model_config = ConfigDict(extra="ignore")
    activeLiveStreamCount: int = 0
    totalHLSWatchersCount: int = 0
    totalWebRTCWatchersCount: int = 0
    totalDASHWatchersCount: int = 0
    totalRTMPWatchersCount: int = 0


class LogFileChunk(BaseModel):
    model_config = ConfigDict(extra="ignore")
    logContent: str = ""
    logContentSize: int = 0
    logFileSize: int = 0
