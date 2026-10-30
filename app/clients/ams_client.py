import time
import asyncio
import hashlib
import logging
import urllib.parse
from enum import Enum
from typing import Optional, List, Dict, Any
import httpx

from app.config import get_settings
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
)

logger = logging.getLogger("ams.client")


class CircuitBreakerState(str, Enum):
    CLOSED = "CLOSED"        # Normal operations
    OPEN = "OPEN"            # Server broken / failing, fast fail
    HALF_OPEN = "HALF_OPEN"  # Testing recovery


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 15.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0

    def can_attempt(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True
        now = time.monotonic()
        if self.state == CircuitBreakerState.OPEN:
            if now - self.last_failure_time > self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                logger.info("[CircuitBreaker] Transitioned from OPEN to HALF_OPEN")
                return True
            return False
        # HALF_OPEN allows single probe
        return True

    def record_success(self):
        self.failure_count = 0
        if self.state != CircuitBreakerState.CLOSED:
            logger.info("[CircuitBreaker] Server recovered. State reset to CLOSED")
            self.state = CircuitBreakerState.CLOSED

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        if self.state == CircuitBreakerState.HALF_OPEN or self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            logger.warning(
                f"[CircuitBreaker] Tripped to OPEN (failures={self.failure_count}). "
                f"Fast-failing for {self.recovery_timeout}s"
            )


class AMSClient:
    """Robust async HTTP client for Ant Media Server (AMS) Community Edition 3.1.0."""

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.AMS_BASE_URL.rstrip("/")
        self.email = self.settings.AMS_USER
        self.password_md5 = hashlib.md5(self.settings.AMS_PASSWORD.encode("utf-8")).hexdigest()
        self.timeout = self.settings.AMS_TIMEOUT_SECONDS
        self.max_retries = self.settings.AMS_MAX_RETRIES

        self._client: Optional[httpx.AsyncClient] = None
        self._client_loop = None
        self._circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=15.0)
        self._authenticated = False
        self._auth_lock = asyncio.Lock()

    async def _get_client(self) -> httpx.AsyncClient:
        current_loop = asyncio.get_running_loop()
        if (
            self._client is None
            or self._client.is_closed
            or self._client_loop != current_loop
        ):
            if self._client and not self._client.is_closed:
                try:
                    await self._client.aclose()
                except Exception:
                    pass
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=self.timeout),
                follow_redirects=True,
            )
            self._client_loop = current_loop
            self._authenticated = False
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    @property
    def is_connected(self) -> bool:
        """Returns True if AMS is in a healthy, reachable state."""
        return self._circuit_breaker.state == CircuitBreakerState.CLOSED and self._authenticated

    async def authenticate(self) -> bool:
        """Authenticate with AMS using email and MD5 hashed password."""
        async with self._auth_lock:
            client = await self._get_client()
            payload = {
                "email": self.email,
                "password": self.password_md5,
            }
            try:
                resp = await client.post("/rest/v2/users/authenticate", json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        self._authenticated = True
                        self._circuit_breaker.record_success()
                        logger.info(f"[AMSClient] Autentikasi berhasil: {data.get('message')}")
                        return True
                    else:
                        logger.warning(f"[AMSClient] Autentikasi ditolak: {data.get('message')}")
                        return False
                logger.warning(f"[AMSClient] HTTP {resp.status_code} saat autentikasi")
                return False
            except Exception as e:
                logger.error(f"[AMSClient] Error saat autentikasi: {e}")
                self._circuit_breaker.record_failure()
                return False

    async def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        is_retry: bool = False,
    ) -> Optional[httpx.Response]:
        """Make an HTTP request to AMS with retry, circuit breaker, and auto-auth."""
        if not self._circuit_breaker.can_attempt():
            return None

        client = await self._get_client()

        if not self._authenticated and not path.endswith("/authenticate"):
            auth_ok = await self.authenticate()
            if not auth_ok:
                return None

        for attempt in range(self.max_retries):
            try:
                resp = await client.request(
                    method=method,
                    url=path,
                    params=params,
                    json=json_body,
                )

                # If session expired or forbidden, re-authenticate once
                if resp.status_code in (401, 403) and not is_retry:
                    logger.warning(f"[AMSClient] Session expired ({resp.status_code}), re-authenticating...")
                    self._authenticated = False
                    if await self.authenticate():
                        return await self.request(
                            method, path, params, json_body, is_retry=True
                        )
                    else:
                        return None

                resp.raise_for_status()
                self._circuit_breaker.record_success()
                return resp

            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as err:
                logger.warning(f"[AMSClient] Network error on {path} (attempt {attempt + 1}/{self.max_retries}): {err}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                else:
                    self._circuit_breaker.record_failure()
                    return None
            except httpx.HTTPStatusError as err:
                logger.warning(f"[AMSClient] HTTP status error on {path}: {err}")
                if resp.status_code >= 500:
                    self._circuit_breaker.record_failure()
                return None
            except Exception as err:
                logger.error(f"[AMSClient] Unexpected error on {path}: {err}")
                self._circuit_breaker.record_failure()
                return None

        return None

    async def proxy_request(self, sub_path: str) -> Optional[httpx.Response]:
        """Call per-app REST API using the AMS Tomcat proxy pattern /rest/v2/request?_path=..."""
        encoded_path = urllib.parse.quote(sub_path.lstrip("/"))
        return await self.request("GET", f"/rest/v2/request?_path={encoded_path}")

    # =========================================================================
    # High-level typed API methods
    # =========================================================================

    async def get_version(self) -> Optional[AMSVersion]:
        resp = await self.request("GET", "/rest/v2/version")
        if resp:
            return AMSVersion(**resp.json())
        return None

    async def get_system_status(self) -> Optional[SystemStatus]:
        resp = await self.request("GET", "/rest/v2/system-status")
        if resp:
            return SystemStatus(**resp.json())
        return None

    async def get_cpu_status(self) -> Optional[CpuUsage]:
        resp = await self.request("GET", "/rest/v2/cpu-status")
        if resp:
            return CpuUsage(**resp.json())
        return None

    async def get_system_memory_status(self) -> Optional[SystemMemoryInfo]:
        resp = await self.request("GET", "/rest/v2/system-memory-status")
        if resp:
            return SystemMemoryInfo(**resp.json())
        return None

    async def get_jvm_memory_status(self) -> Optional[JvmMemoryUsage]:
        resp = await self.request("GET", "/rest/v2/jvm-memory-status")
        if resp:
            return JvmMemoryUsage(**resp.json())
        return None

    async def get_file_system_status(self) -> Optional[FileSystemInfo]:
        resp = await self.request("GET", "/rest/v2/file-system-status")
        if resp:
            return FileSystemInfo(**resp.json())
        return None

    async def get_system_resources(self) -> Optional[SystemResources]:
        """Fetch unified system resources with automatic fallback to granular endpoints."""
        resp = await self.request("GET", "/rest/v2/system-resources")
        resources: Optional[SystemResources] = None
        if resp:
            try:
                resources = SystemResources(**resp.json())
            except Exception as e:
                logger.warning(f"[AMSClient] Error parsing system-resources: {e}")

        # If system-resources endpoint failed or missing, reconstruct from granular status
        if not resources:
            try:
                cpu = await self.get_cpu_status()
                sys_mem = await self.get_system_memory_status()
                jvm_mem = await self.get_jvm_memory_status()
                fs = await self.get_file_system_status()
                ver = await self.get_version()
                if cpu or sys_mem or jvm_mem:
                    resources = SystemResources(
                        cpuUsage=cpu,
                        systemMemoryInfo=sys_mem,
                        jvmMemoryUsage=jvm_mem,
                        fileSystemInfo=fs,
                        softwareVersion=ver,
                    )
            except Exception as err:
                logger.error(f"[AMSClient] Fallback to granular status failed: {err}")

        # Enrich with system-status if not already populated
        if resources and not resources.systemStatus:
            try:
                sys_stat = await self.get_system_status()
                resources.systemStatus = sys_stat
            except Exception:
                pass

        return resources

    async def get_applications(self) -> List[str]:
        resp = await self.request("GET", "/rest/v2/applications")
        if resp:
            data = resp.json()
            return data.get("applications", [])
        return []

    async def get_applications_info(self) -> List[ApplicationItem]:
        resp = await self.request("GET", "/rest/v2/applications-info")
        if resp:
            data = resp.json()
            if isinstance(data, list):
                return [ApplicationItem(**item) for item in data]
        return []

    async def get_broadcasts_count(self, app_name: str) -> int:
        resp = await self.proxy_request(f"{app_name}/rest/v2/broadcasts/count")
        if resp:
            data = resp.json()
            return data.get("number", 0)
        return 0

    async def get_broadcasts_list(
        self, app_name: str, offset: int = 0, size: int = 50
    ) -> List[BroadcastStream]:
        resp = await self.proxy_request(f"{app_name}/rest/v2/broadcasts/list/{offset}/{size}")
        if resp:
            data = resp.json()
            if isinstance(data, list):
                return [BroadcastStream(**item) for item in data]
        return []

    async def get_all_broadcasts(
        self, app_name: str, batch_size: int = 50, max_pages: int = 100
    ) -> List[BroadcastStream]:
        """Fetch ALL broadcasts for an application using pagination loop until exhausted."""
        total_count = await self.get_broadcasts_count(app_name)
        all_streams: List[BroadcastStream] = []
        offset = 0
        seen_ids = set()

        for _ in range(max_pages):
            batch = await self.get_broadcasts_list(app_name, offset=offset, size=batch_size)
            if not batch:
                break

            new_items_found = False
            for s in batch:
                if s.streamId and s.streamId not in seen_ids:
                    seen_ids.add(s.streamId)
                    all_streams.append(s)
                    new_items_found = True

            # If no new items found or returned less than batch_size, stop
            if not new_items_found or len(batch) < batch_size:
                break

            offset += len(batch)
            if total_count > 0 and offset >= total_count:
                break

        return all_streams

    async def get_all_vods(
        self, app_name: str, batch_size: int = 50, max_pages: int = 100
    ) -> List[VoDItem]:
        """Fetch ALL VoD recordings for an application using pagination loop until exhausted."""
        total_count = await self.get_vod_count(app_name)
        all_vods: List[VoDItem] = []
        offset = 0
        seen_ids = set()

        for _ in range(max_pages):
            batch = await self.get_vod_list(app_name, offset=offset, size=batch_size)
            if not batch:
                break

            new_items_found = False
            for v in batch:
                vid = v.vodId or v.filePath or v.vodName
                if vid and vid not in seen_ids:
                    seen_ids.add(vid)
                    all_vods.append(v)
                    new_items_found = True

            if not new_items_found or len(batch) < batch_size:
                break

            offset += len(batch)
            if total_count > 0 and offset >= total_count:
                break

        return all_vods

    async def get_broadcast(self, app_name: str, stream_id: str) -> Optional[BroadcastStream]:
        resp = await self.proxy_request(f"{app_name}/rest/v2/broadcasts/{stream_id}")
        if resp:
            return BroadcastStream(**resp.json())
        return None

    async def get_broadcast_statistics(self, app_name: str) -> Optional[BroadcastStatistics]:
        resp = await self.proxy_request(f"{app_name}/rest/v2/broadcasts/total-broadcast-statistics")
        if resp:
            return BroadcastStatistics(**resp.json())
        return None

    async def get_vod_count(self, app_name: str) -> int:
        resp = await self.proxy_request(f"{app_name}/rest/v2/vods/count")
        if resp:
            data = resp.json()
            return data.get("number", 0)
        return 0

    async def get_vod_list(
        self, app_name: str, offset: int = 0, size: int = 50
    ) -> List[VoDItem]:
        resp = await self.proxy_request(f"{app_name}/rest/v2/vods/list/{offset}/{size}")
        if resp:
            data = resp.json()
            if isinstance(data, list):
                return [VoDItem(**item) for item in data]
        return []

    async def get_log_chunk(
        self, offset: int = 0, size: int = 65536, log_type: str = "server"
    ) -> Optional[LogFileChunk]:
        resp = await self.request(
            "GET",
            f"/rest/v2/log-file/{offset}/{size}",
            params={"logType": log_type},
        )
        if resp:
            return LogFileChunk(**resp.json())
        return None

    async def get_log_tail(
        self, size: int = 1000, log_type: str = "server"
    ) -> Optional[LogFileChunk]:
        resp = await self.request(
            "GET",
            f"/rest/v2/log-file/-1/{size}",
            params={"logType": log_type},
        )
        if resp:
            return LogFileChunk(**resp.json())
        return None


# Global singleton instance
_ams_client: Optional[AMSClient] = None


def get_ams_client() -> AMSClient:
    global _ams_client
    if _ams_client is None:
        _ams_client = AMSClient()
    return _ams_client
