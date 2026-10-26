#!/usr/bin/env python3
"""
scripts/discover_ams.py - Discovery & Sample Collector for Ant Media Server (AMS)
"""
import os
import sys
import json
import hashlib
import urllib.parse
from pathlib import Path
import httpx

# Ensure project root is in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.config import get_settings


def get_md5_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def save_sample(name: str, data: dict | list, samples_dir: Path):
    filepath = samples_dir / f"{name}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  [SAVED] {filepath.name} ({len(json.dumps(data))} bytes)")


def main():
    settings = get_settings()
    base_url = settings.AMS_BASE_URL.rstrip("/")
    samples_dir = BASE_DIR / "docs" / "ams_samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    print(f"[*] Menghubungi Ant Media Server di: {base_url}")
    print(f"[*] User: {settings.AMS_USER}")

    client = httpx.Client(base_url=base_url, timeout=settings.AMS_TIMEOUT_SECONDS)

    # 1. Authenticate to AMS
    auth_payload = {
        "email": settings.AMS_USER,
        "password": get_md5_hash(settings.AMS_PASSWORD)
    }

    try:
        auth_resp = client.post("/rest/v2/users/authenticate", json=auth_payload)
        auth_data = auth_resp.json()
        if not auth_data.get("success"):
            print(f"[!] Autentikasi gagal: {auth_data.get('message')}")
            sys.exit(1)
        print(f"[+] Autentikasi berhasil: {auth_data.get('message')}")
        save_sample("auth_response", auth_data, samples_dir)
    except Exception as e:
        print(f"[!] Gagal autentikasi: {e}")
        sys.exit(1)

    # Helper function for GET request
    def get_json(endpoint: str, name: str, params: dict = None) -> dict | list | None:
        try:
            resp = client.get(endpoint, params=params)
            resp.raise_for_status()
            data = resp.json()
            save_sample(name, data, samples_dir)
            return data
        except Exception as err:
            print(f"  [ERR] {endpoint}: {err}")
            return None

    # Helper function for proxy request to per-app REST API
    def get_proxy_json(path_inside: str, name: str) -> dict | list | None:
        try:
            endpoint = f"/rest/v2/request?_path={urllib.parse.quote(path_inside)}"
            resp = client.get(endpoint)
            resp.raise_for_status()
            data = resp.json()
            save_sample(name, data, samples_dir)
            return data
        except Exception as err:
            print(f"  [ERR] PROXY {path_inside}: {err}")
            return None

    # 2. System Endpoints
    print("\n[*] Mengambil data sistem:")
    get_json("/rest/v2/version", "version")
    get_json("/rest/v2/system-resources", "system_resources")
    get_json("/rest/v2/cpu-status", "cpu_status")
    get_json("/rest/v2/system-memory-status", "system_memory_status")
    get_json("/rest/v2/file-system-status", "file_system_status")

    # 3. Applications
    print("\n[*] Mengambil data aplikasi:")
    apps_data = get_json("/rest/v2/applications", "applications")
    get_json("/rest/v2/applications-info", "applications_info")
    get_json("/rest/v2/applications/settings/atcs", "app_settings_atcs")

    # 4. Per-app broadcasts (using AMS proxy pattern /rest/v2/request?_path=...)
    print("\n[*] Mengambil data stream & broadcast (via proxy request):")
    get_proxy_json("atcs/rest/v2/broadcasts/count", "broadcasts_count_atcs")
    streams = get_proxy_json("atcs/rest/v2/broadcasts/list/0/10", "broadcasts_list_atcs")
    get_proxy_json("atcs/rest/v2/broadcasts/active-live-stream-count", "broadcasts_active_count_atcs")
    get_proxy_json("atcs/rest/v2/broadcasts/total-broadcast-statistics", "broadcast_total_statistics_atcs")

    if streams and len(streams) > 0:
        stream_id = streams[0].get("streamId")
        if stream_id:
            get_proxy_json(f"atcs/rest/v2/broadcasts/{stream_id}", "broadcast_single_sample")

    # Also sample atcs-kota
    get_proxy_json("atcs-kota/rest/v2/broadcasts/count", "broadcasts_count_atcs_kota")
    get_proxy_json("atcs-kota/rest/v2/broadcasts/list/0/5", "broadcasts_list_atcs_kota")

    # 5. Log Files
    print("\n[*] Mengambil sampel file log AMS:")
    log_sample = get_json("/rest/v2/log-file/0/5000", "log_file_sample", params={"logType": "server"})
    if log_sample:
        print(f"  [+] Log file size: {log_sample.get('logFileSize')} bytes, content size: {log_sample.get('logContentSize')} bytes")

    # Error log sample
    get_json("/rest/v2/log-file/0/5000", "log_file_error_sample", params={"logType": "errors"})

    print("\n[+] Discovery selesai. Semua sampel tersimpan di docs/ams_samples/.")


if __name__ == "__main__":
    main()
