import pytest
from fastapi.testclient import TestClient
from app.main import app


def test_dashboard_page_render():
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "System CPU" in resp.text
    assert "Memory &amp; Heap JVM" in resp.text or "Memory & Heap JVM" in resp.text
    assert "Live Streams &amp; Viewer" in resp.text or "Live Streams & Viewer" in resp.text
    assert "chart-cpu" in resp.text
    assert "Daftar Aplikasi Ant Media Server" in resp.text


def test_dashboard_metrics_api():
    client = TestClient(app)
    resp = client.get("/api/dashboard/metrics")
    assert resp.status_code == 200
    data = resp.json()

    assert "ams_connected" in data
    assert "cpu_percent" in data
    assert "system_memory_percent" in data
    assert "jvm_heap_used_mb" in data
    assert "total_live_streams" in data
    assert "history" in data
    assert "cpu" in data["history"]
    assert "memory_system" in data["history"]


def test_dashboard_history_api():
    client = TestClient(app)
    resp = client.get("/api/dashboard/history?hours=1")
    assert resp.status_code == 200
    data = resp.json()
    assert "timestamps" in data
    assert "cpu" in data
    assert "memory_system" in data


def test_dashboard_server_info_api():
    client = TestClient(app)
    resp = client.get("/api/dashboard/server-info")
    assert resp.status_code == 200
    data = resp.json()
    assert "ams_connected" in data
    assert "version_name" in data
    assert "os_name" in data
    assert "processor_count" in data
