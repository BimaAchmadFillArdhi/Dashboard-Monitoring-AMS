import pytest
from fastapi.testclient import TestClient
from app.main import app


def test_stream_detail_page_render():
    client = TestClient(app)
    resp = client.get("/applications/atcs/streams/TC-DemenGlagah-VSS")
    assert resp.status_code == 200
    assert "TC-DemenGlagah-VSS" in resp.text
    assert "chart-detail-bitrate" in resp.text
    assert "chart-detail-speed" in resp.text
    assert "chart-detail-viewers" in resp.text
    assert "Semua Grafik" in resp.text
    assert "Bitrate (Mbps)" in resp.text
    assert "Speed" in resp.text
    assert "Viewer" in resp.text


def test_stream_metric_history_api():
    client = TestClient(app)
    resp = client.get("/api/applications/atcs/streams/TC-DemenGlagah-VSS/history?hours=1")
    assert resp.status_code == 200
    data = resp.json()
    assert "stream_id" in data
    assert data["stream_id"] == "TC-DemenGlagah-VSS"
    assert "timestamps" in data
    assert "bitrate_mbps" in data
    assert "speed" in data
    assert "speed_filtered" in data
    assert "ideal_speed" in data
    assert "viewers" in data


def test_system_streams_history_api():
    client = TestClient(app)
    resp = client.get("/api/dashboard/system-streams-history?hours=1")
    assert resp.status_code == 200
    data = resp.json()
    assert "timestamps" in data
    assert "total_bitrate_mbps" in data
    assert "active_streams" in data
    assert "inactive_streams" in data
    assert "total_viewers" in data
