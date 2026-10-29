import pytest
from fastapi.testclient import TestClient
from app.main import app


def test_applications_page_render():
    client = TestClient(app)
    resp = client.get("/applications")
    assert resp.status_code == 200
    assert "Applications &amp; Stream Monitoring" in resp.text or "Applications & Stream Monitoring" in resp.text
    assert "Aplikasi Aktif:" in resp.text
    assert "Live Streams" in resp.text
    assert "VoD Rekaman" in resp.text


def test_api_applications_overview():
    client = TestClient(app)
    resp = client.get("/api/applications")
    assert resp.status_code == 200
    apps = resp.json()
    assert isinstance(apps, list)
    assert len(apps) > 0
    atcs_app = next((a for a in apps if a["name"] == "atcs"), None)
    assert atcs_app is not None
    assert "live_count" in atcs_app
    assert "problematic_count" in atcs_app


def test_api_applications_streams():
    client = TestClient(app)
    resp = client.get("/api/applications/atcs/streams")
    assert resp.status_code == 200
    streams = resp.json()
    assert isinstance(streams, list)
    assert len(streams) > 0

    s0 = streams[0]
    assert "stream_id" in s0
    assert "health_status" in s0
    assert "masked_url" in s0
    assert "password" not in s0["masked_url"].lower()


def test_api_applications_vods():
    client = TestClient(app)
    resp = client.get("/api/applications/atcs/vods")
    assert resp.status_code == 200
    vods = resp.json()
    assert isinstance(vods, list)


def test_api_stream_detail_masked():
    client = TestClient(app)
    # Get first streamId from atcs
    resp_list = client.get("/api/applications/atcs/streams")
    streams = resp_list.json()
    sid = streams[0]["stream_id"]

    resp = client.get(f"/api/applications/atcs/streams/{sid}")
    assert resp.status_code == 200
    detail = resp.json()

    assert detail["stream_id"] == sid
    assert "masked_url" in detail
    assert "bitrate" in detail
    assert "bitrate_mbps" in detail
    assert "bitrate_kbps" in detail
    assert "speed" in detail
    assert "total_viewers" in detail
    assert "resolution" in detail
    assert "auto_start_stop" in detail


def test_api_stream_history():
    client = TestClient(app)
    resp_list = client.get("/api/applications/atcs/streams")
    streams = resp_list.json()
    sid = streams[0]["stream_id"]

    resp = client.get(f"/api/applications/atcs/streams/{sid}/history?hours=1")
    assert resp.status_code == 200
    hist = resp.json()

    assert hist["stream_id"] == sid
    assert hist["app_name"] == "atcs"
    assert "timestamps" in hist
    assert "bitrate_mbps" in hist
    assert "speed" in hist
    assert "viewers" in hist
    assert "points_count" in hist
    assert hist["points_count"] >= 1


def test_stream_detail_page_render():
    client = TestClient(app)
    resp = client.get("/applications/atcs/streams/TC-DemenGlagah-VSS")
    assert resp.status_code == 200
    assert "TC-DemenGlagah-VSS" in resp.text
    assert "chart-detail-bitrate" in resp.text
    assert "chart-detail-speed" in resp.text
    assert "chart-detail-viewers" in resp.text
    assert "Ingest Bitrate" in resp.text


