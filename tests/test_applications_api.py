import pytest
from fastapi.testclient import TestClient
from app.main import app


def test_applications_page_render():
    client = TestClient(app)
    resp = client.get("/applications")
    assert resp.status_code == 200
    assert "Applications &amp; Stream Monitoring" in resp.text or "Applications & Stream Monitoring" in resp.text
    assert "Daftar Aplikasi AMS" in resp.text
    assert "Live Streams" in resp.text


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
    assert "resolution" in detail
    assert "auto_start_stop" in detail
