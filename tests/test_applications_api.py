import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from fastapi.testclient import TestClient
from app.main import app
from app.clients.ams_client import AMSAPIError
from app.clients.models import ApplicationItem, BroadcastStream
from app.services.application_service import ApplicationService


def test_applications_page_render():
    client = TestClient(app)
    resp = client.get("/applications")
    assert resp.status_code == 200
    assert "Applications &amp; Stream Monitoring" in resp.text or "Applications & Stream Monitoring" in resp.text
    assert "Aplikasi Aktif:" in resp.text
    assert "Streams (" in resp.text
    assert "VoD Rekaman" in resp.text


def test_api_applications_overview(monkeypatch):
    from app.services.application_service import AppSummaryDTO

    monkeypatch.setattr(
        "app.api.applications_api.app_service.get_applications_overview",
        AsyncMock(return_value=[
            AppSummaryDTO(
                name="atcs",
                stream_count=239,
                live_count=143,
                vod_count=0,
                storage_mb=0,
                problematic_count=0,
            )
        ]),
    )
    client = TestClient(app)
    resp = client.get("/api/applications")
    assert resp.status_code == 200
    apps = resp.json()
    assert isinstance(apps, list)
    assert len(apps) > 0
    atcs_app = next((a for a in apps if a["name"] == "atcs"), None)
    assert atcs_app is not None
    assert atcs_app["stream_count"] == 239
    assert atcs_app["live_count"] == 143
    assert atcs_app["problematic_count"] == 0


def test_api_applications_overview_reports_ams_unavailable(monkeypatch):
    monkeypatch.setattr(
        "app.api.applications_api.app_service.get_applications_overview",
        AsyncMock(side_effect=AMSAPIError("applications-info unavailable")),
    )
    resp = TestClient(app).get("/api/applications")

    assert resp.status_code == 503
    assert "AMS" in resp.json()["detail"]


def test_api_app_streams_reports_ams_unavailable(monkeypatch):
    monkeypatch.setattr(
        "app.api.applications_api.app_service.get_streams_for_app",
        AsyncMock(side_effect=AMSAPIError("stream list unavailable")),
    )
    resp = TestClient(app).get("/api/applications/atcs-kota/streams")

    assert resp.status_code == 503
    assert "atcs-kota" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_application_overview_counts_all_and_live_streams(monkeypatch):
    streams = [
        BroadcastStream(streamId=f"stream-{index}", status="broadcasting" if index % 2 else "finished")
        for index in range(239)
    ]
    ams_mock = SimpleNamespace(
        get_applications_info=AsyncMock(return_value=[
            ApplicationItem(name="test-app", liveStreamCount=999, vodCount=0, storage=0)
        ]),
        get_broadcasts_count=AsyncMock(return_value=239),
        get_all_broadcasts=AsyncMock(return_value=streams),
    )
    health_mock = SimpleNamespace(
        evaluate_stream_health=AsyncMock(return_value=SimpleNamespace(status="Sehat"))
    )
    monkeypatch.setattr("app.services.application_service.ams_client", ams_mock)
    monkeypatch.setattr("app.services.application_service.health_service", health_mock)

    summary = await ApplicationService().get_applications_overview(session=None)

    assert summary[0].stream_count == 239
    assert summary[0].live_count == 119
    assert ams_mock.get_all_broadcasts.await_args.kwargs["batch_size"] == 50


@pytest.mark.asyncio
async def test_application_overview_does_not_convert_ams_failure_to_zero(monkeypatch):
    ams_mock = SimpleNamespace(
        get_applications_info=AsyncMock(return_value=[
            ApplicationItem(name="test-app", liveStreamCount=58, vodCount=0, storage=0)
        ]),
        get_broadcasts_count=AsyncMock(return_value=58),
        get_all_broadcasts=AsyncMock(side_effect=AMSAPIError("broadcast list unavailable")),
    )
    monkeypatch.setattr("app.services.application_service.ams_client", ams_mock)

    with pytest.raises(AMSAPIError):
        await ApplicationService().get_applications_overview(session=None)


def test_applications_page_includes_stream_summary_and_fetches_pages():
    client = TestClient(app)
    resp = client.get("/applications")
    assert resp.status_code == 200
    assert "selectedAppSummary.stream_count" in resp.text
    assert "selectedAppSummary.live_count" in resp.text
    assert "const pageSize = 50" in resp.text
    assert "this.allStreams.filter(stream" in resp.text
    assert "appsUnavailable" in resp.text
    assert "streamsUnavailable" in resp.text
    assert "app.live_count == null ? '— live'" in resp.text
    assert "Hitungan aplikasi tidak tersedia dari AMS." in resp.text


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
