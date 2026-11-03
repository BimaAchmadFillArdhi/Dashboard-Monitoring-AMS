from datetime import datetime

import httpx
import pytest

from app.services.whatsapp_notifier import WhatsAppNotifier
from app.services.whatsapp_report_service import (
    ERROR_CATEGORIES,
    JAKARTA_TIMEZONE,
    classify_error,
    format_cctv_report,
    next_report_time,
    resolve_stream_name,
)


@pytest.mark.asyncio
async def test_send_report_uses_green_api_for_group_and_personal_chat(monkeypatch):
    posts = []

    class Response:
        def raise_for_status(self):
            return None

    class MockAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            return None

        async def post(self, url, json):
            posts.append((url, json))
            return Response()

    monkeypatch.setattr(
        "app.services.whatsapp_notifier.httpx.AsyncClient", MockAsyncClient
    )
    notifier = WhatsAppNotifier()
    notifier.enabled = True
    notifier.api_url = "https://api.green-api.com"
    notifier.instance_id = "7103123456"
    notifier.api_token = "test-token"
    notifier.chat_ids = ["628123456789@c.us", "120363123456789@g.us"]

    result = await notifier.send_report("*Laporan CCTV*\nTotal ONLINE: 1")

    assert result == {"sent": 2, "failed": 0, "errors": []}
    assert len(posts) == 2
    for url, payload in posts:
        assert url.endswith("/waInstance7103123456/sendMessage/test-token")
        assert payload["chatId"] in notifier.chat_ids
        assert payload["message"] == "*Laporan CCTV*\nTotal ONLINE: 1"


@pytest.mark.asyncio
async def test_send_report_splits_at_line_boundaries_without_truncating_names(monkeypatch):
    sent_messages = []

    class Response:
        def raise_for_status(self):
            return None

    class MockAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            return None

        async def post(self, url, json):
            sent_messages.append(json["message"])
            return Response()

    monkeypatch.setattr(
        "app.services.whatsapp_notifier.httpx.AsyncClient", MockAsyncClient
    )
    notifier = WhatsAppNotifier()
    notifier.enabled = True
    notifier.api_url = "https://api.green-api.com"
    notifier.instance_id = "7103123456"
    notifier.api_token = "test-token"
    notifier.chat_ids = ["120363123456789@g.us"]

    stream_names = [f"Camera-{index:04d}.stream" for index in range(4000)]
    report = "\n".join(f"{index}. {name}" for index, name in enumerate(stream_names, 1))
    result = await notifier.send_report(report)

    assert result["sent"] == 1
    assert len(sent_messages) > 1
    assert all(
        len(message.encode("utf-16-le")) // 2 <= 20000
        for message in sent_messages
    )
    combined_report = "\n".join(sent_messages)
    assert all(name in combined_report for name in stream_names)
    assert "[Laporan dipotong" not in combined_report


def test_report_rejects_a_single_line_that_cannot_fit_without_cutting_it():
    with pytest.raises(ValueError, match="Satu baris laporan"):
        WhatsAppNotifier._split_report_messages("x" * 20001)


@pytest.mark.asyncio
async def test_send_report_returns_sanitized_green_api_error(monkeypatch):
    class Response:
        status_code = 400

        def json(self):
            return {"message": "Invalid token test-token for instance 7103123456"}

        def raise_for_status(self):
            request = httpx.Request("POST", "https://api.green-api.com")
            raise httpx.HTTPStatusError(
                "bad request", request=request, response=self
            )

    class MockAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_value, traceback):
            return None

        async def post(self, url, json):
            return Response()

    monkeypatch.setattr(
        "app.services.whatsapp_notifier.httpx.AsyncClient", MockAsyncClient
    )
    notifier = WhatsAppNotifier()
    notifier.enabled = True
    notifier.api_url = "https://api.green-api.com"
    notifier.instance_id = "7103123456"
    notifier.api_token = "test-token"
    notifier.chat_ids = ["120363123456789@g.us"]

    result = await notifier.send_report("Test report")

    assert result["sent"] == 0
    assert result["failed"] == 1
    assert "HTTP 400" in result["errors"][0]
    assert "[redacted]" in result["errors"][0]
    assert "test-token" not in result["errors"][0]


def test_green_api_chat_ids_support_personal_number_and_group():
    assert WhatsAppNotifier._parse_chat_ids(
        "+62 812-3456-7890,120363123456789@g.us"
    ) == ["6281234567890@c.us", "120363123456789@g.us"]


def test_timeout_errors_explain_connection_and_delivery_uncertainty():
    notifier = WhatsAppNotifier()

    connect_error = notifier._describe_failure(
        httpx.ConnectTimeout("connect timed out")
    )
    read_error = notifier._describe_failure(httpx.ReadTimeout("read timed out"))

    assert "Koneksi ke server Green API tidak terbentuk" in connect_error
    assert "Status pengiriman belum dapat dipastikan" in read_error


@pytest.mark.parametrize(
    ("title", "raw_message", "expected"),
    [
        ("Koneksi Timeout", "", ERROR_CATEGORIES[0]),
        ("Inisialisasi Pembukaan Stream Gagal", "", ERROR_CATEGORIES[1]),
        ("Gagal Membuka Aliran Stream Kamera", "", ERROR_CATEGORIES[2]),
        ("Alamat Stream Kamera Tidak Ditemukan (404)", "", ERROR_CATEGORIES[3]),
        ("Autentikasi Stream Kamera Ditolak (401/403)", "", ERROR_CATEGORIES[4]),
        ("Unexpected server failure", "", ERROR_CATEGORIES[5]),
    ],
)
def test_classify_error_into_six_report_categories(title, raw_message, expected):
    assert classify_error(title, None, raw_message) == expected


def test_report_groups_camera_names_by_error_without_status_lists():
    report = format_cctv_report(
        [
            (
                "Kamera Lobby",
                "Batas Waktu Koneksi Kamera Habis (Timeout)",
                "timeout",
                "",
            ),
            ("Kamera Lobby", "Timeout", "timeout again", ""),
            ("Kamera Jalan", "unknown", "unknown", ""),
        ],
        datetime(2026, 11, 2, 5, 0, tzinfo=JAKARTA_TIMEZONE),
    )

    assert "Daftar CCTV Aktif" not in report
    assert "Daftar CCTV Mati/Down" not in report
    assert "Total CCTV ONLINE" not in report
    assert "Total CCTV OFFLINE" not in report
    assert "*1. Batas Waktu Koneksi Kamera Habis (Timeout)*" in report
    assert "1. Kamera Lobby" in report
    assert report.count("Kamera Lobby") == 1
    assert "*6. Lainnya*" in report
    assert "Kamera Jalan" in report


def test_report_resolves_unique_partial_stream_id_regardless_of_status():
    stream_names = {
        ("cctv-uptmalioboro", "UPTMalio11_TerangBulan.stream"):
            "UPTMalio11_TerangBulan.stream ",
    }

    assert resolve_stream_name(
        "cctv-uptmalioboro", "UPTMalio11_Te", stream_names
    ) == "UPTMalio11_TerangBulan.stream"


def test_report_keeps_ambiguous_partial_stream_id_instead_of_guessing():
    stream_names = {
        ("cctv-kominfogk", "S4BudeganBarat.stream"): "S4BudeganBarat.stream",
        ("cctv-kominfogk", "S4BudeganDome.stream"): "S4BudeganDome.stream",
        ("cctv-kominfogk", "S4BudeganTimur.stream"): "S4BudeganTimur.stream",
    }

    assert resolve_stream_name("cctv-kominfogk", "S4Bu", stream_names) == "S4Bu"


def test_next_report_time_uses_six_and_twenty_wib():
    morning = datetime(2026, 11, 2, 5, 0, tzinfo=JAKARTA_TIMEZONE)
    evening = datetime(2026, 11, 2, 19, 30, tzinfo=JAKARTA_TIMEZONE)
    after_evening = datetime(2026, 11, 2, 21, 0, tzinfo=JAKARTA_TIMEZONE)

    assert next_report_time(morning).hour == 6
    assert next_report_time(evening).hour == 20
    assert next_report_time(after_evening).day == 3
    assert next_report_time(after_evening).hour == 6
