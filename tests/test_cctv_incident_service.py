from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from app.db.models import (
    AmsStreamCurrent,
    AmsSyncLog,
    CctvIncident,
    CctvIncidentNotification,
    LogEntry,
)
from app.services.cctv_incident_service import (
    CATEGORY_CRITICAL,
    CATEGORY_HIGH,
    CATEGORY_WARNING,
    format_duration,
    CctvIncidentService,
    duration_category,
)
from app.services.whatsapp_report_service import ERROR_CATEGORIES


@pytest.mark.parametrize(
    ("hours", "expected"),
    [
        (5, None),
        (6, CATEGORY_WARNING),
        (23, CATEGORY_WARNING),
        (24, CATEGORY_HIGH),
        (47, CATEGORY_HIGH),
        (48, CATEGORY_CRITICAL),
    ],
)
def test_incident_duration_categories(hours, expected):
    assert duration_category(timedelta(hours=hours)) == expected


def test_application_report_summarizes_stream_status_and_unique_error_cameras():
    now = datetime(2026, 11, 4, 5, 0)
    service = CctvIncidentService()
    report = service.format_application_report(
        ["app-normal", "app-error"],
        {
            "app-error": {
                "camera-1": "finished",
                "camera-2": "broadcasting",
            },
            "app-normal": {"camera-2": "broadcasting"},
        },
        {
            "app-error": {
                "camera-1": {ERROR_CATEGORIES[0], ERROR_CATEGORIES[4]},
                "camera-2": {ERROR_CATEGORIES[1], ERROR_CATEGORIES[2],
                             ERROR_CATEGORIES[3], ERROR_CATEGORIES[5]},
            }
        },
        {},
        now,
    )

    assert "⚠️ LAPORAN MONITORING CCTV AMS" in report
    assert "Dashboard monitoring:" in report
    assert "*Aplikasi: app-error*" in report
    assert "- Total CCTV: 2" in report
    assert "- CCTV ON: 1" in report
    assert "- CCTV OFF: 1" in report
    assert "- Koneksi timeout: 1 CCTV" in report
    assert "- Autentikasi ditolak: 1 CCTV" in report
    assert "- Inisialisasi stream gagal: 1 CCTV" in report
    assert "- Gagal membuka stream: 1 CCTV" in report
    assert "- Alamat stream tidak ditemukan: 1 CCTV" in report
    assert "- Masalah lainnya: 1 CCTV" in report
    assert "*Aplikasi: app-normal*" in report
    assert "- Total CCTV: 1" in report
    assert "- Total seluruh CCTV unik: 2" in report
    assert "- Total CCTV ON: 1" in report
    assert "- Total CCTV OFF: 1" in report
    assert "Jumlah aplikasi normal: 1" in report
    assert "Jumlah aplikasi bermasalah: 1" in report


def test_duration_is_reported_as_hours_and_minutes():
    assert format_duration(timedelta(hours=23, minutes=59)) == "23 jam 59 menit"


@pytest.mark.asyncio
async def test_application_report_groups_unidentified_logs_without_stream_id():
    class Result:
        def __init__(self, rows):
            self.rows = rows

        def __iter__(self):
            return iter(self.rows)

    class Session:
        def __init__(self):
            self.results = iter(
                [
                    Result([SimpleNamespace(app_name="app-a")]),
                    Result([]),
                    Result([]),
                    Result(
                        [
                            SimpleNamespace(app_name="app-a", log_count=2),
                            SimpleNamespace(app_name=None, log_count=1),
                        ]
                    ),
                ]
            )

        async def execute(self, _statement):
            return next(self.results)

    report, summary = await CctvIncidentService().build_application_report(
        Session(),
        datetime(2026, 11, 4, 5, 0),
    )

    assert "Log belum teridentifikasi (stream_id kosong/tidak valid): 2" in report
    assert "Log belum teridentifikasi (tanpa aplikasi)" in report
    assert "Jumlah: 1" in report
    assert summary == {
        "total_applications": 1,
        "total_cctv": 0,
        "cctv_on": 0,
        "cctv_off": 0,
        "problematic_applications": 1,
        "normal_applications": 0,
        "unique_problem_cctv": 0,
        "unidentified_logs": 3,
    }


class RecordingNotifier:
    is_configured = True
    chat_ids = ["test-chat@c.us"]

    def __init__(self):
        self.messages = []

    async def send_report(self, message):
        self.messages.append(message)
        return {"sent": 1, "failed": 0, "errors": []}


async def seed_snapshot(session, now, status="finished"):
    session.add(
        AmsSyncLog(
            app_name="cctv-test",
            sync_type="streams",
            status="SUCCESS",
            items_synced=1,
            duration_ms=1,
            created_at=now,
        )
    )
    stream = AmsStreamCurrent(
        app_name="cctv-test",
        stream_id="camera-01",
        name="Kamera Uji",
        status=status,
        last_seen=now,
    )
    await session.merge(stream)
    await session.flush()
    return stream


async def seed_incident(session, now, hours=6):
    incident = CctvIncident(
        app_name="cctv-test",
        stream_id="camera-01",
        stream_name="Kamera Uji",
        problem_type="Stream AMS tidak aktif (finished)",
        problem_detail="Status stream terakhir dari AMS: finished.",
        first_error_at=now - timedelta(hours=hours),
        last_detected_at=now,
    )
    session.add(incident)
    await session.flush()
    return incident


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("hours", "category", "should_notify"),
    [
        (5, None, False),
        (6, CATEGORY_WARNING, True),
        (23, CATEGORY_WARNING, True),
        (24, CATEGORY_HIGH, True),
        (47, CATEGORY_HIGH, True),
        (48, CATEGORY_CRITICAL, True),
    ],
)
async def test_incident_threshold_notifications(
    test_session, hours, category, should_notify
):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now)
    await seed_incident(test_session, now, hours)
    notifier = RecordingNotifier()
    service = CctvIncidentService()

    result = await service.evaluate_and_notify(test_session, notifier, now)

    assert result["qualified"] == int(should_notify)
    assert bool(notifier.messages) is should_notify
    if should_notify:
        assert "⚠️ LAPORAN MONITORING CCTV AMS" in notifier.messages[0]
        assert "Aplikasi: cctv-test" in notifier.messages[0]
        assert "Status: NORMAL" in notifier.messages[0]


@pytest.mark.asyncio
async def test_recovery_closes_incident_and_next_error_starts_new_one(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now, status="broadcasting")
    old_incident = await seed_incident(test_session, now, hours=8)
    old_incident.last_notified_at = now
    old_incident.last_notified_category = CATEGORY_WARNING
    notifier = RecordingNotifier()
    service = CctvIncidentService()

    await service.evaluate_and_notify(test_session, notifier, now)
    assert old_incident.closed_at == now
    assert notifier.messages == []

    await seed_snapshot(test_session, now + timedelta(minutes=1), status="finished")
    await service.evaluate_and_notify(
        test_session, notifier, now + timedelta(minutes=1)
    )
    result = await test_session.execute(
        select(CctvIncident)
        .where(CctvIncident.closed_at.is_(None))
        .order_by(CctvIncident.id.desc())
    )
    new_incident = result.scalars().one()

    assert new_incident.id != old_incident.id
    assert new_incident.first_error_at == now + timedelta(minutes=1)


@pytest.mark.asyncio
async def test_error_type_changes_without_resetting_incident_start(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now, status="broadcasting")
    incident = await seed_incident(test_session, now, hours=7)
    original_first_error = incident.first_error_at
    test_session.add(
        LogEntry(
            ts=int(now.timestamp() * 1000),
            level="ERROR",
            raw_message="Camera timed out",
            app_name="cctv-test",
            stream_id="camera-01",
            friendly_title="Koneksi Kamera Timeout",
            friendly_message="Koneksi kamera habis waktu.",
            severity_group="masalah",
            fingerprint="test-fingerprint",
        )
    )
    notifier = RecordingNotifier()

    await CctvIncidentService().evaluate_and_notify(test_session, notifier, now)

    assert incident.first_error_at == original_first_error
    assert incident.problem_type == "Koneksi Kamera Timeout"


@pytest.mark.asyncio
async def test_same_category_is_not_notified_again_each_monitor_cycle(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now)
    incident = await seed_incident(test_session, now, hours=7)
    notifier = RecordingNotifier()
    service = CctvIncidentService()

    first = await service.evaluate_and_notify(test_session, notifier, now)
    second = await service.evaluate_and_notify(
        test_session, notifier, now + timedelta(minutes=1)
    )
    history = await test_session.execute(
        select(CctvIncidentNotification).where(
            CctvIncidentNotification.incident_id == incident.id
        )
    )

    assert first["notified"] is True
    assert second["notified"] is False
    assert len(notifier.messages) == 1
    assert len(history.scalars().all()) == 1


@pytest.mark.asyncio
async def test_manual_report_uses_incidents_and_obeys_spam_cooldown(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now)
    incident = await seed_incident(test_session, now, hours=24)
    notifier = RecordingNotifier()
    service = CctvIncidentService()

    sent = await service.evaluate_and_notify(
        test_session, notifier, now, manual=True
    )
    suppressed = await service.evaluate_and_notify(
        test_session,
        notifier,
        now + timedelta(minutes=1),
        manual=True,
    )
    history = await test_session.execute(
        select(CctvIncidentNotification).where(
            CctvIncidentNotification.incident_id == incident.id
        )
    )
    notifications = history.scalars().all()

    assert sent["notified"] is True
    assert "⚠️ LAPORAN MONITORING CCTV AMS" in notifier.messages[0]
    assert "Aplikasi: cctv-test" in notifier.messages[0]
    assert "Status: NORMAL" in notifier.messages[0]
    assert suppressed["notified"] is False
    assert suppressed["suppressed"] is True
    assert len(notifier.messages) == 1
    assert len(notifications) == 1
    assert notifications[0].notification_type == "manual"


@pytest.mark.asyncio
async def test_manual_all_normal_report_is_also_rate_limited(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now, status="broadcasting")
    notifier = RecordingNotifier()
    service = CctvIncidentService()

    sent = await service.evaluate_and_notify(
        test_session, notifier, now, manual=True
    )
    suppressed = await service.evaluate_and_notify(
        test_session,
        notifier,
        now + timedelta(minutes=1),
        manual=True,
    )

    assert sent["notified"] is True
    assert "Status: NORMAL" in notifier.messages[0]
    assert suppressed["suppressed"] is True
    assert len(notifier.messages) == 1


@pytest.mark.asyncio
async def test_stale_snapshot_keeps_persisted_incident_reportable(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now)
    incident = await seed_incident(test_session, now, hours=8)
    notifier = RecordingNotifier()
    report_time = now + timedelta(minutes=10)

    result = await CctvIncidentService().evaluate_and_notify(
        test_session, notifier, report_time
    )

    assert result["notified"] is True
    assert result["qualified"] == 1
    assert incident.closed_at is None
    assert "⚠️ LAPORAN MONITORING CCTV AMS" in notifier.messages[0]


@pytest.mark.asyncio
async def test_application_report_counts_match_mysql_error_query(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    test_session.add_all(
        [
            AmsSyncLog(
                app_name="summary-error-app",
                sync_type="streams",
                status="SUCCESS",
                items_synced=1,
                duration_ms=1,
                created_at=now,
            ),
            AmsSyncLog(
                app_name="summary-normal-app",
                sync_type="streams",
                status="SUCCESS",
                items_synced=1,
                duration_ms=1,
                created_at=now,
            ),
            AmsStreamCurrent(
                app_name="summary-error-app",
                stream_id="summary-camera-1",
                name="Summary Camera",
                status="finished",
                last_seen=now,
            ),
            AmsStreamCurrent(
                app_name="summary-normal-app",
                stream_id="summary-camera-1",
                name="Normal Camera",
                status="broadcasting",
                last_seen=now,
            ),
            AmsStreamCurrent(
                app_name="summary-error-app",
                stream_id="summary-camera-2",
                name="Init Camera",
                status="broadcasting",
                last_seen=now,
            ),
            AmsStreamCurrent(
                app_name="summary-error-app",
                stream_id="summary-camera-3",
                name="Open Camera",
                status="finished",
                last_seen=now,
            ),
            AmsStreamCurrent(
                app_name="summary-error-app",
                stream_id="summary-camera-4",
                name="Not Found Camera",
                status="created",
                last_seen=now,
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="RTSP connection timeout",
                app_name="summary-error-app",
                stream_id="summary-camera-1",
                friendly_title="Koneksi Kamera Timeout",
                friendly_message="Timeout saat menghubungi kamera",
                severity_group="masalah",
                fingerprint="summary-timeout-1",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="RTSP connection timed out",
                app_name="summary-error-app",
                stream_id="summary-camera-1",
                friendly_title="Koneksi Kamera Timeout",
                friendly_message="Timeout kedua",
                severity_group="masalah",
                fingerprint="summary-timeout-2",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="RTSP authentication failed",
                app_name="summary-error-app",
                stream_id="summary-camera-1",
                friendly_title="Autentikasi Stream Ditolak",
                friendly_message="Authentication failed",
                severity_group="masalah",
                fingerprint="summary-auth-1",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="Preparing stream failed",
                app_name="summary-error-app",
                stream_id="summary-camera-2",
                friendly_title="Stream Initialization Failed",
                severity_group="masalah",
                fingerprint="summary-init-1",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="Stream Open Failed",
                app_name="summary-error-app",
                stream_id="summary-camera-3",
                friendly_title="Stream Open Failed",
                severity_group="masalah",
                fingerprint="summary-open-1",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="Stream Not Found",
                app_name="summary-error-app",
                stream_id="summary-camera-4",
                friendly_title="Stream Not Found",
                severity_group="masalah",
                fingerprint="summary-not-found-1",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="camera stream identifier missing",
                app_name="summary-error-app",
                stream_id=None,
                friendly_title="Stream Open Failed",
                severity_group="masalah",
                fingerprint="summary-null-stream",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="camera stream unknown",
                app_name="summary-error-app",
                stream_id="unknown",
                friendly_title="Stream Not Found",
                severity_group="masalah",
                fingerprint="summary-unknown-stream",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="ERROR",
                raw_message="unassigned application stream",
                app_name=None,
                stream_id="unassigned-camera",
                friendly_title="Connection Timeout",
                severity_group="masalah",
                fingerprint="summary-null-app",
            ),
            LogEntry(
                ts=int(now.timestamp() * 1000),
                level="INFO",
                raw_message="Publishing started",
                app_name="summary-error-app",
                stream_id="summary-camera-1",
                severity_group="info",
                fingerprint="summary-info-1",
            ),
        ]
    )
    await test_session.flush()
    await test_session.merge(
        AmsStreamCurrent(
            app_name="summary-error-app",
            stream_id="summary-camera-1",
            name="Summary Camera",
            status="finished",
            last_seen=now,
        )
    )
    await test_session.flush()

    report, summary = await CctvIncidentService().build_application_report(
        test_session, now
    )
    mysql_streams = await test_session.execute(
        select(LogEntry.app_name, LogEntry.stream_id)
        .where(
            LogEntry.level == "ERROR",
            LogEntry.app_name == "summary-error-app",
            LogEntry.stream_id.is_not(None),
            func.lower(func.trim(LogEntry.stream_id)).not_in(
                ("", "unknown", "null", "none", "n/a", "na", "undefined",
                 "tidak diketahui", "tidak dikenal")
            ),
        )
    )
    mysql_unique_streams = {
        (app_name, stream_id.strip())
        for app_name, stream_id in mysql_streams
    }

    assert summary == {
        "total_applications": 2,
        "total_cctv": 4,
        "cctv_on": 2,
        "cctv_off": 2,
        "problematic_applications": 2,
        "normal_applications": 0,
    }
    assert mysql_unique_streams == {
        ("summary-error-app", "summary-camera-1"),
        ("summary-error-app", "summary-camera-2"),
        ("summary-error-app", "summary-camera-3"),
        ("summary-error-app", "summary-camera-4"),
    }
    assert report.count("summary-camera-1") == 0
    assert "Total seluruh CCTV unik: 4" in report
    assert "- Total CCTV ON: 2" in report
    assert "- Total CCTV OFF: 2" in report
    assert "Koneksi timeout: 1 CCTV" in report
    assert "Autentikasi ditolak: 1 CCTV" in report
    assert "Inisialisasi stream gagal: 1 CCTV" in report
    assert "Gagal membuka stream: 1 CCTV" in report
    assert "Alamat stream tidak ditemukan: 1 CCTV" in report
    assert "Log belum teridentifikasi (stream_id kosong/tidak valid): 2" in report
    assert "Aplikasi: summary-normal-app" in report
    assert "Log belum teridentifikasi (tanpa aplikasi)" in report


@pytest.mark.asyncio
async def test_error_detail_changes_keep_existing_incident_start(test_session):
    now = datetime(2026, 11, 4, 5, 0)
    await seed_snapshot(test_session, now, status="broadcasting")
    incident = await seed_incident(test_session, now, hours=12)
    original_first_error = incident.first_error_at
    test_session.add(
        LogEntry(
            ts=int(now.timestamp() * 1000),
            level="ERROR",
            raw_message="Camera authentication rejected",
            app_name="cctv-test",
            stream_id="camera-01",
            friendly_title="Autentikasi Ditolak",
            friendly_message="Kredensial kamera ditolak.",
            severity_group="masalah",
            fingerprint="auth-fingerprint",
        )
    )
    await CctvIncidentService().evaluate_and_notify(
        test_session, RecordingNotifier(), now
    )

    assert incident.first_error_at == original_first_error
    assert incident.problem_type == "Autentikasi Ditolak"
