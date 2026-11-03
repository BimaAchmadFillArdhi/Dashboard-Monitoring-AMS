import time
import pytest
from sqlalchemy import select

from app.db.models import (
    AmsStreamCurrent,
    AmsStreamStatusHistory,
    AmsSyncLog,
    IngestState,
    LogEntry,
    UnmatchedPattern,
)
from app.clients.models import LogFileChunk
from app.config import Settings
from app.ingest.log_ingestor import (
    RawLogEntry,
    parse_raw_log_content,
    extract_stream_id,
    extract_app_name,
    compute_fingerprint,
    LogIngestor,
)
from app.services.log_retention import (
    retain_log_entries,
    retain_status_history,
    retain_sync_logs,
)


@pytest.mark.parametrize("retention_limit", ["50", "100"])
def test_log_retention_limit_accepts_supported_environment_values(retention_limit):
    settings = Settings(
        _env_file=None,
        DATABASE_URL="mysql+aiomysql://localhost/ams_monitoring_test",
        LOG_RETENTION_LIMIT=retention_limit,
    )
    assert settings.LOG_RETENTION_LIMIT == int(retention_limit)


def test_log_retention_limit_rejects_unsupported_value():
    with pytest.raises(ValueError):
        Settings(
            _env_file=None,
            DATABASE_URL="mysql+aiomysql://localhost/ams_monitoring_test",
            LOG_RETENTION_LIMIT="75",
        )


def test_parse_multiline_log():
    raw_content = (
        "2026-10-26 12:54:09,259 [Thread-704] INFO  io.antmedia.muxer.MuxAdaptor - First line for streamId:cam1\n"
        "2026-10-26 12:54:09,765 [Thread-704] ERROR io.antmedia.muxer.HLSMuxer - Exception occurred\n"
        "java.lang.RuntimeException: Frame error\n"
        "\tat io.antmedia.muxer.HLSMuxer.write(HLSMuxer.java:123)\n"
        "\tat io.antmedia.muxer.MuxAdaptor.run(MuxAdaptor.java:45)\n"
        "2026-10-26 12:54:10,000 [Thread-705] INFO  io.antmedia.Stream - Third line\n"
    )

    entries = parse_raw_log_content(raw_content)
    assert len(entries) == 3

    assert entries[0].level == "INFO"
    assert "First line" in entries[0].raw_message

    # Multi-line stack trace combined
    assert entries[1].level == "ERROR"
    assert "java.lang.RuntimeException: Frame error" in entries[1].raw_message
    assert "HLSMuxer.java:123" in entries[1].raw_message

    assert entries[2].level == "INFO"
    assert "Third line" in entries[2].raw_message


def test_extract_stream_id_and_app():
    msg1 = "Stream queue size:0 speed:-12.088 for streamId:TC-DemenGlagah-VSS"
    assert extract_stream_id(msg1) == "TC-DemenGlagah-VSS"

    msg2 = "cannot write video frame to muxer(APMD.stream.m3u8). Pts: 123"
    assert extract_stream_id(msg2) == "APMD.stream"

    msg3 = "Stream:kamera_kota is alive -> true"
    assert extract_stream_id(msg3) == "kamera_kota"

    msg_app = "Application /atcs-kota/ stream started"
    assert extract_app_name(msg_app) == "atcs-kota"


def test_fingerprint_computation():
    fp1 = compute_fingerprint("rule_1", "raw message 123", "cam1", "WARN")
    fp2 = compute_fingerprint("rule_1", "raw message 456", "cam1", "WARN")
    # Same rule, same stream, same level -> identical fingerprint for deduplication
    assert fp1 == fp2

    fp_different_stream = compute_fingerprint("rule_1", "raw message 123", "cam2", "WARN")
    assert fp1 != fp_different_stream


@pytest.mark.asyncio
async def test_ingest_chunk_data(test_session):
    ingestor = LogIngestor()

    raw_text = (
        "2026-10-26 12:54:09,765 [Thread-704] WARN  io.antmedia.muxer.HLSMuxer - "
        "cannot write video frame to muxer(TC-DemenGlagah-VSS.m3u8). Pts: 123 dts:123  Error is Numerical result out of range\n"
        "2026-10-26 12:54:10,765 [Thread-704] WARN  io.antmedia.muxer.HLSMuxer - "
        "cannot write video frame to muxer(TC-DemenGlagah-VSS.m3u8). Pts: 124 dts:124  Error is Numerical result out of range\n"
        "2026-10-26 12:54:11,000 [Thread-705] INFO  io.antmedia.Test - Unrecognized custom event 999\n"
    )

    parsed = parse_raw_log_content(raw_text)
    count = await ingestor.ingest_chunk_data(parsed, test_session)
    assert count == 3

    # Check database records
    res = await test_session.execute(select(LogEntry).order_by(LogEntry.ts))
    logs = res.scalars().all()
    assert len(logs) == 3

    # Check translated fields
    assert logs[0].rule_id == "cannot_write_video_frame_numerical_range"
    assert logs[0].severity_group == "masalah"
    assert "TC-DemenGlagah-VSS" in logs[0].friendly_message
    assert logs[0].stream_id == "TC-DemenGlagah-VSS"

    # Both warnings for the same stream share the same fingerprint
    assert logs[0].fingerprint == logs[1].fingerprint

    # Check unmatched pattern recorded
    up_res = await test_session.execute(select(UnmatchedPattern))
    unmatched = up_res.scalars().all()
    assert len(unmatched) == 1
    assert "Unrecognized custom event" in unmatched[0].pattern_sample


@pytest.mark.asyncio
async def test_poll_commits_log_entries_with_incremental_offset(test_session, monkeypatch):
    ingestor = LogIngestor()
    content = (
        "2026-10-26 12:54:09,765 [Thread-1] INFO  io.antmedia.Test - "
        "Incremental event streamId:camera-1\n"
    )
    chunk = LogFileChunk(
        logContent=content,
        logContentSize=len(content.encode("utf-8")),
        logFileSize=len(content.encode("utf-8")),
    )

    async def get_log_chunk(**kwargs):
        return chunk

    monkeypatch.setattr(ingestor.ams_client, "get_log_chunk", get_log_chunk)

    assert await ingestor.poll_server_logs(test_session) == 1

    state = (
        await test_session.execute(
            select(IngestState).where(IngestState.key == "server_log_offset")
        )
    ).scalar_one()
    logs = (await test_session.execute(select(LogEntry))).scalars().all()
    assert state.last_offset == chunk.logContentSize
    assert len(logs) == 1


@pytest.mark.asyncio
async def test_poll_retries_incomplete_trailing_line(test_session, monkeypatch):
    ingestor = LogIngestor()
    first_line = (
        "2026-10-26 12:54:09,765 [Thread-1] INFO  io.antmedia.Test - "
        "First event streamId:camera-1\n"
    )
    second_line = (
        "2026-10-26 12:54:10,765 [Thread-1] INFO  io.antmedia.Test - "
        "Second event streamId:camera-1\n"
    )
    first_content = first_line + second_line[:30]
    chunks = [
        LogFileChunk(
            logContent=first_content,
            logContentSize=len(first_content.encode("utf-8")),
            logFileSize=len((first_line + second_line).encode("utf-8")),
        ),
        LogFileChunk(
            logContent=second_line,
            logContentSize=len(second_line.encode("utf-8")),
            logFileSize=len((first_line + second_line).encode("utf-8")),
        ),
    ]
    requested_offsets = []

    async def get_log_chunk(*, offset, **kwargs):
        requested_offsets.append(offset)
        return chunks[len(requested_offsets) - 1]

    monkeypatch.setattr(ingestor.ams_client, "get_log_chunk", get_log_chunk)

    assert await ingestor.poll_server_logs(test_session) == 1
    assert requested_offsets == [0]
    state = (
        await test_session.execute(
            select(IngestState).where(IngestState.key == "server_log_offset")
        )
    ).scalar_one()
    assert state.last_offset == len(first_line.encode("utf-8"))

    assert await ingestor.poll_server_logs(test_session) == 1
    assert requested_offsets == [0, len(first_line.encode("utf-8"))]
    logs = (await test_session.execute(select(LogEntry))).scalars().all()
    assert len(logs) == 2


@pytest.mark.asyncio
async def test_failed_log_ingestion_does_not_advance_cursor(test_session, monkeypatch):
    ingestor = LogIngestor()
    content = (
        "2026-10-26 12:54:09,765 [Thread-1] INFO  io.antmedia.Test - "
        "Incremental event streamId:camera-1\n"
    )
    chunk = LogFileChunk(
        logContent=content,
        logContentSize=len(content.encode("utf-8")),
        logFileSize=len(content.encode("utf-8")),
    )

    async def get_log_chunk(**kwargs):
        return chunk

    original_ingest = ingestor.ingest_chunk_data

    async def fail_after_ingest(entries, session):
        await original_ingest(entries, session)
        raise RuntimeError("simulate failure before cursor commit")

    monkeypatch.setattr(ingestor.ams_client, "get_log_chunk", get_log_chunk)
    monkeypatch.setattr(ingestor, "ingest_chunk_data", fail_after_ingest)

    with pytest.raises(RuntimeError, match="simulate failure"):
        await ingestor.poll_server_logs(test_session)
    await test_session.rollback()

    state = (
        await test_session.execute(
            select(IngestState).where(IngestState.key == "server_log_offset")
        )
    ).scalar_one()
    logs = (await test_session.execute(select(LogEntry))).scalars().all()
    assert state.last_offset == 0
    assert logs == []


@pytest.mark.asyncio
@pytest.mark.parametrize("insert_count", [55, 105])
async def test_ingest_preserves_logs_for_time_based_retention(test_session, insert_count):
    ingestor = LogIngestor()
    test_session.add_all(
        [
            AmsStreamCurrent(app_name="app-one", stream_id="camera-one", status="broadcasting"),
            AmsStreamCurrent(app_name="app-two", stream_id="camera-two", status="broadcasting"),
        ]
    )
    await test_session.commit()

    entries = []
    for stream_id in ("camera-one", "camera-two"):
        entries.extend(
            RawLogEntry(
                ts=timestamp,
                level="INFO",
                logger="test",
                raw_message=f"event streamId:{stream_id}",
            )
            for timestamp in range(insert_count)
        )

    await ingestor.ingest_chunk_data(entries, test_session)

    for app_name, stream_id in (
        ("app-one", "camera-one"),
        ("app-two", "camera-two"),
    ):
        result = await test_session.execute(
            select(LogEntry)
            .where(LogEntry.app_name == app_name, LogEntry.stream_id == stream_id)
            .order_by(LogEntry.ts.desc(), LogEntry.id.desc())
        )
        logs = result.scalars().all()
        assert len(logs) == insert_count
        assert [log.ts for log in logs] == list(
            range(insert_count - 1, -1, -1)
        )


@pytest.mark.asyncio
async def test_log_retention_keeps_same_stream_id_separate_by_application(test_session):
    entries = [
        LogEntry(
            ts=timestamp,
            level="INFO",
            raw_message="camera event",
            app_name=app_name,
            stream_id="shared-camera",
            severity_group="info",
            fingerprint=f"{app_name}-{timestamp}",
        )
        for app_name in ("app-one", "app-two")
        for timestamp in range(105)
    ]
    test_session.add_all(entries)
    await test_session.flush()

    deleted = await retain_log_entries(test_session, 100)
    await test_session.commit()

    assert deleted == 10
    for app_name in ("app-one", "app-two"):
        result = await test_session.execute(
            select(LogEntry)
            .where(
                LogEntry.app_name == app_name,
                LogEntry.stream_id == "shared-camera",
            )
            .order_by(LogEntry.ts.desc(), LogEntry.id.desc())
        )
        logs = result.scalars().all()
        assert len(logs) == 100
        assert logs[0].ts == 104


@pytest.mark.asyncio
async def test_ambiguous_stream_id_is_not_assigned_to_an_arbitrary_app(test_session):
    ingestor = LogIngestor()
    test_session.add_all(
        [
            AmsStreamCurrent(
                app_name=app_name,
                stream_id="shared-camera",
                status="broadcasting",
            )
            for app_name in ("app-one", "app-two")
        ]
    )
    await test_session.commit()

    await ingestor.ingest_chunk_data(
        [
            RawLogEntry(
                ts=1,
                level="INFO",
                logger="test",
                raw_message="event streamId:shared-camera",
            )
        ],
        test_session,
    )

    result = await test_session.execute(select(LogEntry))
    log = result.scalar_one()
    assert log.stream_id == "shared-camera"
    assert log.app_name is None


@pytest.mark.asyncio
async def test_retains_sync_and_status_logs_per_identity(test_session):
    from datetime import datetime, timedelta

    sync_logs = [
        AmsSyncLog(
            app_name=app_name,
            sync_type="streams",
            status="SUCCESS",
            items_synced=1,
            duration_ms=1,
            created_at=datetime(2026, 1, 1) + timedelta(seconds=index),
        )
        for app_name in ("app-one", "app-two")
        for index in range(105)
    ]
    status_logs = [
        AmsStreamStatusHistory(
            app_name=app_name,
            stream_id=stream_id,
            old_status="finished",
            new_status="broadcasting",
            ts=index,
        )
        for app_name, stream_id in (("app-one", "camera-one"), ("app-two", "camera-two"))
        for index in range(105)
    ]
    test_session.add_all(sync_logs + status_logs)
    await test_session.flush()

    assert await retain_sync_logs(test_session, 100) == 10
    assert await retain_status_history(test_session, 100) == 10
    await test_session.commit()

    for app_name in ("app-one", "app-two"):
        result = await test_session.execute(
            select(AmsSyncLog)
            .where(AmsSyncLog.app_name == app_name)
            .order_by(AmsSyncLog.created_at.desc(), AmsSyncLog.id.desc())
        )
        rows = result.scalars().all()
        assert len(rows) == 100
        assert rows[0].created_at == datetime(2026, 1, 1) + timedelta(seconds=104)

    for app_name, stream_id in (("app-one", "camera-one"), ("app-two", "camera-two")):
        result = await test_session.execute(
            select(AmsStreamStatusHistory)
            .where(
                AmsStreamStatusHistory.app_name == app_name,
                AmsStreamStatusHistory.stream_id == stream_id,
            )
            .order_by(AmsStreamStatusHistory.ts.desc(), AmsStreamStatusHistory.id.desc())
        )
        rows = result.scalars().all()
        assert len(rows) == 100
        assert rows[0].ts == 104
