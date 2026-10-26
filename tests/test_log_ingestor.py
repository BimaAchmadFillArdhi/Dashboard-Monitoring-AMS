import time
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select

from app.db.models import Base, LogEntry, UnmatchedPattern
from app.ingest.log_ingestor import (
    parse_raw_log_content,
    extract_stream_id,
    extract_app_name,
    compute_fingerprint,
    LogIngestor,
)


@pytest_asyncio.fixture
async def test_session():
    # In-memory SQLite for isolated testing
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


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
