from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    Float,
    String,
    Text,
    DateTime,
    Index,
)
from app.db.session import Base

# Universal ID column that autoincrements on both SQLite (ROWID Integer) and MySQL (BIGINT)
AutoId = BigInteger().with_variant(Integer, "sqlite")


class LogEntry(Base):
    __tablename__ = "log_entries"

    id = Column(AutoId, primary_key=True, autoincrement=True)
    ts = Column(BigInteger, nullable=False)  # Epoch milliseconds
    level = Column(String(10), nullable=False)  # ERROR, WARN, INFO, DEBUG
    logger = Column(String(255), nullable=True)
    raw_message = Column(Text, nullable=False)
    app_name = Column(String(64), nullable=True)
    stream_id = Column(String(128), nullable=True)
    rule_id = Column(String(64), nullable=True)
    friendly_title = Column(String(255), nullable=True)
    friendly_message = Column(Text, nullable=True)
    severity_group = Column(String(20), nullable=False, default="tidak_dikenal")  # info, perhatian, masalah, tidak_dikenal
    action_hint = Column(Text, nullable=True)
    category = Column(String(50), nullable=True)  # muxing, jaringan, stream, sistem, auth, viewer
    fingerprint = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_log_entries_ts", "ts"),
        Index("ix_log_entries_stream_ts", "stream_id", "ts"),
        Index("ix_log_entries_ts_level", "ts", "level"),
        Index("ix_log_entries_level_ts", "level", "ts"),
        Index("ix_log_entries_stream_fp_ts", "stream_id", "fingerprint", "ts"),
        Index("ix_log_entries_fingerprint", "fingerprint"),
        Index("ix_log_entries_fingerprint_ts", "fingerprint", "ts"),
    )


class UnmatchedPattern(Base):
    __tablename__ = "unmatched_patterns"

    id = Column(AutoId, primary_key=True, autoincrement=True)
    pattern_sample = Column(Text, nullable=False)
    level = Column(String(10), nullable=False)
    occurrences = Column(Integer, default=1, nullable=False)
    first_seen = Column(BigInteger, nullable=False)
    last_seen = Column(BigInteger, nullable=False)

    __table_args__ = (
        Index("ix_unmatched_level", "level"),
        Index("ix_unmatched_last_seen", "last_seen"),
    )


class IngestState(Base):
    __tablename__ = "ingest_states"

    key = Column(String(64), primary_key=True)  # e.g. "server_log", "error_log"
    last_offset = Column(BigInteger, default=0, nullable=False)
    last_file_size = Column(BigInteger, default=0, nullable=False)
    last_line_hash = Column(String(64), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AppUser(Base):
    __tablename__ = "app_users"

    id = Column(AutoId, primary_key=True, autoincrement=True)
    username = Column(String(64), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(32), default="operator", nullable=False)  # admin, operator
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class ServerMetricHistory(Base):
    __tablename__ = "server_metrics_history"

    id = Column(AutoId, primary_key=True, autoincrement=True)
    ts = Column(BigInteger, nullable=False)  # Epoch milliseconds
    cpu_percent = Column(Float, nullable=False, default=0.0)
    cpu_load_avg = Column(Float, nullable=False, default=0.0)
    system_memory_percent = Column(Float, nullable=False, default=0.0)
    system_memory_used_gb = Column(Float, nullable=False, default=0.0)
    jvm_heap_used_mb = Column(Float, nullable=False, default=0.0)
    disk_percent = Column(Float, nullable=False, default=0.0)
    disk_used_gb = Column(Float, nullable=False, default=0.0)
    total_live_streams = Column(Integer, nullable=False, default=0)
    total_viewers = Column(Integer, nullable=False, default=0)
    db_query_avg_ms = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_server_metrics_history_ts", "ts"),
    )


class StreamMetricHistory(Base):
    __tablename__ = "stream_metrics_history"

    id = Column(AutoId, primary_key=True, autoincrement=True)
    ts = Column(BigInteger, nullable=False)  # Epoch milliseconds
    app_name = Column(String(64), nullable=False)
    stream_id = Column(String(128), nullable=False)
    status = Column(String(32), nullable=False, default="finished")
    bitrate = Column(BigInteger, nullable=False, default=0)         # in bps
    bitrate_mbps = Column(Float, nullable=False, default=0.0)    # in Mbps
    speed = Column(Float, nullable=False, default=0.0)
    total_viewers = Column(Integer, nullable=False, default=0)
    hls_viewers = Column(Integer, nullable=False, default=0)
    webrtc_viewers = Column(Integer, nullable=False, default=0)
    dash_viewers = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_stream_metrics_ts", "ts"),
        Index("ix_stream_metrics_app_stream_ts", "app_name", "stream_id", "ts"),
    )


class AmsSyncLog(Base):
    """Audit log for each periodic or manual AMS REST API data synchronization."""
    __tablename__ = "ams_sync_logs"

    id = Column(AutoId, primary_key=True, autoincrement=True)
    app_name = Column(String(64), nullable=True)  # Specific app or "ALL"
    sync_type = Column(String(32), nullable=False, default="streams")  # streams, server_metrics, vods
    status = Column(String(20), nullable=False)  # SUCCESS, FAILED, WARNING
    items_synced = Column(Integer, default=0, nullable=False)
    duration_ms = Column(Integer, default=0, nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_ams_sync_logs_created_at", "created_at"),
        Index("ix_ams_sync_logs_app_type", "app_name", "sync_type"),
    )


class AmsStreamCurrent(Base):
    """Latest state of each broadcast/stream across all AMS applications."""
    __tablename__ = "ams_streams_current"

    app_name = Column(String(64), primary_key=True)
    stream_id = Column(String(128), primary_key=True)
    name = Column(String(255), nullable=True)
    status = Column(String(32), nullable=False, default="finished")  # broadcasting, finished, created
    stream_type = Column(String(32), nullable=True)                  # streamSource, ipCamera, liveStream
    stream_url = Column(Text, nullable=True)                         # Raw RTSP / RTMP url
    masked_url = Column(Text, nullable=True)                         # RTSP url with credentials stripped
    bitrate = Column(BigInteger, default=0, nullable=False)
    speed = Column(Float, default=0.0, nullable=False)
    total_viewers = Column(Integer, default=0, nullable=False)
    hls_viewers = Column(Integer, default=0, nullable=False)
    webrtc_viewers = Column(Integer, default=0, nullable=False)
    dash_viewers = Column(Integer, default=0, nullable=False)
    width = Column(Integer, default=0, nullable=False)
    height = Column(Integer, default=0, nullable=False)
    created_date = Column(BigInteger, nullable=True)
    last_seen = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_ams_streams_status", "status"),
        Index("ix_ams_streams_last_seen", "last_seen"),
    )


class AmsStreamStatusHistory(Base):
    """Audit log recording every status transition (e.g. broadcasting <-> finished) of a stream."""
    __tablename__ = "ams_stream_status_history"

    id = Column(AutoId, primary_key=True, autoincrement=True)
    app_name = Column(String(64), nullable=False)
    stream_id = Column(String(128), nullable=False)
    old_status = Column(String(32), nullable=True)
    new_status = Column(String(32), nullable=False)
    details = Column(Text, nullable=True)
    ts = Column(BigInteger, nullable=False)  # Epoch milliseconds
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_status_history_ts", "ts"),
        Index("ix_status_history_stream", "app_name", "stream_id", "ts"),
    )
