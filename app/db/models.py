from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Text,
    DateTime,
    Index,
)
from app.db.session import Base


class LogEntry(Base):
    __tablename__ = "log_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
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

    id = Column(Integer, primary_key=True, autoincrement=True)
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

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(32), default="operator", nullable=False)  # admin, operator
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
