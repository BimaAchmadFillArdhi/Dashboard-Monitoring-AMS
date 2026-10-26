import re
import time
import hashlib
import logging
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Any
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db.models import LogEntry, UnmatchedPattern, IngestState
from app.clients.ams_client import get_ams_client
from app.translator.engine import get_translator

logger = logging.getLogger("ams.ingestor")

# Timestamp pattern: 2026-10-26 12:54:09,259
LOG_LINE_REGEX = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<time>\d{2}:\d{2}:\d{2}(?:,\d{3})?)\s+"
    r"\[(?P<thread>[^\]]+)\]\s+"
    r"(?P<level>ERROR|WARN|INFO|DEBUG|TRACE)\s+"
    r"(?P<logger>[\w\.\-]+)\s+-\s+"
    r"(?P<message>.*)$"
)

# Stream ID extraction patterns (case-insensitive)
STREAM_ID_PATTERNS = [
    re.compile(r"(?i)streamId:([A-Za-z0-9_\-\.]+)"),
    re.compile(r"(?i)stream:([A-Za-z0-9_\-\.]+)"),
    re.compile(r"\(([A-Za-z0-9_\-\.]+)\.m3u8\)"),
    re.compile(r"for ([A-Za-z0-9_\-\.]+)\s+-"),
    re.compile(r"for ([A-Za-z0-9_\-\.]+)\.m3u8"),
]

# Known applications for app_name detection
KNOWN_APPS = ["LiveApp", "WebRTCApp", "atcs", "atcs-kota", "cctv-sekolah", "live"]


def parse_timestamp_ms(date_str: str, time_str: str) -> int:
    """Convert '2026-10-26' and '12:54:09,259' into epoch milliseconds."""
    try:
        clean_time = time_str.replace(",", ".")
        dt_str = f"{date_str} {clean_time}"
        if "." in clean_time:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
        else:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)


def extract_stream_id(raw_message: str) -> Optional[str]:
    """Extract CCTV stream ID from message text."""
    for pattern in STREAM_ID_PATTERNS:
        m = pattern.search(raw_message)
        if m:
            sid = m.group(1).strip()
            # Filter out non-stream false positives
            if sid and len(sid) > 1 and not sid.isdigit():
                return sid
    return None


def extract_app_name(raw_message: str) -> Optional[str]:
    """Detect application name if present in message or path."""
    for app in KNOWN_APPS:
        if f"/{app}/" in raw_message or f" {app} " in raw_message or f":{app}:" in raw_message:
            return app
    return None


def normalize_pattern(message: str) -> str:
    """Normalize variable IDs, timestamps, hex addresses, and numbers to compute a stable fingerprint."""
    # Replace stream IDs
    msg = re.sub(r"streamId:[A-Za-z0-9_\-\.]+", "streamId:{id}", message)
    msg = re.sub(r"stream:[A-Za-z0-9_\-\.]+", "stream:{id}", msg)
    msg = re.sub(r"\([A-Za-z0-9_\-\.]+\.m3u8\)", "({id}.m3u8)", msg)
    # Replace hex memory addresses
    msg = re.sub(r"0x[0-9a-fA-F]+", "0xADDR", msg)
    # Replace digits
    msg = re.sub(r"\d+", "N", msg)
    # Collapse multiple whitespaces
    msg = re.sub(r"\s+", " ", msg).strip()
    return msg


def compute_fingerprint(rule_id: Optional[str], raw_message: str, stream_id: Optional[str], level: str) -> str:
    """Calculate deduplication fingerprint hash."""
    if rule_id:
        seed = f"{rule_id}:{stream_id or ''}:{level}"
    else:
        norm = normalize_pattern(raw_message)
        seed = f"{norm}:{stream_id or ''}:{level}"
    return hashlib.md5(seed.encode("utf-8")).hexdigest()


class RawLogEntry:
    def __init__(self, ts: int, level: str, logger: str, raw_message: str):
        self.ts = ts
        self.level = level
        self.logger = logger
        self.raw_message = raw_message

    def append_message(self, line: str):
        self.raw_message += "\n" + line


def parse_raw_log_content(content: str) -> List[RawLogEntry]:
    """Parse log text, combining multi-line stack traces into single entries."""
    if not content:
        return []

    entries: List[RawLogEntry] = []
    current_entry: Optional[RawLogEntry] = None

    for line in content.splitlines():
        line = line.rstrip()
        if not line:
            continue

        m = LOG_LINE_REGEX.match(line)
        if m:
            if current_entry:
                entries.append(current_entry)

            ts = parse_timestamp_ms(m.group("date"), m.group("time"))
            current_entry = RawLogEntry(
                ts=ts,
                level=m.group("level"),
                logger=m.group("logger"),
                raw_message=m.group("message"),
            )
        else:
            # Continuation of previous message (e.g. Java stack trace)
            if current_entry:
                current_entry.append_message(line)

    if current_entry:
        entries.append(current_entry)

    return entries


class LogIngestor:
    def __init__(self):
        self.settings = get_settings()
        self.ams_client = get_ams_client()
        self.translator = get_translator()

    async def ingest_chunk_data(
        self, entries: List[RawLogEntry], session: AsyncSession
    ) -> int:
        """Translate parsed entries and batch insert into SQLite database."""
        if not entries:
            return 0

        db_entries = []
        unmatched_dict: Dict[Tuple[str, str], List[int]] = {}

        for e in entries:
            stream_id = extract_stream_id(e.raw_message)
            app_name = extract_app_name(e.raw_message)

            # Translate to friendly Indonesian
            trans = self.translator.translate(e.raw_message, e.level, stream_id)

            fp = compute_fingerprint(trans.rule_id, e.raw_message, stream_id, e.level)

            db_entry = LogEntry(
                ts=e.ts,
                level=e.level,
                logger=e.logger,
                raw_message=e.raw_message,
                app_name=app_name,
                stream_id=stream_id,
                rule_id=trans.rule_id,
                friendly_title=trans.friendly_title,
                friendly_message=trans.friendly_message,
                severity_group=trans.severity,
                action_hint=trans.action_hint,
                category=trans.category,
                fingerprint=fp,
            )
            db_entries.append(db_entry)

            if not trans.matched:
                key = (normalize_pattern(e.raw_message), e.level)
                if key not in unmatched_dict:
                    unmatched_dict[key] = []
                unmatched_dict[key].append(e.ts)

        # Batch insert log entries
        session.add_all(db_entries)

        # Record or update unmatched patterns
        for (pattern_sample, level), timestamps in unmatched_dict.items():
            stmt = select(UnmatchedPattern).where(
                UnmatchedPattern.pattern_sample == pattern_sample,
                UnmatchedPattern.level == level,
            )
            res = await session.execute(stmt)
            existing = res.scalar_one_or_none()
            if existing:
                existing.occurrences += len(timestamps)
                existing.last_seen = max(existing.last_seen, max(timestamps))
            else:
                up = UnmatchedPattern(
                    pattern_sample=pattern_sample,
                    level=level,
                    occurrences=len(timestamps),
                    first_seen=min(timestamps),
                    last_seen=max(timestamps),
                )
                session.add(up)

        await session.commit()
        return len(db_entries)

    async def poll_server_logs(self, session: AsyncSession, log_type: str = "server") -> int:
        """Fetch latest chunk from AMS, handle log rotation, and ingest into database."""
        state_key = f"{log_type}_log_offset"
        stmt = select(IngestState).where(IngestState.key == state_key)
        res = await session.execute(stmt)
        state = res.scalar_one_or_none()

        if not state:
            state = IngestState(key=state_key, last_offset=0, last_file_size=0)
            session.add(state)
            await session.commit()

        # Fetch log chunk (64KB at a time)
        chunk = await self.ams_client.get_log_chunk(
            offset=state.last_offset, size=65536, log_type=log_type
        )
        if not chunk or not chunk.logFileSize:
            return 0

        # Detect log rotation: file size shrunk below previous offset
        if chunk.logFileSize < state.last_offset:
            logger.info(
                f"[LogIngestor] Log rotation detected for {log_type} "
                f"(fileSize={chunk.logFileSize} < last_offset={state.last_offset}). Resetting to 0."
            )
            state.last_offset = 0

        if not chunk.logContent:
            # File hasn't grown
            state.last_file_size = chunk.logFileSize
            await session.commit()
            return 0

        # Parse and ingest chunk
        raw_entries = parse_raw_log_content(chunk.logContent)
        ingested_count = await self.ingest_chunk_data(raw_entries, session)

        # Update offset
        state.last_offset += chunk.logContentSize
        state.last_file_size = chunk.logFileSize
        await session.commit()

        if ingested_count > 0:
            logger.info(
                f"[LogIngestor] Ingested {ingested_count} log entries from {log_type} "
                f"(offset: {state.last_offset}/{chunk.logFileSize})"
            )

        return ingested_count

    async def cleanup_old_logs(self, session: AsyncSession) -> int:
        """Purge logs older than LOG_RETENTION_DAYS."""
        retention_days = self.settings.LOG_RETENTION_DAYS
        if retention_days <= 0:
            return 0

        cutoff_ts = int((time.time() - (retention_days * 86400)) * 1000)
        stmt = delete(LogEntry).where(LogEntry.ts < cutoff_ts)
        res = await session.execute(stmt)
        deleted_count = res.rowcount
        await session.commit()
        if deleted_count > 0:
            logger.info(f"[LogIngestor] Deleted {deleted_count} log entries older than {retention_days} days.")
        return deleted_count
