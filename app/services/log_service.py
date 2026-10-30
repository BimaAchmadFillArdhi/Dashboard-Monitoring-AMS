import csv
import io
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from pydantic import BaseModel
from sqlalchemy import select, func, and_, or_, desc, asc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import LogEntry
from app.translator.engine import get_translator


class LogItemDTO(BaseModel):
    id: int
    ts: int
    time_formatted: str
    level: str
    logger: Optional[str] = None
    stream_id: Optional[str] = None
    app_name: Optional[str] = None
    friendly_title: Optional[str] = None
    friendly_message: Optional[str] = None
    action_hint: Optional[str] = None
    severity_group: str = "tidak_dikenal"
    raw_message: str
    fingerprint: str
    repeat_count: int = 1
    first_seen_time: Optional[str] = None
    last_seen_time: Optional[str] = None


class LogQueryResponse(BaseModel):
    entries: List[LogItemDTO]
    counts: Dict[str, int]
    total_filtered: int
    has_more: bool
    next_cursor: Optional[int] = None


class LogService:
    @staticmethod
    def _parse_date_range_ms(date_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
        """Parse YYYY-MM-DD or return None for all dates."""
        if not date_str or date_str.upper() == "ALL":
            return None, None
        try:
            # Handle single day
            dt_start = datetime.strptime(date_str, "%Y-%m-%d")
            start_ms = int(dt_start.timestamp() * 1000)
            end_ms = start_ms + (86400 * 1000) - 1
            return start_ms, end_ms
        except Exception:
            return None, None

    async def get_filter_options(self, session: AsyncSession) -> Dict[str, List[str]]:
        """Return distinct stream IDs and app names available in the system."""
        stmt_streams = (
            select(LogEntry.stream_id)
            .where(LogEntry.stream_id.isnot(None))
            .distinct()
            .order_by(LogEntry.stream_id.asc())
            .limit(100)
        )
        res_streams = await session.execute(stmt_streams)
        streams = [s[0] for s in res_streams.all() if s[0]]

        stmt_apps = (
            select(LogEntry.app_name)
            .where(LogEntry.app_name.isnot(None))
            .distinct()
            .order_by(LogEntry.app_name.asc())
        )
        res_apps = await session.execute(stmt_apps)
        apps = [a[0] for a in res_apps.all() if a[0]]

        # Ensure known apps are included
        default_apps = ["atcs", "atcs-kota", "cctv-sekolah", "LiveApp", "WebRTCApp"]
        for da in default_apps:
            if da not in apps:
                apps.append(da)

        return {
            "stream_ids": streams,
            "app_names": sorted(apps),
        }

    async def query_logs(
        self,
        session: AsyncSession,
        date_str: Optional[str] = None,
        level: Optional[str] = None,
        stream_id: Optional[str] = None,
        app_name: Optional[str] = None,
        search_q: Optional[str] = None,
        fingerprint: Optional[str] = None,
        dedupe: bool = False,
        cursor_id: Optional[int] = None,
        limit: int = 50,
    ) -> LogQueryResponse:
        start_ms, end_ms = self._parse_date_range_ms(date_str)

        # Base filter conditions
        conditions = []
        if start_ms is not None and end_ms is not None:
            conditions.append(LogEntry.ts.between(start_ms, end_ms))
        if stream_id and stream_id != "ALL":
            conditions.append(LogEntry.stream_id == stream_id)
        if app_name and app_name != "ALL":
            if app_name.upper() in ("SYSTEM", "SISTEM", "NONE"):
                conditions.append(LogEntry.app_name.is_(None))
            else:
                conditions.append(LogEntry.app_name == app_name)
        if fingerprint:
            conditions.append(LogEntry.fingerprint == fingerprint)
        if search_q:
            q_like = f"%{search_q}%"
            conditions.append(
                or_(
                    LogEntry.friendly_message.ilike(q_like),
                    LogEntry.raw_message.ilike(q_like),
                    LogEntry.stream_id.ilike(q_like),
                    LogEntry.friendly_title.ilike(q_like),
                )
            )

        # 1. Calculate Level Counts across ALL matching records in database
        count_conditions = list(conditions)
        stmt_counts = select(LogEntry.level, func.count(LogEntry.id))
        if count_conditions:
            stmt_counts = stmt_counts.where(and_(*count_conditions))
        stmt_counts = stmt_counts.group_by(LogEntry.level)
        res_counts = await session.execute(stmt_counts)
        counts_dict = {"ERROR": 0, "WARN": 0, "INFO": 0, "DEBUG": 0, "TOTAL": 0}
        total_all = 0
        for lvl, cnt in res_counts.all():
            if lvl in counts_dict:
                counts_dict[lvl] = cnt
            total_all += cnt
        counts_dict["TOTAL"] = total_all

        # 2. Add level filter if specified
        if level and level != "ALL":
            conditions.append(LogEntry.level == level)

        # 3. Handle Deduplication Query vs Flat Query
        if dedupe:
            # Group by fingerprint, taking max(ts), count(*)
            stmt_grp = (
                select(
                    LogEntry.fingerprint,
                    LogEntry.stream_id,
                    LogEntry.app_name,
                    LogEntry.level,
                    LogEntry.friendly_title,
                    LogEntry.friendly_message,
                    LogEntry.action_hint,
                    LogEntry.severity_group,
                    func.max(LogEntry.id).label("max_id"),
                    func.count(LogEntry.id).label("cnt"),
                    func.min(LogEntry.ts).label("min_ts"),
                    func.max(LogEntry.ts).label("max_ts"),
                    func.max(LogEntry.raw_message).label("sample_raw"),
                )
            )
            if conditions:
                stmt_grp = stmt_grp.where(and_(*conditions))
            stmt_grp = stmt_grp.group_by(LogEntry.fingerprint).order_by(desc("max_ts")).limit(limit + 1)
            res_grp = await session.execute(stmt_grp)
            grp_rows = res_grp.all()

            has_more = len(grp_rows) > limit
            items = grp_rows[:limit]

            translator = get_translator()
            entries = []
            for r in items:
                dt_last = datetime.fromtimestamp(r.max_ts / 1000)
                dt_first = datetime.fromtimestamp(r.min_ts / 1000)
                raw_msg = r.sample_raw or ""
                f_title = r.friendly_title
                f_msg = r.friendly_message
                act_hint = r.action_hint
                sev = r.severity_group

                if (
                    not f_msg
                    or f_msg == raw_msg
                    or "stream CCTV  " in (f_msg or "")
                    or f_title in ("Pesan teknis belum diterjemahkan", "Pesan Sistem Umum")
                ):
                    tr = translator.translate(raw_msg, level=r.level, stream_id=r.stream_id)
                    f_title = tr.friendly_title
                    f_msg = tr.friendly_message
                    sev = tr.severity
                    act_hint = tr.action_hint or act_hint

                entries.append(
                    LogItemDTO(
                        id=r.max_id,
                        ts=r.max_ts,
                        time_formatted=dt_last.strftime("%H:%M:%S"),
                        level=r.level,
                        stream_id=r.stream_id,
                        app_name=r.app_name,
                        friendly_title=f_title,
                        friendly_message=f_msg,
                        action_hint=act_hint,
                        severity_group=sev,
                        raw_message=raw_msg,
                        fingerprint=r.fingerprint,
                        repeat_count=r.cnt,
                        first_seen_time=dt_first.strftime("%H:%M:%S"),
                        last_seen_time=dt_last.strftime("%H:%M:%S"),
                    )
                )

            next_cursor = None
            total_filtered = counts_dict.get(level, total_all) if (level and level != "ALL") else total_all

            return LogQueryResponse(
                entries=entries,
                counts=counts_dict,
                total_filtered=total_filtered,
                has_more=has_more,
                next_cursor=next_cursor,
            )

        else:
            # Flat query with cursor pagination
            query_conditions = list(conditions)
            if cursor_id:
                query_conditions.append(LogEntry.id < cursor_id)

            stmt = select(LogEntry)
            if query_conditions:
                stmt = stmt.where(and_(*query_conditions))
            stmt = stmt.order_by(desc(LogEntry.id)).limit(limit + 1)
            res = await session.execute(stmt)
            rows = res.scalars().all()

            has_more = len(rows) > limit
            items = rows[:limit]

            entries = []
            for log in items:
                dt = datetime.fromtimestamp(log.ts / 1000)
                raw_msg = log.raw_message or ""
                f_title = log.friendly_title
                f_msg = log.friendly_message
                act_hint = log.action_hint
                sev = log.severity_group

                if (
                    not f_msg
                    or f_msg == raw_msg
                    or "stream CCTV  " in (f_msg or "")
                    or f_title in ("Pesan teknis belum diterjemahkan", "Pesan Sistem Umum")
                ):
                    tr = translator.translate(raw_msg, level=log.level, stream_id=log.stream_id)
                    f_title = tr.friendly_title
                    f_msg = tr.friendly_message
                    sev = tr.severity
                    act_hint = tr.action_hint or act_hint

                entries.append(
                    LogItemDTO(
                        id=log.id,
                        ts=log.ts,
                        time_formatted=dt.strftime("%H:%M:%S"),
                        level=log.level,
                        logger=log.logger,
                        stream_id=log.stream_id,
                        app_name=log.app_name,
                        friendly_title=f_title,
                        friendly_message=f_msg,
                        action_hint=act_hint,
                        severity_group=sev,
                        raw_message=raw_msg,
                        fingerprint=log.fingerprint,
                        repeat_count=1,
                    )
                )

            next_cursor = items[-1].id if (items and has_more) else None
            total_filtered = counts_dict.get(level, total_all) if (level and level != "ALL") else total_all

            return LogQueryResponse(
                entries=entries,
                counts=counts_dict,
                total_filtered=total_filtered,
                has_more=has_more,
                next_cursor=next_cursor,
            )

    async def export_logs_csv(
        self,
        session: AsyncSession,
        date_str: Optional[str] = None,
        level: Optional[str] = None,
        stream_id: Optional[str] = None,
        search_q: Optional[str] = None,
    ) -> str:
        """Generate CSV export string according to active filters."""
        start_ms, end_ms = self._parse_date_range_ms(date_str)
        conditions = []
        if start_ms is not None and end_ms is not None:
            conditions.append(LogEntry.ts.between(start_ms, end_ms))
        if stream_id and stream_id != "ALL":
            conditions.append(LogEntry.stream_id == stream_id)
        if level and level != "ALL":
            conditions.append(LogEntry.level == level)
        if search_q:
            q_like = f"%{search_q}%"
            conditions.append(
                or_(
                    LogEntry.friendly_message.ilike(q_like),
                    LogEntry.raw_message.ilike(q_like),
                    LogEntry.stream_id.ilike(q_like),
                )
            )
        stmt = select(LogEntry)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        stmt = stmt.order_by(asc(LogEntry.ts)).limit(10000)
        res = await session.execute(stmt)
        logs = res.scalars().all()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "ID", "Waktu (ISO)", "Level", "CCTV / Stream ID", "Aplikasi",
            "Pesan Log", "Saran Tindakan", "Pesan Mentah (Raw)"
        ])

        for l in logs:
            dt = datetime.fromtimestamp(l.ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([
                l.id,
                dt,
                l.level,
                l.stream_id or "",
                l.app_name or "",
                l.friendly_message or l.friendly_title or "",
                l.action_hint or "",
                l.raw_message,
            ])

        return output.getvalue()


# Global singleton
_log_service: Optional[LogService] = None


def get_log_service() -> LogService:
    global _log_service
    if _log_service is None:
        _log_service = LogService()
    return _log_service
