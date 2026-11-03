import csv
import io
import time
from collections import OrderedDict
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from pydantic import BaseModel
from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import LogEntry


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
    category: Optional[str] = None
    action_hint: Optional[str] = None
    severity_group: str = "tidak_dikenal"
    raw_message: Optional[str] = None
    fingerprint: str
    repeat_count: int = 1
    first_seen_time: Optional[str] = None
    last_seen_time: Optional[str] = None


class LogDetailDTO(BaseModel):
    id: int
    logger: Optional[str] = None
    raw_message: str
    fingerprint: str


class LogQueryResponse(BaseModel):
    entries: List[LogItemDTO]
    counts: Dict[str, int]
    category_counts: Dict[str, int]
    total_filtered: int
    has_more: bool
    next_cursor: Optional[int] = None
    latest_id: Optional[int] = None
    next_after_id: Optional[int] = None


class LogService:
    STATS_CACHE_TTL_SECONDS = 30
    STATS_CACHE_MAX_ENTRIES = 128

    def __init__(self):
        self._stats_cache: OrderedDict[
            Tuple[Any, ...], Tuple[float, Dict[str, int], Dict[str, int], int]
        ] = OrderedDict()

    def invalidate_stats_cache(self) -> None:
        self._stats_cache.clear()

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
        after_id: Optional[int] = None,
        limit: int = 50,
    ) -> LogQueryResponse:
        start_ms, end_ms = self._parse_date_range_ms(date_str)

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

        stats_key = (
            start_ms,
            end_ms,
            stream_id if stream_id != "ALL" else None,
            app_name if app_name != "ALL" else None,
            search_q,
            fingerprint,
        )
        stats = self._stats_cache.get(stats_key)
        now = time.monotonic()
        if stats is not None and stats[0] > now:
            self._stats_cache.move_to_end(stats_key)
            counts_dict = dict(stats[1])
            category_counts = dict(stats[2])
            total_all = stats[3]
        else:
            if stats is not None:
                del self._stats_cache[stats_key]
            stmt_counts = (
                select(LogEntry.level, LogEntry.category, func.count(LogEntry.id))
                .group_by(LogEntry.level, LogEntry.category)
            )
            if conditions:
                stmt_counts = stmt_counts.where(and_(*conditions))
            res_counts = await session.execute(stmt_counts)
            counts_dict = {"ERROR": 0, "WARN": 0, "INFO": 0, "DEBUG": 0, "TOTAL": 0}
            category_counts: Dict[str, int] = {}
            total_all = 0
            for lvl, category, count in res_counts.all():
                if lvl in counts_dict:
                    counts_dict[lvl] += count
                category_counts[category or "tanpa_kategori"] = (
                    category_counts.get(category or "tanpa_kategori", 0) + count
                )
                total_all += count
            counts_dict["TOTAL"] = total_all
            self._stats_cache[stats_key] = (
                now + self.STATS_CACHE_TTL_SECONDS,
                dict(counts_dict),
                dict(category_counts),
                total_all,
            )
            while len(self._stats_cache) > self.STATS_CACHE_MAX_ENTRIES:
                self._stats_cache.popitem(last=False)

        if level and level != "ALL":
            conditions.append(LogEntry.level == level)

        latest_id = await session.scalar(select(func.max(LogEntry.id)))

        if dedupe:
            max_id = func.max(LogEntry.id)
            if after_id is not None:
                recent = select(
                    LogEntry.fingerprint,
                    max_id.label("latest_update_id"),
                )
                recent_conditions = list(conditions)
                recent_conditions.append(LogEntry.id > after_id)
                if latest_id is not None:
                    recent_conditions.append(LogEntry.id <= latest_id)
                recent = (
                    recent.where(and_(*recent_conditions))
                    .group_by(LogEntry.fingerprint)
                    .subquery()
                )
                grouped = (
                    select(
                        LogEntry.fingerprint,
                        max_id.label("max_id"),
                        func.count(LogEntry.id).label("cnt"),
                        func.min(LogEntry.ts).label("min_ts"),
                        func.max(LogEntry.ts).label("max_ts"),
                        recent.c.latest_update_id,
                    )
                    .join(recent, recent.c.fingerprint == LogEntry.fingerprint)
                    .where(*conditions)
                    .group_by(LogEntry.fingerprint, recent.c.latest_update_id)
                    .order_by(recent.c.latest_update_id.asc())
                    .limit(limit + 1)
                )
            else:
                grouped = select(
                    LogEntry.fingerprint,
                    max_id.label("max_id"),
                    func.count(LogEntry.id).label("cnt"),
                    func.min(LogEntry.ts).label("min_ts"),
                    func.max(LogEntry.ts).label("max_ts"),
                )
                if conditions:
                    grouped = grouped.where(and_(*conditions))
                grouped = grouped.group_by(LogEntry.fingerprint)
                if cursor_id is not None:
                    grouped = grouped.having(max_id < cursor_id)
                grouped = grouped.order_by(desc(max_id)).limit(limit + 1)
            grouped = grouped.subquery()
            stmt = (
                select(
                    LogEntry.id,
                    LogEntry.ts,
                    LogEntry.level,
                    LogEntry.logger,
                    LogEntry.stream_id,
                    LogEntry.app_name,
                    LogEntry.friendly_title,
                    LogEntry.friendly_message,
                    LogEntry.category,
                    LogEntry.action_hint,
                    LogEntry.severity_group,
                    LogEntry.fingerprint,
                    grouped.c.cnt,
                    grouped.c.min_ts,
                    grouped.c.max_ts,
                )
                .join(grouped, LogEntry.id == grouped.c.max_id)
            )
            if after_id is not None:
                stmt = stmt.order_by(desc(grouped.c.max_id))
            else:
                stmt = stmt.order_by(desc(grouped.c.max_id)).limit(limit + 1)
            grp_rows = (await session.execute(stmt)).mappings().all()

            has_more = len(grp_rows) > limit
            items = grp_rows[:limit]
            entries = []
            for r in items:
                dt_last = datetime.fromtimestamp(r["max_ts"] / 1000)
                dt_first = datetime.fromtimestamp(r["min_ts"] / 1000)
                entries.append(
                    LogItemDTO(
                        id=r["id"],
                        ts=r["max_ts"],
                        time_formatted=dt_last.strftime("%H:%M:%S"),
                        level=r["level"],
                        logger=r["logger"],
                        stream_id=r["stream_id"],
                        app_name=r["app_name"],
                        friendly_title=r["friendly_title"],
                        friendly_message=(
                            r["friendly_message"]
                            or r["friendly_title"]
                            or "Pesan teknis belum diterjemahkan"
                        ),
                        category=r["category"],
                        action_hint=r["action_hint"],
                        severity_group=r["severity_group"],
                        fingerprint=r["fingerprint"],
                        repeat_count=r["cnt"],
                        first_seen_time=dt_first.strftime("%H:%M:%S"),
                        last_seen_time=dt_last.strftime("%H:%M:%S"),
                    )
                )

            next_cursor = (
                items[-1]["id"] if (items and has_more and after_id is None) else None
            )
            next_after_id = None
            if after_id is not None:
                next_after_id = (
                    items[-1]["id"] if has_more and items else latest_id
                )
            total_filtered = counts_dict.get(level, total_all) if (level and level != "ALL") else total_all

            return LogQueryResponse(
                entries=entries,
                counts=counts_dict,
                category_counts=category_counts,
                total_filtered=total_filtered,
                has_more=has_more,
                next_cursor=next_cursor,
                latest_id=latest_id,
                next_after_id=next_after_id,
            )

        else:
            query_conditions = list(conditions)
            if cursor_id is not None:
                query_conditions.append(LogEntry.id < cursor_id)
            if after_id is not None:
                query_conditions.append(LogEntry.id > after_id)
                if latest_id is not None:
                    query_conditions.append(LogEntry.id <= latest_id)

            stmt = select(
                LogEntry.id,
                LogEntry.ts,
                LogEntry.level,
                LogEntry.logger,
                LogEntry.stream_id,
                LogEntry.app_name,
                LogEntry.friendly_title,
                LogEntry.friendly_message,
                LogEntry.category,
                LogEntry.action_hint,
                LogEntry.severity_group,
                LogEntry.fingerprint,
            )
            if query_conditions:
                stmt = stmt.where(and_(*query_conditions))
            stmt = stmt.order_by(
                LogEntry.id.asc() if after_id is not None else LogEntry.id.desc()
            ).limit(limit + 1)
            rows = (await session.execute(stmt)).mappings().all()

            has_more = len(rows) > limit
            items = rows[:limit]

            entries = []
            for log in items:
                dt = datetime.fromtimestamp(log["ts"] / 1000)
                entries.append(
                    LogItemDTO(
                        id=log["id"],
                        ts=log["ts"],
                        time_formatted=dt.strftime("%H:%M:%S"),
                        level=log["level"],
                        logger=log["logger"],
                        stream_id=log["stream_id"],
                        app_name=log["app_name"],
                        friendly_title=log["friendly_title"],
                        friendly_message=(
                            log["friendly_message"]
                            or log["friendly_title"]
                            or "Pesan teknis belum diterjemahkan"
                        ),
                        category=log["category"],
                        action_hint=log["action_hint"],
                        severity_group=log["severity_group"],
                        fingerprint=log["fingerprint"],
                        repeat_count=1,
                    )
                )

            next_cursor = (
                items[-1]["id"]
                if (items and has_more and after_id is None)
                else None
            )
            next_after_id = None
            if after_id is not None:
                next_after_id = (
                    items[-1]["id"] if has_more and items else latest_id
                )
            total_filtered = counts_dict.get(level, total_all) if (level and level != "ALL") else total_all

            return LogQueryResponse(
                entries=entries,
                counts=counts_dict,
                category_counts=category_counts,
                total_filtered=total_filtered,
                has_more=has_more,
                next_cursor=next_cursor,
                latest_id=latest_id,
                next_after_id=next_after_id,
            )

    async def get_log_detail(
        self, session: AsyncSession, log_id: int
    ) -> Optional[LogDetailDTO]:
        row = (
            await session.execute(
                select(
                    LogEntry.id,
                    LogEntry.logger,
                    LogEntry.raw_message,
                    LogEntry.fingerprint,
                ).where(LogEntry.id == log_id)
            )
        ).mappings().one_or_none()
        if row is None:
            return None
        return LogDetailDTO(**row)

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
