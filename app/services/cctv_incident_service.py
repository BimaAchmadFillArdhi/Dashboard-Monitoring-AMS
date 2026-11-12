import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

from sqlalchemy import case, func, or_, select, union
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db.models import (
    AmsStreamCurrent,
    AmsSyncLog,
    CctvIncident,
    CctvIncidentNotification,
    LogEntry,
)
from app.services.whatsapp_notifier import WhatsAppNotifier
from app.services.whatsapp_report_service import (
    ERROR_CATEGORIES,
    JAKARTA_TIMEZONE,
    classify_error,
)

logger = logging.getLogger("ams.cctv_incidents")

CATEGORY_WARNING = "Peringatan"
CATEGORY_HIGH = "Prioritas Tinggi"
CATEGORY_CRITICAL = "Kritis"
REPORT_ERROR_LABELS = {
    ERROR_CATEGORIES[0]: "Koneksi timeout",
    ERROR_CATEGORIES[1]: "Inisialisasi stream gagal",
    ERROR_CATEGORIES[2]: "Gagal membuka stream",
    ERROR_CATEGORIES[3]: "Alamat stream tidak ditemukan",
    ERROR_CATEGORIES[4]: "Autentikasi ditolak",
    ERROR_CATEGORIES[5]: "Masalah lainnya",
}
INVALID_STREAM_IDS = (
    "",
    "unknown",
    "null",
    "none",
    "n/a",
    "na",
    "undefined",
    "tidak diketahui",
    "tidak dikenal",
)
_database_name = make_url(get_settings().DATABASE_URL).database or "default"
CCTV_INCIDENT_LOCK_NAME = (
    "ams_cctv_incidents_"
    + re.sub(r"[^A-Za-z0-9_]", "_", _database_name)[:45]
)


def duration_category(duration: timedelta) -> Optional[str]:
    hours = duration.total_seconds() / 3600
    if hours >= 48:
        return CATEGORY_CRITICAL
    if hours >= 24:
        return CATEGORY_HIGH
    if hours >= 6:
        return CATEGORY_WARNING
    return None


def format_duration(duration: timedelta) -> str:
    total_minutes = max(0, int(duration.total_seconds() // 60))
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours} jam {minutes:02d} menit"


class CctvIncidentService:
    def __init__(self):
        self.settings = get_settings()
        self._run_lock = asyncio.Lock()
        self._last_manual_report_at: Optional[datetime] = None

    async def evaluate_and_notify(
        self,
        session: AsyncSession,
        notifier: WhatsAppNotifier,
        now: Optional[datetime] = None,
        *,
        manual: bool = False,
    ) -> dict:
        """Update open incidents from the latest successful MySQL AMS snapshot."""
        async with self._run_lock:
            return await self._evaluate_and_notify(session, notifier, now, manual)

    async def _evaluate_and_notify(
        self,
        session: AsyncSession,
        notifier: WhatsAppNotifier,
        now: Optional[datetime] = None,
        manual: bool = False,
    ) -> dict:
        now = now or datetime.utcnow()
        if now.tzinfo is not None:
            now = now.astimezone(timezone.utc).replace(tzinfo=None)

        snapshot_max_age = max(
            120,
            self.settings.STREAM_METRICS_INTERVAL_SECONDS * 4,
        )
        snapshot_cutoff = now - timedelta(seconds=snapshot_max_age)
        latest_sync_ids = (
            select(func.max(AmsSyncLog.id).label("latest_id"))
            .where(
                AmsSyncLog.sync_type == "streams",
                AmsSyncLog.app_name.is_not(None),
                AmsSyncLog.app_name != "ALL",
            )
            .group_by(AmsSyncLog.app_name)
            .subquery()
        )
        sync_result = await session.execute(
            select(AmsSyncLog.app_name, AmsSyncLog.status, AmsSyncLog.created_at)
            .join(latest_sync_ids, AmsSyncLog.id == latest_sync_ids.c.latest_id)
            .where(
                AmsSyncLog.status == "SUCCESS",
                AmsSyncLog.created_at >= snapshot_cutoff,
            )
        )
        fresh_apps = {row.app_name for row in sync_result}
        if not fresh_apps:
            logger.warning(
                "[CCTVIncidents] No recent successful stream snapshot; "
                "preserving and reporting persisted incidents without state changes"
            )
        streams = []
        if fresh_apps:
            stream_result = await session.execute(
                select(AmsStreamCurrent).where(
                    AmsStreamCurrent.app_name.in_(fresh_apps)
                )
            )
            streams = stream_result.scalars().all()

        error_cutoff_ms = int(
            (now - timedelta(seconds=self.settings.CCTV_INCIDENT_RECENT_ERROR_SECONDS))
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
        error_filter = (
            LogEntry.app_name.in_(fresh_apps),
            LogEntry.app_name.is_not(None),
            LogEntry.stream_id.is_not(None),
            LogEntry.level == "ERROR",
            LogEntry.ts >= error_cutoff_ms,
        )
        error_summary = (
            select(
                LogEntry.app_name.label("app_name"),
                LogEntry.stream_id.label("stream_id"),
                func.max(LogEntry.id).label("latest_id"),
            )
            .where(*error_filter)
            .group_by(LogEntry.app_name, LogEntry.stream_id)
            .subquery()
        )
        latest_errors = {}
        if fresh_apps:
            error_result = await session.execute(
                select(
                    LogEntry.app_name,
                    LogEntry.stream_id,
                    LogEntry.friendly_title,
                    LogEntry.friendly_message,
                    LogEntry.raw_message,
                    LogEntry.category,
                )
                .join(
                    error_summary,
                    LogEntry.id == error_summary.c.latest_id,
                )
            )
            latest_errors = {
                (row.app_name, row.stream_id): row for row in error_result
            }

        streams_by_key = {
            (stream.app_name, stream.stream_id): stream for stream in streams
        }
        active_problems = {}
        for key, stream in streams_by_key.items():
            error = latest_errors.get(key)
            if stream.status == "broadcasting" and error is None:
                continue
            if error is not None:
                problem_type = (
                    error.friendly_title
                    or error.category
                    or classify_error(None, error.friendly_message, error.raw_message or "")
                )
                problem_detail = (error.friendly_message or "")[:500]
            else:
                problem_type = f"Stream AMS tidak aktif ({stream.status})"
                problem_detail = (
                    f"Status stream terakhir dari AMS: {stream.status}."
                )
            active_problems[key] = (stream, problem_type, problem_detail)

        open_incidents_stmt = select(CctvIncident).where(
            CctvIncident.closed_at.is_(None)
        )
        if fresh_apps:
            open_incidents_stmt = open_incidents_stmt.where(
                CctvIncident.app_name.in_(fresh_apps)
            )
        open_result = await session.execute(open_incidents_stmt)
        open_incidents = open_result.scalars().all()
        open_by_key = {}
        duplicate_open_incidents = []
        for incident in open_incidents:
            key = (incident.app_name, incident.stream_id)
            if key in open_by_key:
                duplicate_open_incidents.append(incident)
            else:
                open_by_key[key] = incident

        for key, incident in open_by_key.items():
            if incident.app_name in fresh_apps and key not in active_problems:
                incident.closed_at = now
        for incident in duplicate_open_incidents:
            if incident.app_name in fresh_apps:
                incident.closed_at = now

        for key, (stream, problem_type, problem_detail) in active_problems.items():
            incident = open_by_key.get(key)
            if incident is None or incident.closed_at is not None:
                incident = CctvIncident(
                    app_name=stream.app_name,
                    stream_id=stream.stream_id,
                    stream_name=stream.name or stream.stream_id,
                    problem_type=problem_type,
                    problem_detail=problem_detail,
                    first_error_at=now,
                    last_detected_at=now,
                )
                session.add(incident)
                open_by_key[key] = incident
            else:
                incident.stream_name = stream.name or stream.stream_id
                incident.problem_type = problem_type
                incident.problem_detail = problem_detail
                incident.last_detected_at = now

        await session.flush()
        active_result = await session.execute(
            select(CctvIncident).where(CctvIncident.closed_at.is_(None))
        )
        active_by_key = {}
        for incident in active_result.scalars():
            key = (incident.app_name, incident.stream_id)
            existing = active_by_key.get(key)
            if existing is None or incident.first_error_at < existing.first_error_at:
                active_by_key[key] = incident
        active_incidents = list(active_by_key.values())
        qualified = [
            incident
            for incident in active_incidents
            if duration_category(now - incident.first_error_at) is not None
        ]
        due_incidents = []
        should_send = False
        if notifier.is_configured:
            if manual:
                last_attempt = await session.scalar(
                    select(func.max(CctvIncidentNotification.attempted_at))
                )
                if (
                    self._last_manual_report_at is not None
                    and (
                        last_attempt is None
                        or self._last_manual_report_at > last_attempt
                    )
                ):
                    last_attempt = self._last_manual_report_at
                manual_cooldown = timedelta(
                    seconds=self.settings.CCTV_INCIDENT_MANUAL_COOLDOWN_SECONDS
                )
                if last_attempt is not None and now - last_attempt < manual_cooldown:
                    await session.commit()
                    return {
                        "checked": len(streams),
                        "active": len(active_incidents),
                        "qualified": len(qualified),
                        "notified": False,
                        "suppressed": True,
                    }
                due_incidents = qualified
                should_send = True
            else:
                repeat_interval = timedelta(
                    seconds=self.settings.CCTV_INCIDENT_REPEAT_INTERVAL_SECONDS
                )
                for incident in qualified:
                    category = duration_category(now - incident.first_error_at)
                    if category is None:
                        continue
                    if (
                        incident.last_notified_category != category
                        or incident.last_notified_at is None
                        or now - incident.last_notified_at >= repeat_interval
                    ):
                        due_incidents.append(incident)
                should_send = bool(due_incidents)

        if not should_send:
            await session.commit()
            return {
                "checked": len(streams),
                "active": len(active_incidents),
                "qualified": len(qualified),
                "notified": False,
            }

        category_changed = any(
            incident.last_notified_category
            != duration_category(now - incident.first_error_at)
            for incident in due_incidents
        )
        notification_type = (
            "manual"
            if manual
            else "category_update"
            if category_changed
            else "reminder"
        )
        batch_id = str(uuid4())
        report, app_summary = await self.build_application_report(session, now)
        if manual:
            self._last_manual_report_at = now
        history_rows = []
        for incident in qualified:
            category = duration_category(now - incident.first_error_at)
            if category is None:
                continue
            incident.last_notified_at = now
            incident.last_notified_category = category
            history = CctvIncidentNotification(
                incident_id=incident.id,
                batch_id=batch_id,
                notification_type=notification_type,
                duration_category=category,
                status="PENDING",
                attempted_at=now,
            )
            session.add(history)
            history_rows.append(history)

        await session.commit()
        try:
            send_result = await notifier.send_report(report)
            sent_count = int(send_result.get("sent", 0))
            failed_count = int(send_result.get("failed", 0))
            status = (
                "SENT"
                if failed_count == 0
                else "PARTIAL"
                if sent_count > 0
                else "FAILED"
            )
            result_detail = json.dumps(
                send_result.get("errors", []),
                ensure_ascii=False,
            )
        except Exception as err:
            logger.exception(
                "[CCTVIncidents] WhatsApp notification batch %s failed",
                batch_id,
            )
            sent_count = 0
            failed_count = len(notifier.chat_ids)
            status = "FAILED"
            result_detail = str(err)[:2000]

        for history in history_rows:
            history.status = status
            history.completed_at = datetime.utcnow()
            history.sent_count = sent_count
            history.failed_count = failed_count
            history.result_detail = result_detail
        await session.commit()
        return {
            "checked": len(streams),
            "active": len(active_incidents),
            "qualified": len(qualified),
            "applications": app_summary["total_applications"],
            "problematic_applications": app_summary["problematic_applications"],
            "normal_applications": app_summary["normal_applications"],
            "unique_problem_cctv": app_summary["unique_problem_cctv"],
            "unidentified_logs": app_summary["unidentified_logs"],
            "notified": True,
            "batch_id": batch_id,
            "status": status,
            "sent": sent_count,
            "failed": failed_count,
        }

    async def build_application_report(
        self,
        session: AsyncSession,
        now: Optional[datetime] = None,
    ) -> tuple[str, dict]:
        """Aggregate all application error logs in MySQL and format one report."""
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        local_now = now.astimezone(JAKARTA_TIMEZONE)

        known_apps = union(
            select(AmsStreamCurrent.app_name.label("app_name")).where(
                AmsStreamCurrent.app_name.is_not(None)
            ),
            select(AmsSyncLog.app_name.label("app_name")).where(
                AmsSyncLog.sync_type == "streams",
                AmsSyncLog.app_name.is_not(None),
                AmsSyncLog.app_name != "ALL",
            ),
            select(LogEntry.app_name.label("app_name")).where(
                LogEntry.app_name.is_not(None)
            ),
        ).subquery()
        app_result = await session.execute(
            select(known_apps.c.app_name).order_by(known_apps.c.app_name)
        )
        app_names = [row.app_name for row in app_result]

        message = func.lower(
            func.concat(
                func.coalesce(LogEntry.friendly_title, ""),
                " ",
                func.coalesce(LogEntry.friendly_message, ""),
                " ",
                func.coalesce(LogEntry.raw_message, ""),
            )
        )
        error_category = case(
            (
                or_(
                    message.like("%timeout%"),
                    message.like("%timed out%"),
                    message.like("%batas waktu%"),
                ),
                ERROR_CATEGORIES[0],
            ),
            (
                or_(
                    message.like("%inisialisasi pembukaan stream gagal%"),
                    message.like("%stream initialization failed%"),
                    message.like("%prepare for opening%"),
                    message.like("%preparing stream%"),
                ),
                ERROR_CATEGORIES[1],
            ),
            (
                or_(
                    message.like("%alamat stream kamera tidak ditemukan%"),
                    message.like("%stream not found%"),
                    message.like("%404 not found%"),
                    message.like("%404%"),
                ),
                ERROR_CATEGORIES[3],
            ),
            (
                or_(
                    message.like("%autentikasi stream%"),
                    message.like("%autentikasi ditolak%"),
                    message.like("%authentication failed%"),
                    message.like("%unauthorized%"),
                    message.like("%401%"),
                    message.like("%403%"),
                ),
                ERROR_CATEGORIES[4],
            ),
            (
                or_(
                    message.like("%gagal membuka aliran stream kamera%"),
                    message.like("%stream open failed%"),
                    message.like("%failed to open stream%"),
                    message.like("%cannot open stream%"),
                ),
                ERROR_CATEGORIES[2],
            ),
            else_=ERROR_CATEGORIES[5],
        ).label("error_category")
        normalized_stream_id = func.lower(func.trim(LogEntry.stream_id))
        invalid_stream_id = or_(
            LogEntry.stream_id.is_(None),
            normalized_stream_id.in_(INVALID_STREAM_IDS),
        )
        unidentified_condition = or_(
            invalid_stream_id,
            LogEntry.app_name.is_(None),
        )
        unidentified_by_app: dict[Optional[str], dict[str, int]] = {}
        stream_result = await session.execute(
            select(
                LogEntry.app_name,
                LogEntry.stream_id,
                error_category.label("error_category"),
            )
            .where(
                LogEntry.level == "ERROR",
                LogEntry.app_name.is_not(None),
                ~invalid_stream_id,
            )
            .group_by(LogEntry.app_name, LogEntry.stream_id, error_category)
        )
        stream_state_result = await session.execute(
            select(
                AmsStreamCurrent.app_name,
                AmsStreamCurrent.stream_id,
                AmsStreamCurrent.status,
            )
        )
        streams_by_app: dict[str, dict[str, str]] = {}
        global_stream_states: dict[str, set[str]] = {}
        for row in stream_state_result:
            stream_id = row.stream_id.strip()
            streams_by_app.setdefault(row.app_name, {})[stream_id] = row.status
            global_stream_states.setdefault(stream_id, set()).add(row.status)

        errors_by_app: dict[str, dict[str, set[str]]] = {}
        for row in stream_result:
            if row.app_name is None:
                continue
            stream_id = row.stream_id.strip()
            errors_by_app.setdefault(row.app_name, {}).setdefault(
                stream_id, set()
            ).add(row.error_category)

        unidentified_result = await session.execute(
            select(
                LogEntry.app_name,
                func.count(LogEntry.id).label("log_count"),
            )
            .where(LogEntry.level == "ERROR", unidentified_condition)
            .group_by(LogEntry.app_name)
        )
        for row in unidentified_result:
            bucket = unidentified_by_app.setdefault(
                row.app_name,
                {"count": 0},
            )
            bucket["count"] += row.log_count

        summary = self._application_report_summary(
            app_names,
            streams_by_app,
            errors_by_app,
            unidentified_by_app,
            global_stream_states,
        )
        return (
            self.format_application_report(
                app_names,
                streams_by_app,
                errors_by_app,
                unidentified_by_app,
                local_now,
            ),
            summary,
        )

    @staticmethod
    def _application_report_summary(
        app_names: list[str],
        streams_by_app: dict[str, dict[str, str]],
        errors_by_app: dict[str, dict[str, set[str]]],
        unidentified_by_app: dict[Optional[str], dict],
        global_stream_states: dict[str, set[str]],
    ) -> dict:
        total_on = sum(
            "broadcasting" in statuses
            for statuses in global_stream_states.values()
        )
        problematic_apps = sum(
            any(
                status != "broadcasting"
                for status in streams_by_app.get(app_name, {}).values()
            )
            or bool(errors_by_app.get(app_name))
            or bool(unidentified_by_app.get(app_name, {}).get("count"))
            for app_name in app_names
        )
        problem_stream_ids = {
            stream_id
            for app_name, app_streams in streams_by_app.items()
            for stream_id, status in app_streams.items()
            if status != "broadcasting"
        }
        problem_stream_ids.update(
            stream_id
            for app_errors in errors_by_app.values()
            for stream_id in app_errors
        )
        return {
            "total_applications": len(app_names),
            "total_cctv": len(global_stream_states),
            "cctv_on": total_on,
            "cctv_off": len(global_stream_states) - total_on,
            "problematic_applications": problematic_apps,
            "normal_applications": len(app_names) - problematic_apps,
            "unique_problem_cctv": len(problem_stream_ids),
            "unidentified_logs": sum(
                bucket.get("count", 0)
                for bucket in unidentified_by_app.values()
            ),
        }

    def format_application_report(
        self,
        app_names: list[str],
        streams_by_app: dict[str, dict[str, str]],
        errors_by_app: dict[str, dict[str, set[str]]],
        unidentified_by_app: dict[Optional[str], dict],
        report_time: datetime,
    ) -> str:
        if report_time.tzinfo is None:
            report_time = report_time.replace(tzinfo=timezone.utc)
        local_time = report_time.astimezone(JAKARTA_TIMEZONE)
        lines = [
            "⚠️ LAPORAN MONITORING CCTV AMS",
            f"Tanggal dan waktu: {local_time:%Y-%m-%d %H:%M:%S} WIB",
            f"Dashboard monitoring: {self.settings.APP_PUBLIC_URL.rstrip('/')}",
        ]
        problematic_apps = 0
        global_stream_states: dict[str, set[str]] = {}
        for app_name in sorted(app_names, key=str.casefold):
            app_streams = streams_by_app.get(app_name, {})
            app_errors = errors_by_app.get(app_name, {})
            unidentified = unidentified_by_app.get(app_name, {})
            app_on = sum(
                status == "broadcasting" for status in app_streams.values()
            )
            app_off = len(app_streams) - app_on
            is_problematic = bool(
                app_off or app_errors or unidentified.get("count")
            )
            problematic_apps += int(is_problematic)
            for stream_id, status in app_streams.items():
                global_stream_states.setdefault(stream_id, set()).add(status)
            lines.extend(
                [
                    "",
                    f"*Aplikasi: {app_name}*",
                    f"- Total CCTV: {len(app_streams)}",
                    f"- CCTV ON: {app_on}",
                    f"- CCTV OFF: {app_off}",
                    "**Ringkasan masalah:**",
                ]
            )
            for category in ERROR_CATEGORIES:
                cctv_count = sum(
                    category in reasons for reasons in app_errors.values()
                )
                lines.append(
                    f"- {REPORT_ERROR_LABELS[category]}: {cctv_count} CCTV"
                )

            if unidentified.get("count"):
                lines.append(
                    "Log belum teridentifikasi (stream_id kosong/tidak valid): "
                    f"{unidentified['count']}"
                )

        system_unidentified = unidentified_by_app.get(None, {})
        if system_unidentified.get("count"):
            lines.extend(
                [
                    "",
                    "*Log belum teridentifikasi (tanpa aplikasi)*",
                    f"Jumlah: {system_unidentified['count']}",
                ]
            )

        total_cctv = len(global_stream_states)
        total_on = sum(
            "broadcasting" in statuses
            for statuses in global_stream_states.values()
        )
        total_off = total_cctv - total_on
        lines.extend(
            [
                "",
                "*RINGKASAN KESELURUHAN AMS*",
                f"- Total aplikasi: {len(app_names)}",
                f"- Total seluruh CCTV unik: {total_cctv}",
                f"- Total CCTV ON: {total_on}",
                f"- Total CCTV OFF: {total_off}",
                f"- Jumlah aplikasi normal: {len(app_names) - problematic_apps}",
                f"- Jumlah aplikasi bermasalah: {problematic_apps}",
            ]
        )
        return "\n".join(lines)


_cctv_incident_service: Optional[CctvIncidentService] = None


def get_cctv_incident_service() -> CctvIncidentService:
    global _cctv_incident_service
    if _cctv_incident_service is None:
        _cctv_incident_service = CctvIncidentService()
    return _cctv_incident_service
