import re
from datetime import datetime, timedelta
from typing import Dict, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AmsStreamCurrent, LogEntry

JAKARTA_TIMEZONE = ZoneInfo("Asia/Jakarta")
ERROR_CATEGORIES = (
    "Batas Waktu Koneksi Kamera Habis (Timeout)",
    "Inisialisasi Pembukaan Stream Gagal",
    "Gagal Membuka Aliran Stream Kamera",
    "Alamat Stream Kamera Tidak Ditemukan",
    "Autentikasi Stream Ditolak",
    "Lainnya",
)


def classify_error(title: Optional[str], message: Optional[str], raw_message: str) -> str:
    text = " ".join((title or "", message or "", raw_message or "")).lower()
    normalized = re.sub(r"\s+", " ", text)

    if any(term in normalized for term in ("timeout", "timed out", "batas waktu")):
        return ERROR_CATEGORIES[0]
    if any(
        term in normalized
        for term in (
            "inisialisasi pembukaan stream gagal",
            "prepare for opening",
            "preparing stream",
        )
    ):
        return ERROR_CATEGORIES[1]
    if any(
        term in normalized
        for term in (
            "alamat stream kamera tidak ditemukan",
            "stream not found",
            "404 not found",
            "404",
        )
    ):
        return ERROR_CATEGORIES[3]
    if any(
        term in normalized
        for term in (
            "autentikasi stream",
            "autentikasi ditolak",
            "authentication failed",
            "unauthorized",
            "401",
            "403",
        )
    ):
        return ERROR_CATEGORIES[4]
    if any(
        term in normalized
        for term in (
            "gagal membuka aliran stream kamera",
            "failed to open stream",
            "cannot open stream",
        )
    ):
        return ERROR_CATEGORIES[2]
    return ERROR_CATEGORIES[5]


def resolve_stream_name(
    app_name: Optional[str],
    stream_id: Optional[str],
    stream_names: Dict[Tuple[Optional[str], str], Optional[str]],
) -> str:
    if not stream_id:
        return "Tidak diketahui"
    stream_id = stream_id.strip()

    exact_name = stream_names.get((app_name, stream_id))
    if exact_name and exact_name.strip():
        return exact_name.strip()

    candidates = {
        name.strip()
        for (candidate_app, candidate_id), name in stream_names.items()
        if candidate_id.startswith(stream_id)
        and (not app_name or candidate_app == app_name)
        and name
        and name.strip()
    }
    if len(candidates) == 1:
        return next(iter(candidates))
    return stream_id


async def prepare_report_errors(
    session: AsyncSession, logs: Sequence[LogEntry]
) -> list[Tuple[str, Optional[str], Optional[str], str]]:
    result = await session.execute(
        select(
            AmsStreamCurrent.app_name,
            AmsStreamCurrent.stream_id,
            AmsStreamCurrent.name,
        )
    )
    stream_names = {
        (app_name, stream_id): name
        for app_name, stream_id, name in result.all()
    }
    return [
        (
            resolve_stream_name(log.app_name, log.stream_id, stream_names),
            log.friendly_title,
            log.friendly_message,
            log.raw_message,
        )
        for log in logs
    ]


def format_cctv_report(
    errors: Sequence[Tuple[str, Optional[str], Optional[str], str]],
    report_time: datetime,
    error_period: str = "12 Jam Terakhir",
) -> str:
    local_time = report_time.astimezone(JAKARTA_TIMEZONE)
    grouped = {category: set() for category in ERROR_CATEGORIES}
    for camera_name, title, message, raw_message in errors:
        grouped[classify_error(title, message, raw_message)].add(camera_name)

    lines = [
        f"*Laporan Otomatis CCTV ({local_time:%Y-%m-%d %H:%M:%S} WIB)*",
        "",
        f"*Daftar Error {error_period}*:",
    ]
    for index, category in enumerate(ERROR_CATEGORIES, start=1):
        names = sorted(grouped[category], key=str.casefold)
        lines.append(f"*{index}. {category}*")
        lines.extend(
            f"{item}. {name}" for item, name in enumerate(names, start=1)
        )
        if not names:
            lines.append("- Tidak ada")

    return "\n".join(lines)


async def build_cctv_report(
    session: AsyncSession, report_time: Optional[datetime] = None
) -> str:
    report_time = report_time or datetime.now(JAKARTA_TIMEZONE)
    if report_time.tzinfo is None:
        report_time = report_time.replace(tzinfo=JAKARTA_TIMEZONE)

    cutoff_ms = int((report_time - timedelta(hours=12)).timestamp() * 1000)
    log_result = await session.execute(
        select(LogEntry)
        .where(LogEntry.level == "ERROR", LogEntry.ts >= cutoff_ms)
        .order_by(LogEntry.ts.asc())
    )
    errors = await prepare_report_errors(session, log_result.scalars().all())
    return format_cctv_report(errors, report_time)


def next_report_time(now: datetime) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=JAKARTA_TIMEZONE)
    local_now = now.astimezone(JAKARTA_TIMEZONE)
    candidates = [
        local_now.replace(hour=6, minute=0, second=0, microsecond=0),
        local_now.replace(hour=20, minute=0, second=0, microsecond=0),
    ]
    future = [candidate for candidate in candidates if candidate > local_now]
    if future:
        return future[0]
    return (local_now + timedelta(days=1)).replace(
        hour=6, minute=0, second=0, microsecond=0
    )
