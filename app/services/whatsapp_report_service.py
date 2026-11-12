import re
from typing import Optional
from zoneinfo import ZoneInfo

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
