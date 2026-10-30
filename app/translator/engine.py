import os
import re
import yaml
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path
from pydantic import BaseModel

logger = logging.getLogger("ams.translator")


def make_friendly_stream_name(stream_id: Optional[str]) -> str:
    """Format a stream ID into a clean, human-readable camera name."""
    if not stream_id or stream_id.lower() in ("kamera", "system", "sistem"):
        return "kamera"
    name = stream_id.strip()
    for suffix in [".stream", ".m3u8", ".mp4", ".flv", ".ts"]:
        if name.lower().endswith(suffix):
            name = name[:-len(suffix)]
    for prefix in ["ATCS_", "atcs_", "cctv_", "CCTV_", "TC-", "tc-"]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    name = name.replace("_", " ").replace("-", " ").strip()
    # Return formatted title
    return name.title() if name else stream_id


class TranslationResult(BaseModel):
    rule_id: Optional[str] = None
    friendly_title: str
    friendly_message: str
    severity: str = "tidak_dikenal"  # info, perhatian, masalah, tidak_dikenal
    action_hint: Optional[str] = None
    category: Optional[str] = "lainnya"
    matched: bool = False


class Rule:
    def __init__(self, data: Dict[str, Any]):
        self.id = data["id"]
        self.match_pattern = data["match"]
        self.level = data.get("level")
        self.title = data.get("title", "")
        self.message_template = data.get("message", "")
        self.severity = data.get("severity", "info")
        self.saran_tindakan = data.get("saran_tindakan")
        self.kategori = data.get("kategori", "lainnya")

        try:
            self.regex = re.compile(self.match_pattern)
        except re.error as e:
            logger.error(f"[Translator] Invalid regex in rule {self.id}: {e}")
            self.regex = None

    def match(self, raw_message: str, level: str, stream_id: Optional[str] = None) -> Optional[TranslationResult]:
        # Filter by level if specified in rule
        if self.level and level and self.level.upper() != level.upper():
            return None

        if not self.regex:
            return None

        m = self.regex.search(raw_message)
        if not m:
            return None

        # Build substitutions from named capture groups + fallback stream_id
        subs = m.groupdict()
        if "stream_id" not in subs or not subs["stream_id"]:
            subs["stream_id"] = stream_id or "Kamera"

        sid = subs.get("stream_id", "")
        clean_sid = make_friendly_stream_name(sid)

        # If stream_id was generic but stream_url was captured, extract name from URL
        if clean_sid.lower() == "kamera" and "stream_url" in subs and subs["stream_url"]:
            url_part = subs["stream_url"].split("?")[0].split("&")[0].split("/")[-1]
            if url_part:
                clean_sid = make_friendly_stream_name(url_part)
                if subs["stream_id"] == "Kamera":
                    subs["stream_id"] = url_part

        subs["clean_stream_id"] = clean_sid

        # Safely render friendly message
        try:
            rendered_msg = self.message_template
            for k, v in subs.items():
                rendered_msg = rendered_msg.replace(f"{{{k}}}", str(v) if v is not None else "")
            # Clean any remaining unbound placeholders like {placeholder}
            rendered_msg = re.sub(r'\{[A-Za-z0-9_]+\}', '', rendered_msg).strip()
        except Exception:
            rendered_msg = self.message_template

        return TranslationResult(
            rule_id=self.id,
            friendly_title=self.title,
            friendly_message=rendered_msg,
            severity=self.severity,
            action_hint=self.saran_tindakan,
            category=self.kategori,
            matched=True,
        )


class TranslatorEngine:
    def __init__(self, rules_path: Optional[str] = None):
        if rules_path is None:
            rules_path = str(Path(__file__).resolve().parent / "rules.yaml")
        self.rules_path = rules_path
        self.rules: List[Rule] = []
        self._last_loaded_mtime = 0.0
        self.reload_rules()

    def reload_rules(self) -> int:
        """Load or reload rules from rules.yaml without application restart."""
        if not os.path.exists(self.rules_path):
            logger.warning(f"[Translator] Rules file not found at {self.rules_path}")
            return 0

        try:
            mtime = os.path.getmtime(self.rules_path)
            with open(self.rules_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            rule_items = data.get("rules", [])
            new_rules = [Rule(item) for item in rule_items if "id" in item and "match" in item]
            self.rules = new_rules
            self._last_loaded_mtime = mtime
            logger.info(f"[Translator] Loaded {len(self.rules)} translation rules from {self.rules_path}")
            return len(self.rules)
        except Exception as e:
            logger.error(f"[Translator] Failed to load rules: {e}")
            return len(self.rules)

    def check_and_reload_if_modified(self):
        """Hot-reload rules if the yaml file on disk was modified."""
        try:
            if os.path.exists(self.rules_path):
                mtime = os.path.getmtime(self.rules_path)
                if mtime > self._last_loaded_mtime:
                    self.reload_rules()
        except Exception:
            pass

    def translate(
        self, raw_message: str, level: str = "INFO", stream_id: Optional[str] = None
    ) -> TranslationResult:
        """Translate a raw technical log message into human-friendly Indonesian text with cause explanation."""
        self.check_and_reload_if_modified()

        for rule in self.rules:
            result = rule.match(raw_message, level, stream_id)
            if result:
                return result

        # Fallback for unrecognized messages: provide general explanation without fabricating unknown causes
        clean_name = make_friendly_stream_name(stream_id) if stream_id else "sistem"
        lvl = (level or "INFO").upper()

        if lvl == "ERROR":
            fallback_title = "Peringatan Kesalahan Operasional"
            fallback_msg = (
                f"Terjadi kesalahan internal pada Ant Media Server saat memproses operasi {clean_name}. "
                "Inti masalah teknis belum memiliki aturan terjemahan spesifik. "
                "Silakan periksa bagian 'Pesan Log Asli (Teknis)' di bawah untuk informasi error lengkap."
            )
            fallback_hint = "Periksa kestabilan server sumber dan pastikan konfigurasi stream sudah benar."
            severity = "masalah"
        elif lvl in ("WARN", "WARNING"):
            fallback_title = "Peringatan Kondisi Operasional"
            fallback_msg = (
                f"Terdapat peringatan kondisi operasional pada {clean_name}. "
                "Sistem mendeteksi anomali namun proses streaming tetap berjalan. "
                "Lihat 'Pesan Log Asli (Teknis)' di bawah untuk rincian."
            )
            fallback_hint = "Amati apakah peringatan ini berulang secara terus-menerus."
            severity = "perhatian"
        elif lvl == "DEBUG":
            fallback_title = "Informasi Penelusuran Diagnostik (Debug)"
            fallback_msg = (
                f"Pencatatan aktivitas diagnostik dan penelusuran internal AMS untuk {clean_name}. "
                "Pesan ini ditujukan untuk kebutuhan analisa teknis mendalam."
            )
            fallback_hint = "Tidak memerlukan tindakan operasional."
            severity = "info"
        else:
            fallback_title = "Informasi Aktivitas Sistem"
            fallback_msg = (
                f"Pemberitahuan aktivitas operasional rutin Ant Media Server terkait {clean_name}. "
                "Layanan berjalan normal. Rincian teknis tersedia pada 'Pesan Log Asli'."
            )
            fallback_hint = "Tidak ada tindakan yang diperlukan."
            severity = "info"

        return TranslationResult(
            rule_id=None,
            friendly_title=fallback_title,
            friendly_message=fallback_msg,
            severity=severity,
            action_hint=fallback_hint,
            category="sistem",
            matched=False,
        )


# Global singleton
_translator_instance: Optional[TranslatorEngine] = None


def get_translator() -> TranslatorEngine:
    global _translator_instance
    if _translator_instance is None:
        _translator_instance = TranslatorEngine()
    return _translator_instance
