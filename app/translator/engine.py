import os
import re
import yaml
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path
from pydantic import BaseModel

logger = logging.getLogger("ams.translator")


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
        # Filter by level if specified
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

        # Safely render friendly message
        try:
            # Replace missing keys gracefully with empty string
            rendered_msg = self.message_template
            for k, v in subs.items():
                rendered_msg = rendered_msg.replace(f"{{{k}}}", str(v) if v is not None else "")
            # Clean any remaining unbound placeholders like {error}
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
        """Translate a raw technical log message into human-friendly Indonesian text."""
        self.check_and_reload_if_modified()

        for rule in self.rules:
            result = rule.match(raw_message, level, stream_id)
            if result:
                return result

        # Fallback for unrecognized messages
        return TranslationResult(
            rule_id=None,
            friendly_title="Pesan teknis belum diterjemahkan",
            friendly_message=raw_message,
            severity="tidak_dikenal",
            action_hint="Belum ada rekomendasi tindakan khusus untuk pesan ini.",
            category="lainnya",
            matched=False,
        )


# Global singleton
_translator_instance: Optional[TranslatorEngine] = None


def get_translator() -> TranslatorEngine:
    global _translator_instance
    if _translator_instance is None:
        _translator_instance = TranslatorEngine()
    return _translator_instance
