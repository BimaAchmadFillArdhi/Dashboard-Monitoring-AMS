import asyncio
import logging
from typing import List, Sequence
from urllib.parse import quote

import httpx

from app.config import get_settings

logger = logging.getLogger("ams.whatsapp")
MAX_MESSAGE_LENGTH = 20000


class WhatsAppNotifier:
    def __init__(self):
        settings = get_settings()
        self.enabled = settings.GREEN_API_ENABLED
        self.api_url = settings.GREEN_API_URL.rstrip("/")
        self.instance_id = settings.GREEN_API_INSTANCE_ID.strip()
        self.api_token = settings.GREEN_API_TOKEN.strip()
        self.chat_ids = self._parse_chat_ids(settings.GREEN_API_CHAT_IDS)

        missing_settings = [
            setting
            for setting, value in (
                ("GREEN_API_INSTANCE_ID", self.instance_id),
                ("GREEN_API_TOKEN", self.api_token),
                ("GREEN_API_CHAT_IDS", self.chat_ids),
            )
            if not value
        ]
        if self.enabled and missing_settings:
            logger.error(
                "Green API notifications are enabled but missing configuration: %s",
                ", ".join(missing_settings),
            )

    @staticmethod
    def _parse_chat_ids(configured_chat_ids: str) -> List[str]:
        chat_ids = []
        for value in configured_chat_ids.split(","):
            chat_id = value.strip()
            if not chat_id:
                continue
            if chat_id.endswith(("@c.us", "@g.us", "@lid")):
                chat_ids.append(chat_id)
            else:
                phone = "".join(char for char in chat_id if char.isdigit())
                if phone:
                    chat_ids.append(f"{phone}@c.us")
        return chat_ids

    @property
    def is_configured(self) -> bool:
        return self.enabled and all(
            (self.api_url, self.instance_id, self.api_token, self.chat_ids)
        )

    def _message_url(self) -> str:
        instance_id = quote(self.instance_id, safe="")
        api_token = quote(self.api_token, safe="")
        return (
            f"{self.api_url}/waInstance{instance_id}/"
            f"sendMessage/{api_token}"
        )

    async def send_report(self, message: str) -> dict:
        if not self.is_configured:
            raise RuntimeError("Green API belum aktif atau konfigurasinya belum lengkap.")

        messages = self._split_report_messages(message)

        timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            results = await asyncio.gather(
                *(
                    self._post_report(client, chat_id, messages)
                    for chat_id in self.chat_ids
                ),
                return_exceptions=True,
            )

        sent = 0
        failed = 0
        errors = []
        for chat_id, result in zip(self.chat_ids, results):
            if isinstance(result, Exception):
                failed += 1
                status_code = (
                    result.response.status_code
                    if isinstance(result, httpx.HTTPStatusError)
                    else None
                )
                error = self._describe_failure(result)
                errors.append(error)
                logger.error(
                    "Green API report send failed for chat %s (%s%s).",
                    chat_id,
                    type(result).__name__,
                    f", status={status_code}" if status_code else "",
                )
            else:
                sent += 1
        return {"sent": sent, "failed": failed, "errors": errors}

    def _describe_failure(self, error: Exception) -> str:
        if isinstance(error, httpx.HTTPStatusError):
            status_code = error.response.status_code
            try:
                body = error.response.json()
            except ValueError:
                body = {}
            detail = None
            if isinstance(body, dict):
                for key in ("message", "error", "description"):
                    value = body.get(key)
                    if isinstance(value, str) and value.strip():
                        detail = value.strip()
                        break
            if detail:
                detail = detail.replace(self.api_token, "[redacted]")
                detail = detail.replace(self.instance_id, "[redacted]")
                detail = detail[:300]
                return f"Green API menolak permintaan (HTTP {status_code}): {detail}"
            return f"Green API menolak permintaan (HTTP {status_code})."
        if isinstance(error, httpx.ConnectTimeout):
            return "Koneksi ke server Green API tidak terbentuk dalam 10 detik."
        if isinstance(error, httpx.ReadTimeout):
            return (
                "Green API tidak memberi jawaban dalam 30 detik. Status pengiriman "
                "belum dapat dipastikan; periksa WhatsApp tujuan sebelum mencoba lagi."
            )
        if isinstance(error, httpx.TimeoutException):
            return "Permintaan ke Green API melewati batas waktu."
        return f"Koneksi ke Green API gagal ({type(error).__name__})."

    @staticmethod
    def _split_report_messages(message: str) -> List[str]:
        lines = message.splitlines(keepends=True)
        if not lines:
            return [message]

        messages = []
        current_message = ""
        current_heading = ""
        for line in lines:
            line_length = len(line.encode("utf-16-le")) // 2
            if line_length > MAX_MESSAGE_LENGTH:
                raise ValueError(
                    "Satu baris laporan melebihi batas panjang pesan WhatsApp."
                )

            current_length = len(current_message.encode("utf-16-le")) // 2
            if current_message and current_length + line_length > MAX_MESSAGE_LENGTH:
                messages.append(current_message)
                line_text = line.strip()
                is_heading = line_text.startswith("*") and line_text.endswith("*")
                heading = (
                    f"{current_heading}\n"
                    if current_heading and not is_heading
                    else ""
                )
                heading_length = len(heading.encode("utf-16-le")) // 2
                if heading_length + line_length > MAX_MESSAGE_LENGTH:
                    heading = ""
                current_message = heading + line
            else:
                current_message += line

            heading_text = line.strip()
            if heading_text.startswith("*") and heading_text.endswith("*"):
                current_heading = heading_text

        if current_message:
            messages.append(current_message)
        return messages

    async def _post_report(
        self,
        client: httpx.AsyncClient,
        chat_id: str,
        messages: Sequence[str],
    ) -> None:
        for message in messages:
            payload = {"chatId": chat_id, "message": message}
            response = await client.post(self._message_url(), json=payload)
            response.raise_for_status()
