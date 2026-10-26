import hmac
import hashlib
import json
import base64
import time
from typing import Optional
import bcrypt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db.models import AppUser

settings = get_settings()


def hash_password(password: str) -> str:
    pw_bytes = password.encode("utf-8")[:72]
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pw_bytes, salt).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        pw_bytes = plain_password.encode("utf-8")[:72]
        return bcrypt.checkpw(pw_bytes, hashed_password.encode("utf-8"))
    except Exception:
        return False


def sign_session_cookie(user_id: int, username: str, role: str) -> str:
    """Create a signed session cookie payload: base64(payload).signature."""
    payload = {
        "user_id": user_id,
        "username": username,
        "role": role,
        "exp": int(time.time()) + (7 * 86400),  # 7 days
    }
    raw_json = json.dumps(payload).encode("utf-8")
    b64_payload = base64.urlsafe_b64encode(raw_json).decode("utf-8")
    signature = hmac.new(
        settings.SECRET_KEY.encode("utf-8"), b64_payload.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"{b64_payload}.{signature}"


def verify_session_cookie(cookie_str: Optional[str]) -> Optional[dict]:
    """Validate signed session cookie and return user payload or None."""
    if not cookie_str or "." not in cookie_str:
        return None
    try:
        b64_payload, signature = cookie_str.split(".", 1)
        expected_sig = hmac.new(
            settings.SECRET_KEY.encode("utf-8"), b64_payload.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            return None

        raw_json = base64.urlsafe_b64decode(b64_payload.encode("utf-8")).decode("utf-8")
        data = json.loads(raw_json)
        if data.get("exp", 0) < int(time.time()):
            return None
        return data
    except Exception:
        return None


async def ensure_default_users(session: AsyncSession):
    """Seed initial default admin and operator accounts if no users exist."""
    stmt = select(AppUser).limit(1)
    res = await session.execute(stmt)
    existing = res.first()
    if not existing:
        admin_user = AppUser(
            username="admin",
            password_hash=hash_password("admin12345"),
            role="admin",
        )
        operator_user = AppUser(
            username="operator",
            password_hash=hash_password("operator12345"),
            role="operator",
        )
        session.add_all([admin_user, operator_user])
        await session.commit()
        print("[Auth] Created default users: admin (admin12345) & operator (operator12345)")
