# ============================================================
# Module: Voice Store (voice_store.py)
# 语音音频持久化：Supabase Storage 独立桶（ombre-voice），
# 不进相册（相册按 OB 记忆桶列，这里只放文件不建桶）。
# 省她手机内存：录音存云端，语音条回放走签名 URL。
# ============================================================
import os
import time
import uuid
import logging

import httpx

logger = logging.getLogger("ombre_brain.voice_store")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
VOICE_BUCKET = os.environ.get("SUPABASE_VOICE_BUCKET", "ombre-voice").strip()


def _headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}


def is_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


async def ensure_bucket():
    if not is_configured():
        return
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(f"{SUPABASE_URL}/storage/v1/bucket/{VOICE_BUCKET}", headers=_headers())
        if r.status_code == 404:
            await c.post(
                f"{SUPABASE_URL}/storage/v1/bucket",
                headers={**_headers(), "Content-Type": "application/json"},
                json={"id": VOICE_BUCKET, "name": VOICE_BUCKET, "public": False},
            )
            logger.info(f"Created Supabase voice bucket (private): {VOICE_BUCKET}")


async def upload_audio(data: bytes, ext: str = "webm", content_type: str = "audio/webm") -> dict:
    if not is_configured():
        raise RuntimeError("Supabase Storage 未配置（需 SUPABASE_URL + SUPABASE_KEY）")
    path = f"{int(time.time())}_{uuid.uuid4().hex[:8]}.{ext}"
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(
            f"{SUPABASE_URL}/storage/v1/object/{VOICE_BUCKET}/{path}",
            headers={**_headers(), "Content-Type": content_type},
            content=data,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"上传失败: {r.status_code} {r.text}")
    return {"path": path}


async def signed_url(path: str, expires_in: int = 86400) -> str:
    """签个回放 URL（默认 24h）。拿不到就返空。"""
    if not is_configured():
        return ""
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.post(
            f"{SUPABASE_URL}/storage/v1/object/sign/{VOICE_BUCKET}/{path}",
            headers={**_headers(), "Content-Type": "application/json"},
            json={"expiresIn": expires_in},
        )
        if r.status_code == 200:
            tok = r.json().get("signedURL", "")
            if tok:
                return f"{SUPABASE_URL}/storage/v1{tok}" if tok.startswith("/") else tok
    return ""
