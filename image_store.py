# ============================================================
# Module: Image Store (image_store.py)
# 模块：图片持久化
#
# 通过 Supabase Storage REST API 存取图片，
# 配合 OmbreBrain 记忆桶存文字描述，实现跨会话图片持久化。
#
# 设计：
#   - 图片二进制存 Supabase Storage（不走 SDK，直接 REST）
#   - 文字描述存 OmbreBrain 记忆桶（domain="照片"）
#   - breath 浮现的是描述（~30 token），实际图片按需读取
# ============================================================

import os
import time
import uuid
import base64
import logging
from io import BytesIO

import httpx

logger = logging.getLogger("ombre_brain.image_store")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
SUPABASE_BUCKET = os.environ.get("SUPABASE_STORAGE_BUCKET", "ombre-images").strip()


def _headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }


def is_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


async def ensure_bucket():
    if not is_configured():
        return
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{SUPABASE_URL}/storage/v1/bucket/{SUPABASE_BUCKET}",
            headers=_headers(),
        )
        if resp.status_code == 404:
            await client.post(
                f"{SUPABASE_URL}/storage/v1/bucket",
                headers={**_headers(), "Content-Type": "application/json"},
                json={"id": SUPABASE_BUCKET, "name": SUPABASE_BUCKET, "public": False},
            )
            logger.info(f"Created Supabase Storage bucket (private): {SUPABASE_BUCKET}")


async def upload_image(data: bytes, filename: str, content_type: str = "image/jpeg") -> dict:
    if not is_configured():
        raise RuntimeError("Supabase Storage 未配置（需要 SUPABASE_URL + SUPABASE_KEY）")

    ext = filename.rsplit(".", 1)[-1] if "." in filename else "jpg"
    storage_path = f"{int(time.time())}_{uuid.uuid4().hex[:8]}.{ext}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{storage_path}",
            headers={**_headers(), "Content-Type": content_type},
            content=data,
        )
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"上传失败: {resp.status_code} {resp.text}")

    canonical_url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{storage_path}"
    return {"path": storage_path, "url": canonical_url}


async def list_images() -> list[dict]:
    if not is_configured():
        return []
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{SUPABASE_URL}/storage/v1/object/list/{SUPABASE_BUCKET}",
            headers={**_headers(), "Content-Type": "application/json"},
            json={"prefix": "", "limit": 500, "sortBy": {"column": "created_at", "order": "desc"}},
        )
        if resp.status_code != 200:
            return []
        items = resp.json()
        result = []
        for item in items:
            if item.get("id") is None:
                continue
            name = item.get("name", "")
            result.append({
                "name": name,
                "created_at": item.get("created_at", ""),
                "size": item.get("metadata", {}).get("size", 0),
            })
        return result


async def create_signed_url(path: str, expires_in: int = 3600) -> str:
    if not is_configured():
        return ""
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}/{path}",
            headers={**_headers(), "Content-Type": "application/json"},
            json={"expiresIn": expires_in},
        )
        if resp.status_code == 200:
            data = resp.json()
            token = data.get("signedURL", "")
            if token.startswith("/"):
                return f"{SUPABASE_URL}/storage/v1{token}"
            return token
    return f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}"


async def create_signed_urls(
    paths: list[str], expires_in: int = 3600, transform: dict | None = None
) -> dict[str, str]:
    """Batch-sign storage paths. `transform` (e.g. {"width":320,"height":320,
    "resize":"cover"}) asks Supabase's on-the-fly image transform to return a
    resized thumbnail instead of the original — requires the Image
    Transformation add-on on the Supabase project; unsupported projects will
    just fail this call (caller falls back to full-size, see /api/images)."""
    if not is_configured() or not paths:
        return {}
    body = {"expiresIn": expires_in, "paths": paths}
    if transform:
        body["transform"] = transform
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"{SUPABASE_URL}/storage/v1/object/sign/{SUPABASE_BUCKET}",
            headers={**_headers(), "Content-Type": "application/json"},
            json=body,
        )
        if resp.status_code == 200:
            result = {}
            for item in resp.json():
                path = item.get("path", "")
                signed = item.get("signedURL", "")
                if signed.startswith("/"):
                    signed = f"{SUPABASE_URL}/storage/v1{signed}"
                result[path] = signed
            return result
    return {}


async def delete_image(path: str) -> bool:
    if not is_configured():
        return False
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.delete(
            f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{path}",
            headers=_headers(),
        )
        return resp.status_code in (200, 204)


async def get_image_url(path: str) -> str:
    return await create_signed_url(path)
