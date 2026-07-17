# ============================================================
# Test: couple avatar API — POST /api/avatar, GET /api/avatars
# 情头接口测试：设置 / 读取聊天头像（她 / 他）
# ============================================================

import json
import pytest
from unittest.mock import patch

import server


class FakeRequest:
    """Minimal stand-in for a starlette Request — only what our routes touch."""

    def __init__(self, json_body=None):
        self._json_body = json_body

    async def json(self):
        if self._json_body is None:
            raise ValueError("no body")
        return self._json_body


def _json(resp):
    return json.loads(resp.body)


# --- pure helper: _avatar_config_key ---

def test_avatar_config_key_valid_roles():
    assert server._avatar_config_key("her") == "avatar_her"
    assert server._avatar_config_key("him") == "avatar_him"


def test_avatar_config_key_invalid_roles():
    assert server._avatar_config_key("") == ""
    assert server._avatar_config_key("them") == ""
    assert server._avatar_config_key(None) == ""
    assert server._avatar_config_key("Her") == ""  # case-sensitive, no fuzzy matching


# --- POST /api/avatar ---

@pytest.mark.asyncio
async def test_api_avatar_set_invalid_role():
    with patch.object(server, "_require_auth", return_value=None):
        req = FakeRequest({"role": "bad", "image_id": "x"})
        resp = await server.api_avatar_set(req)
    assert resp.status_code == 400
    assert "role" in _json(resp)["error"]


@pytest.mark.asyncio
async def test_api_avatar_set_missing_image_id():
    with patch.object(server, "_require_auth", return_value=None):
        req = FakeRequest({"role": "her", "image_id": ""})
        resp = await server.api_avatar_set(req)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_api_avatar_set_bad_json():
    with patch.object(server, "_require_auth", return_value=None):
        req = FakeRequest(None)  # .json() raises
        resp = await server.api_avatar_set(req)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_api_avatar_set_bucket_not_found(bucket_mgr):
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        req = FakeRequest({"role": "her", "image_id": "nonexistent-id"})
        resp = await server.api_avatar_set(req)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_api_avatar_set_no_storage_path(bucket_mgr):
    bid = await bucket_mgr.create(content="没有图片链接的桶", domain=["照片"], tags=["照片"])
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        req = FakeRequest({"role": "her", "image_id": bid})
        resp = await server.api_avatar_set(req)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_api_avatar_set_success(bucket_mgr):
    content = (
        "## 照片\n\n测试\n\n"
        "![photo](https://xxx.supabase.co/storage/v1/object/public/photos/abc123.jpg)"
    )
    bid = await bucket_mgr.create(content=content, domain=["照片"], tags=["照片"])
    saved = {}

    def fake_set_config(key, value):
        saved[key] = value
        return True

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "set_config", side_effect=fake_set_config):
        req = FakeRequest({"role": "her", "image_id": bid})
        resp = await server.api_avatar_set(req)

    assert resp.status_code == 200
    assert _json(resp) == {"ok": True}
    # _extract_storage_path strips the leading bucket-name segment ("photos/")
    assert saved.get("avatar_her") == "abc123.jpg"


# --- GET /api/avatars ---

@pytest.mark.asyncio
async def test_api_avatars_get_empty_when_unset():
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "get_config", return_value=None):
        resp = await server.api_avatars_get(FakeRequest())
    assert resp.status_code == 200
    assert _json(resp) == {"her": "", "him": ""}


@pytest.mark.asyncio
async def test_api_avatars_get_empty_when_storage_not_configured():
    def fake_get_config(key):
        return {"avatar_her": "her.jpg", "avatar_him": "him.jpg"}.get(key)

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "get_config", side_effect=fake_get_config), \
         patch.object(server, "_img_is_configured", return_value=False):
        resp = await server.api_avatars_get(FakeRequest())
    assert resp.status_code == 200
    # storage not configured -> no signing attempted -> both blank, no crash
    assert _json(resp) == {"her": "", "him": ""}


@pytest.mark.asyncio
async def test_api_avatars_get_signed_urls():
    def fake_get_config(key):
        return {"avatar_her": "her.jpg", "avatar_him": "him.jpg"}.get(key)

    async def fake_sign_urls(paths, expires_in=3600):
        return {p: f"https://signed.example/{p}" for p in paths}

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "get_config", side_effect=fake_get_config), \
         patch.object(server, "_img_is_configured", return_value=True), \
         patch("image_store.create_signed_urls", side_effect=fake_sign_urls):
        resp = await server.api_avatars_get(FakeRequest())

    assert resp.status_code == 200
    body = _json(resp)
    assert body == {
        "her": "https://signed.example/her.jpg",
        "him": "https://signed.example/him.jpg",
    }


@pytest.mark.asyncio
async def test_api_avatars_get_signing_failure_is_swallowed():
    def fake_get_config(key):
        return {"avatar_her": "her.jpg", "avatar_him": ""}.get(key)

    async def fake_sign_urls_raises(paths, expires_in=3600):
        raise RuntimeError("supabase unreachable")

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "get_config", side_effect=fake_get_config), \
         patch.object(server, "_img_is_configured", return_value=True), \
         patch("image_store.create_signed_urls", side_effect=fake_sign_urls_raises):
        resp = await server.api_avatars_get(FakeRequest())

    # signing failure must not 500 the endpoint
    assert resp.status_code == 200
    assert _json(resp) == {"her": "", "him": ""}
