# ============================================================
# Test: mood check-in API — POST/GET /api/checkin
# 心情打卡接口测试：记一条打卡 / 读最近一条
# ============================================================

import json
import pytest
from unittest.mock import patch

import server
import checkin_store


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


# --- POST /api/checkin ---

@pytest.mark.asyncio
async def test_api_checkin_create_bad_json():
    with patch.object(server, "_require_auth", return_value=None):
        resp = await server.api_checkin_create(FakeRequest(None))
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_api_checkin_create_both_empty_400(bucket_mgr):
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        resp = await server.api_checkin_create(FakeRequest({"mood": "  ", "text": ""}))
    assert resp.status_code == 400
    assert "mood" in _json(resp)["error"] or "text" in _json(resp)["error"]


@pytest.mark.asyncio
async def test_api_checkin_create_mood_and_text_success(bucket_mgr):
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        resp = await server.api_checkin_create(FakeRequest({"mood": "emo", "text": "有点累"}))
    assert resp.status_code == 200
    body = _json(resp)
    assert body["ok"] is True
    assert body["mood"] == "emo"
    assert body["text"] == "有点累"
    assert body["ts"]
    # 真落盘了，且还没被消费（还没喂给克克）
    rec = checkin_store.load_checkin(bucket_mgr.base_dir)
    assert rec["consumed"] is False


@pytest.mark.asyncio
async def test_api_checkin_create_mood_only_success(bucket_mgr):
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        resp = await server.api_checkin_create(FakeRequest({"mood": "开心"}))
    assert resp.status_code == 200
    assert _json(resp)["text"] == ""


@pytest.mark.asyncio
async def test_api_checkin_create_requires_auth(bucket_mgr):
    from starlette.responses import JSONResponse
    with patch.object(server, "_require_auth",
                       return_value=JSONResponse({"error": "Unauthorized"}, status_code=401)):
        resp = await server.api_checkin_create(FakeRequest({"mood": "开心"}))
    assert resp.status_code == 401


# --- GET /api/checkin ---

@pytest.mark.asyncio
async def test_api_checkin_latest_empty_when_none(bucket_mgr):
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        resp = await server.api_checkin_latest(FakeRequest())
    assert resp.status_code == 200
    assert _json(resp) == {"mood": "", "text": "", "ts": ""}


@pytest.mark.asyncio
async def test_api_checkin_latest_reflects_last_post(bucket_mgr):
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        await server.api_checkin_create(FakeRequest({"mood": "想你", "text": ""}))
        resp = await server.api_checkin_latest(FakeRequest())
    body = _json(resp)
    assert body["mood"] == "想你"
    assert body["ts"]


@pytest.mark.asyncio
async def test_api_checkin_latest_does_not_consume(bucket_mgr):
    """GET 只是给前端看展示用，不该把 consumed 标记掉——那是 pending_line 的活。"""
    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        await server.api_checkin_create(FakeRequest({"mood": "生气", "text": ""}))
        await server.api_checkin_latest(FakeRequest())
    rec = checkin_store.load_checkin(bucket_mgr.base_dir)
    assert rec["consumed"] is False
