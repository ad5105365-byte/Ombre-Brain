# ============================================================
# Test: /phone-report /phone-activity — 手机活动上报
# ============================================================

import json
import pytest
from unittest.mock import patch

import server


class _Req:
    def __init__(self, body=None, token=None):
        self._body = body or {}
        self.headers = {"authorization": f"Bearer {token}"} if token else {}

    async def json(self):
        return self._body


def _body(resp):
    return json.loads(resp.body)


@pytest.mark.asyncio
async def test_closed_when_token_unset(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", ""):
        resp = await server.phone_report(_Req({"app": "小红书"}, token="whatever"))
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_wrong_token_rejected(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", "secret"):
        resp = await server.phone_report(_Req({"app": "小红书"}, token="wrong"))
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_report_and_query_roundtrip(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", "secret"):
        for app in ("小红书", "微信", "王者荣耀"):
            resp = await server.phone_report(_Req({"app": app}, token="secret"))
            assert _body(resp)["ok"]

        listed = _body(await server.phone_activity(_Req(token="secret")))
        assert [e["app"] for e in listed[:3]] == ["王者荣耀", "微信", "小红书"]

        summary = _body(await server.phone_activity_summary(_Req(token="secret")))
        assert summary["count"] == 3
        assert summary["recent_apps"][0] == "王者荣耀"
        assert summary["last_active"]


@pytest.mark.asyncio
async def test_ring_buffer_keeps_last_100(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", "secret"):
        for i in range(105):
            await server.phone_report(_Req({"app": f"app{i}"}, token="secret"))
        listed = _body(await server.phone_activity(_Req(token="secret")))
    assert len(listed) == 100
    assert listed[0]["app"] == "app104"
    assert listed[-1]["app"] == "app5"
