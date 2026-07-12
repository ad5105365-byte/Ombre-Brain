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
async def test_ring_buffer_keeps_last_n(bucket_mgr):
    keep = server.PHONE_ACTIVITY_KEEP
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", "secret"):
        for i in range(keep + 5):
            await server.phone_report(_Req({"app": f"app{i}"}, token="secret"))
        listed = _body(await server.phone_activity(_Req(token="secret")))
    assert len(listed) == keep
    assert listed[0]["app"] == f"app{keep + 4}"
    assert listed[-1]["app"] == "app5"


def _insert(rows):
    conn = server._phone_db()
    conn.executemany(
        "INSERT INTO phone_activity (app_name, opened_at) VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


class _DailyReq(_Req):
    def __init__(self, token=None, date=None):
        super().__init__(token=token)
        self.query_params = {"date": date} if date else {}


# 日报表：间隔记给前一个App，最后一笔和长间隔封顶30分钟
@pytest.mark.asyncio
async def test_daily_usage_estimate(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", "secret"):
        _insert([
            ("小红书", "2026-07-10 10:00:00"),
            ("微信", "2026-07-10 10:10:00"),      # 小红书 +10分钟
            ("抖音", "2026-07-10 10:15:00"),      # 微信 +5分钟
            ("王者荣耀", "2026-07-10 12:00:00"),  # 抖音间隔105分钟 → 封顶30
            ("Claude", "2026-07-11 09:00:00"),    # 隔天首笔，封住王者的时长（封顶30）
        ])
        body = _body(await server.phone_activity_daily(
            _DailyReq(token="secret", date="2026-07-10")))
    by_app = {e["app"]: e for e in body["apps"]}
    assert set(by_app) == {"小红书", "微信", "抖音", "王者荣耀"}  # 隔天的Claude不算进来
    assert by_app["小红书"]["minutes"] == 10
    assert by_app["微信"]["minutes"] == 5
    assert by_app["抖音"]["minutes"] == 30
    assert by_app["王者荣耀"]["minutes"] == 30
    assert body["apps"][0]["app"] in ("抖音", "王者荣耀")  # 按时长倒序
    assert body["total_minutes"] == 75


@pytest.mark.asyncio
async def test_daily_counts_opens(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", "secret"):
        _insert([
            ("抖音", "2026-07-10 08:00:00"),
            ("微信", "2026-07-10 08:05:00"),
            ("抖音", "2026-07-10 08:06:00"),
            ("微信", "2026-07-10 08:20:00"),
        ])
        body = _body(await server.phone_activity_daily(
            _DailyReq(token="secret", date="2026-07-10")))
    by_app = {e["app"]: e for e in body["apps"]}
    assert by_app["抖音"]["opens"] == 2
    assert by_app["抖音"]["minutes"] == 19  # 5 + 14
    assert by_app["微信"]["opens"] == 2


@pytest.mark.asyncio
async def test_daily_empty_day(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "OMBRE_PHONE_TOKEN", "secret"):
        body = _body(await server.phone_activity_daily(
            _DailyReq(token="secret", date="2026-01-01")))
    assert body["apps"] == []
    assert body["total_minutes"] == 0
