# ============================================================
# Test: 时间与手机活动注入 — opus46 供词第 1、2 条的药
#
# "记得主动调时间工具/查定位"这类指令模型天生执行不了；
# 解法是把当前时间和她手机最近活动直接塞进每次自动注入。
# ============================================================

import sqlite3
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import server


def _bucket(bid, name, content, pinned=False):
    return {
        "id": bid,
        "content": content,
        "metadata": {
            "name": name, "pinned": pinned, "resolved": False,
            "type": "dynamic", "tags": [], "importance": 7,
            "valence": 0.5, "arousal": 0.5,
        },
    }


def test_now_line_format():
    line = server._now_line()
    assert line.startswith("⏰ 深圳现在：")
    assert "周" in line


def test_phone_line_silent_without_token(monkeypatch):
    monkeypatch.setattr(server, "OMBRE_PHONE_TOKEN", "")
    assert server._phone_recent_line() is None


def test_phone_line_reads_latest(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "OMBRE_PHONE_TOKEN", "secret")
    db = sqlite3.connect(tmp_path / "phone.db")
    db.execute("CREATE TABLE phone_activity (id INTEGER PRIMARY KEY, app_name TEXT, opened_at TEXT)")
    db.execute("INSERT INTO phone_activity (app_name, opened_at) VALUES (?, ?)",
               ("小红书", "2026-07-10 19:59:40"))
    db.commit()
    monkeypatch.setattr(server, "_phone_db",
                        lambda: sqlite3.connect(tmp_path / "phone.db"))
    line = server._phone_recent_line()
    assert line == "📱 她手机最近：小红书（07-10 19:59）"


@pytest.mark.asyncio
async def test_breath_hook_carries_time(monkeypatch):
    monkeypatch.setattr(server, "OMBRE_PHONE_TOKEN", "")
    decay = MagicMock()
    decay.calculate_score = MagicMock(return_value=1.0)
    buckets = [_bucket("p1", "核心准则", "干净的原文", pinned=True)]
    with patch.object(server.bucket_mgr, "list_all", AsyncMock(return_value=buckets)), \
         patch.object(server.dehydrator, "dehydrate", AsyncMock(return_value="干净摘要")), \
         patch.object(server, "decay_engine", decay), \
         patch.object(server, "_ensure_reminder_loop", MagicMock()), \
         patch.object(server, "_fire_webhook", AsyncMock()):
        response = await server.breath_hook(None)
    body = response.body.decode("utf-8")
    assert body.startswith("[Ombre Brain - 记忆浮现] ⏰ 深圳现在：")


class _FakeRequest:
    async def json(self):
        return {"query": "昨天我们聊了什么"}


@pytest.mark.asyncio
async def test_recall_carries_time():
    match = _bucket("r1", "昨天", "干净的内容")
    with patch.object(server.bucket_mgr, "search", AsyncMock(return_value=[match])), \
         patch.object(server.embedding_engine, "search_similar", AsyncMock(return_value=[])), \
         patch.object(server.bucket_mgr, "touch", AsyncMock()), \
         patch.object(server.dehydrator, "dehydrate", AsyncMock(return_value="干净摘要")):
        response = await server.recall_hook(_FakeRequest())
    body = response.body.decode("utf-8")
    assert "⏰ 深圳现在：" in body
    assert body.rstrip().endswith("</心记浮现>")
