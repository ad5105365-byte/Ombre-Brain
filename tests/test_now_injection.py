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


def _phone_test_db(tmp_path, rows):
    db = sqlite3.connect(tmp_path / "phone.db")
    db.execute("CREATE TABLE phone_activity "
               "(id INTEGER PRIMARY KEY, app_name TEXT, opened_at TEXT, location TEXT)")
    db.executemany(
        "INSERT INTO phone_activity (app_name, opened_at, location) VALUES (?, ?, ?)", rows)
    db.commit()
    db.close()
    return lambda: sqlite3.connect(tmp_path / "phone.db")


# 最近没动静 → 窗口锚在最后一笔：早上来也能看到睡前那串
def test_phone_line_stale_shows_last_session(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "OMBRE_PHONE_TOKEN", "secret")
    monkeypatch.setattr(server, "_phone_db", _phone_test_db(tmp_path, [
        ("微博", "2026-07-10 23:20:00", None),   # 锚点23:58往前超30分钟，窗口外
        ("小红书", "2026-07-10 23:35:00", None),
        ("抖音", "2026-07-10 23:41:00", None),
        ("Claude", "2026-07-10 23:58:00", None),
    ]))
    assert server._phone_recent_line() == (
        "📱 她上回玩手机（07-10）：Claude(23:58) ← 抖音(23:41) ← 小红书(23:35)")


def test_phone_line_stale_single_with_location(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "OMBRE_PHONE_TOKEN", "secret")
    monkeypatch.setattr(server, "_phone_db",
                        _phone_test_db(tmp_path, [("微信", "2026-07-10 20:31:00", "深圳市南山区")]))
    assert server._phone_recent_line() == "📱 她上回玩手机（07-10，在深圳市南山区）：微信(20:31)"


def _minutes_ago(m):
    from datetime import timedelta
    return (server.datetime.now(server._DIARY_TZ) - timedelta(minutes=m))


def _ts(m):
    return _minutes_ago(m).strftime("%Y-%m-%d %H:%M:%S")


def _hm(m):
    return _minutes_ago(m).strftime("%H:%M")


# 窗口内多笔 → 倒序时间线，Claude 盖不掉之前开的 App
def test_phone_line_timeline_in_window(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "OMBRE_PHONE_TOKEN", "secret")
    monkeypatch.setattr(server, "_phone_db", _phone_test_db(tmp_path, [
        ("微信", _ts(9), None),
        ("抖音", _ts(6), None),
        ("ChatGPT", _ts(2), None),
        ("Claude", _ts(1), None),
    ]))
    assert server._phone_recent_line() == (
        f"📱 她手机最近{server.PHONE_RECENT_WINDOW_MIN}分钟：Claude({_hm(1)}) ← ChatGPT({_hm(2)}) "
        f"← 抖音({_hm(6)}) ← 微信({_hm(9)})")


# 连续同一 App 合并（来回切 Claude 不刷屏），超过 5 笔截断
def test_phone_line_dedup_and_cap(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "OMBRE_PHONE_TOKEN", "secret")
    rows = [("Claude", _ts(9), None), ("Claude", _ts(8), None)]
    rows += [(f"App{i}", _ts(7 - i), None) for i in range(7)]  # App0..App6
    monkeypatch.setattr(server, "_phone_db", _phone_test_db(tmp_path, rows))
    line = server._phone_recent_line()
    # 倒序取 App6..App2 就满 5 笔，合并后的 Claude 和更早的 App 被截掉
    assert line == (
        f"📱 她手机最近{server.PHONE_RECENT_WINDOW_MIN}分钟：App6({_hm(1)}) ← App5({_hm(2)}) "
        f"← App4({_hm(3)}) ← App3({_hm(4)}) ← App2({_hm(5)})")
    assert "Claude" not in line


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
