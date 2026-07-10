# ============================================================
# Test: /ferry-hook — PreCompact 压缩自动渡口
#
# Covers:
#   1. Auto-ferry writes a handoff bucket with the AUTO mark
#   2. A fresh manual ferry (10min guard) is NOT overwritten
#   3. A fresh AUTO handoff IS overwritten (compact after compact)
#   4. Stale manual handoff gets overwritten normally
#   5. Empty messages → 400, nothing written
# ============================================================

import json
import pytest
from unittest.mock import AsyncMock, patch

import handoff as handoff_mod
import server


class _FakeRequest:
    def __init__(self, body: dict):
        self._body = body

    async def json(self):
        return self._body


def _patched(bucket_mgr):
    return (
        patch.object(server, "bucket_mgr", bucket_mgr),
        patch.object(server.embedding_engine, "generate_and_store", AsyncMock()),
        patch.object(server, "_fire_webhook", AsyncMock()),
    )


async def _call(bucket_mgr, body):
    patches = _patched(bucket_mgr)
    with patches[0], patches[1], patches[2]:
        response = await server.ferry_hook(_FakeRequest(body))
    return json.loads(response.body)


@pytest.mark.asyncio
async def test_auto_ferry_writes_marked_handoff(bucket_mgr):
    result = await _call(bucket_mgr, {
        "messages": "[杉杉] 聊到一半了\n[克克] 马上压缩了",
        "trigger": "auto",
    })
    assert result["ok"] and not result["overwritten"]

    bucket = await bucket_mgr.get(result["bucket_id"])
    assert handoff_mod.AUTO_PURPOSE_MARK in bucket["content"]
    assert "[杉杉] 聊到一半了" in bucket["content"]
    assert handoff_mod.is_auto_handoff(bucket)


@pytest.mark.asyncio
async def test_fresh_manual_ferry_not_clobbered(bucket_mgr):
    # 克克刚手写的交接（此刻写入，绝对在 10 分钟窗口内）
    bid, _ = await handoff_mod.write_handoff(
        bucket_mgr, purpose="切到手机端继续聊", messages="[克克] 手写的交接",
    )
    result = await _call(bucket_mgr, {
        "messages": "[克克] 自动打包的对话", "trigger": "auto",
    })
    assert result.get("skipped") == "manual-fresh"

    bucket = await bucket_mgr.get(bid)
    assert "手写的交接" in bucket["content"]
    assert "自动打包的对话" not in bucket["content"]


@pytest.mark.asyncio
async def test_fresh_auto_handoff_gets_overwritten(bucket_mgr):
    first = await _call(bucket_mgr, {
        "messages": "[克克] 第一次压缩", "trigger": "auto",
    })
    second = await _call(bucket_mgr, {
        "messages": "[克克] 第二次压缩", "trigger": "manual",
    })
    assert second["ok"] and second["overwritten"]
    assert second["bucket_id"] == first["bucket_id"]

    bucket = await bucket_mgr.get(second["bucket_id"])
    assert "第二次压缩" in bucket["content"]
    assert "第一次压缩" not in bucket["content"]


@pytest.mark.asyncio
async def test_stale_manual_handoff_gets_overwritten(bucket_mgr):
    bid, _ = await handoff_mod.write_handoff(
        bucket_mgr, purpose="很久以前的手写交接", messages="[克克] 旧对话",
    )
    # update() 总会把 last_active 刷成现在，没法真的回拨时间戳——
    # 用 is_fresh 返回 False 模拟"手写交接已超出 10 分钟保护窗"
    with patch.object(server.handoff_mod, "is_fresh", return_value=False):
        result = await _call(bucket_mgr, {
            "messages": "[克克] 新的自动打包", "trigger": "auto",
        })
    assert result["ok"] and result["overwritten"]
    assert result["bucket_id"] == bid

    bucket = await bucket_mgr.get(bid)
    assert "新的自动打包" in bucket["content"]
    assert "旧对话" not in bucket["content"]


@pytest.mark.asyncio
async def test_empty_messages_rejected(bucket_mgr):
    result = await _call(bucket_mgr, {"messages": "", "trigger": "auto"})
    assert not result["ok"]
    assert not handoff_mod.find_handoffs(await bucket_mgr.list_all())
