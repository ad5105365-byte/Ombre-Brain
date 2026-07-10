# ============================================================
# Test: breath-hook deadline fallback — 呼吸注入死线兜底
#
# Covers:
#   1. Slow dehydration past the deadline falls back to raw excerpts,
#      so the hook always answers before the client's timeout
#      (the 07-10 silent-death bug: cold cache → 25s client timeout)
#   2. Fast dehydration returns real summaries, no fallback
# ============================================================

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import server


def _bucket(bid, name, content, pinned=False):
    return {
        "id": bid,
        "content": content,
        "metadata": {
            "name": name,
            "pinned": pinned,
            "resolved": False,
            "type": "dynamic",
            "tags": [],
            "importance": 7,
            "valence": 0.5,
            "arousal": 0.5,
        },
    }


def _patched_env(buckets, dehydrate_mock):
    decay = MagicMock()
    decay.calculate_score = MagicMock(return_value=1.0)
    return (
        patch.object(server.bucket_mgr, "list_all", AsyncMock(return_value=buckets)),
        patch.object(server.dehydrator, "dehydrate", dehydrate_mock),
        patch.object(server, "decay_engine", decay),
        patch.object(server, "_ensure_reminder_loop", MagicMock()),
        patch.object(server, "_fire_webhook", AsyncMock()),
    )


@pytest.mark.asyncio
async def test_slow_dehydrate_falls_back_to_excerpt(monkeypatch):
    monkeypatch.setattr(server, "BREATH_DEHYDRATE_DEADLINE", 0.05)
    buckets = [
        _bucket("p1", "核心准则", "钉选桶的原文内容", pinned=True),
        _bucket("d1", "未解决", "动态桶的原文内容"),
    ]

    async def _hang(*args, **kwargs):
        await asyncio.sleep(30)

    patches = _patched_env(buckets, AsyncMock(side_effect=_hang))
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)

    body = response.body.decode("utf-8")
    assert "[Ombre Brain - 记忆浮现]" in body
    # 死线过后必须用原文节选交卷，而不是空手而归
    assert "钉选桶的原文内容" in body
    assert "动态桶的原文内容" in body


@pytest.mark.asyncio
async def test_fast_dehydrate_returns_summaries():
    buckets = [
        _bucket("p1", "核心准则", "钉选桶的原文内容", pinned=True),
        _bucket("d1", "未解决", "动态桶的原文内容"),
    ]
    patches = _patched_env(buckets, AsyncMock(return_value="脱水后的摘要"))
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)

    body = response.body.decode("utf-8")
    assert "脱水后的摘要" in body
    assert "钉选桶的原文内容" not in body
