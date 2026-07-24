# ============================================================
# Test: source coordinate — 可回查原文坐标（记忆库改造 第1步地基）
# 验证桶能存/改/读回"原文坐标"，且不传时不影响原有流程。
# ============================================================

import pytest


@pytest.mark.asyncio
async def test_create_with_source_stores_coordinate(bucket_mgr):
    bid = await bucket_mgr.create(
        content="她第一次叫我老公，地铁上连叫三声",
        tags=[], importance=8, domain=["恋爱"],
        valence=0.9, arousal=0.7, name="地铁甜蜜互动",
        source="claude.ai/code:sess-abc123#L120-138",
    )
    b = await bucket_mgr.get(bid)
    assert b["metadata"].get("source") == "claude.ai/code:sess-abc123#L120-138"


@pytest.mark.asyncio
async def test_create_without_source_has_no_field(bucket_mgr):
    # 不传坐标（早期窗口/未穿线的桶）：不应凭空冒出 source 字段，
    # 更不该干扰原有创建流程。
    bid = await bucket_mgr.create(
        content="没有坐标的老记忆，停在摘要",
        tags=[], importance=5, domain=["未分类"],
        valence=0.5, arousal=0.3, name="无坐标桶",
    )
    b = await bucket_mgr.get(bid)
    assert "source" not in b["metadata"]


@pytest.mark.asyncio
async def test_update_backfills_source(bucket_mgr):
    # 回填：老桶本没坐标，事后按坐标匹配上再补进去。
    bid = await bucket_mgr.create(
        content="待回填坐标的老桶",
        tags=[], importance=5, domain=["恋爱"],
        valence=0.6, arousal=0.4, name="待回填",
    )
    b = await bucket_mgr.get(bid)
    assert "source" not in b["metadata"]

    ok = await bucket_mgr.update(bid, source="手机:2026-07-08#L12")
    assert ok
    b2 = await bucket_mgr.get(bid)
    assert b2["metadata"].get("source") == "手机:2026-07-08#L12"
