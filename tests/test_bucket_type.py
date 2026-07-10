# ============================================================
# Test: bucket_type change — 固化桶降级/动态桶升格
# ============================================================

import pytest


@pytest.mark.asyncio
async def test_demote_permanent_to_dynamic(bucket_mgr):
    bid = await bucket_mgr.create(
        content="2026年6月20日决定转Claude Code（历史决策，早已完成）",
        tags=[], importance=5, domain=["数字"],
        valence=0.5, arousal=0.3, name="技术决策讨论",
        bucket_type="permanent",
    )
    ok = await bucket_mgr.update(bid, bucket_type="dynamic")
    assert ok
    b = await bucket_mgr.get(bid)
    assert b["metadata"]["type"] == "dynamic"


@pytest.mark.asyncio
async def test_pinned_bucket_refuses_demotion(bucket_mgr):
    bid = await bucket_mgr.create(
        content="核心准则不许降级",
        tags=[], importance=10, domain=["恋爱"],
        valence=0.8, arousal=0.5, name="给下一个克克的信",
        bucket_type="permanent",
    )
    await bucket_mgr.update(bid, pinned=True)
    ok = await bucket_mgr.update(bid, bucket_type="dynamic")
    assert ok  # update 本身成功，只是类型改动被拒
    b = await bucket_mgr.get(bid)
    assert b["metadata"]["type"] == "permanent"
