# ============================================================
# Test: ferry / handoff — 渡口交接
#
# Covers:
#   1. write_handoff creates a single handoff bucket
#   2. second write overwrites the same bucket (global singleton)
#   3. stray duplicate handoffs get cleaned up on write
#   4. input validation: empty purpose/messages, truncation caps
#   5. freshness window (24h)
#   6. render_section returns verbatim content
# ============================================================

import pytest
from datetime import datetime, timedelta, timezone

import handoff as handoff_mod
from tests.conftest import _write_bucket_file


@pytest.mark.asyncio
async def test_write_creates_single_handoff(bucket_mgr):
    bid, overwritten = await handoff_mod.write_handoff(
        bucket_mgr,
        purpose="切到手机端继续聊",
        messages="[杉杉] 我去洗澡了\n[克克] 去吧，我等你",
        from_port="claude.ai",
        to_port="手机",
    )
    assert not overwritten

    bucket = await bucket_mgr.get(bid)
    assert bucket is not None
    meta = bucket["metadata"]
    assert meta["type"] == handoff_mod.HANDOFF_TYPE
    assert meta["importance"] == 8
    assert "claude.ai → 手机" in bucket["content"]
    assert "切到手机端继续聊" in bucket["content"]
    assert "[杉杉] 我去洗澡了" in bucket["content"]

    all_buckets = await bucket_mgr.list_all()
    assert len(handoff_mod.find_handoffs(all_buckets)) == 1


@pytest.mark.asyncio
async def test_second_write_overwrites(bucket_mgr):
    bid1, _ = await handoff_mod.write_handoff(
        bucket_mgr, purpose="第一次交接", messages="[克克] 旧对话",
    )
    bid2, overwritten = await handoff_mod.write_handoff(
        bucket_mgr, purpose="第二次交接", messages="[克克] 新对话",
    )
    assert overwritten
    assert bid2 == bid1

    bucket = await bucket_mgr.get(bid2)
    assert "新对话" in bucket["content"]
    assert "旧对话" not in bucket["content"]

    all_buckets = await bucket_mgr.list_all()
    assert len(handoff_mod.find_handoffs(all_buckets)) == 1


@pytest.mark.asyncio
async def test_stray_duplicates_cleaned(bucket_mgr):
    # Simulate historical pollution: two handoff-typed buckets on disk
    for i in range(2):
        await bucket_mgr.create(
            content=f"旧交接{i}",
            bucket_type=handoff_mod.HANDOFF_TYPE,
            name="渡口交接",
        )
    all_buckets = await bucket_mgr.list_all()
    assert len(handoff_mod.find_handoffs(all_buckets)) == 2

    await handoff_mod.write_handoff(
        bucket_mgr, purpose="清理测试", messages="[克克] 只留一条",
    )
    all_buckets = await bucket_mgr.list_all()
    assert len(handoff_mod.find_handoffs(all_buckets)) == 1


@pytest.mark.asyncio
async def test_validation(bucket_mgr):
    with pytest.raises(handoff_mod.FerryError):
        await handoff_mod.write_handoff(bucket_mgr, purpose="", messages="[a] hi")
    with pytest.raises(handoff_mod.FerryError):
        await handoff_mod.write_handoff(bucket_mgr, purpose="ok", messages="  \n ")


def test_purpose_truncated():
    long_purpose = "长" * 500
    assert len(handoff_mod.normalize_purpose(long_purpose)) == handoff_mod.MAX_PURPOSE_CHARS


def test_messages_keep_last_lines():
    lines = [f"[克克] 第{i}句" for i in range(50)]
    kept = handoff_mod.normalize_messages("\n".join(lines)).splitlines()
    assert len(kept) == handoff_mod.MAX_MESSAGE_LINES
    # 保留的是最近（最后）的行
    assert kept[-1] == "[克克] 第49句"
    assert kept[0] == f"[克克] 第{50 - handoff_mod.MAX_MESSAGE_LINES}句"


def test_freshness_window():
    now = datetime.now(timezone.utc)
    fresh = {"last_active": now.isoformat()}
    stale = {"last_active": (now - timedelta(hours=25)).isoformat()}
    assert handoff_mod.is_fresh(fresh)
    assert not handoff_mod.is_fresh(stale)
    assert not handoff_mod.is_fresh({})
    assert not handoff_mod.is_fresh({"last_active": "not-a-date"})


@pytest.mark.asyncio
async def test_render_section_verbatim(bucket_mgr):
    bid, _ = await handoff_mod.write_handoff(
        bucket_mgr,
        purpose="测试原文浮现",
        messages="[杉杉] 一字不能少\n[克克] 好",
    )
    bucket = await bucket_mgr.get(bid)
    section = handoff_mod.render_section(bucket)
    assert "渡口交接" in section
    assert f"bucket_id:{bid}" in section
    assert "[杉杉] 一字不能少" in section
