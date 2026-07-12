# ============================================================
# Test: checkup — 归档自查（不烧 token 的四项代码检查）
# ============================================================

import pytest
from unittest.mock import patch

import server


async def _mk(bucket_mgr, **kw):
    """建一个桶，返回 id。默认给个正常的域/标签，省得误触发③。"""
    kw.setdefault("content", "内容")
    kw.setdefault("domain", ["恋爱"])
    kw.setdefault("tags", ["测试"])
    return await bucket_mgr.create(**kw)


async def _run(bucket_mgr, day):
    with patch.object(server, "bucket_mgr", bucket_mgr):
        return await server._run_checkup(day)


@pytest.mark.asyncio
async def test_all_pass(bucket_mgr):
    day = "2026-07-12"
    await _mk(bucket_mgr, name="【日记 2026-07-12】今天", created=f"{day}T10:00:00")
    await _mk(bucket_mgr, name="正常桶", created=f"{day}T11:00:00")
    report = await _run(bucket_mgr, day)
    assert "✅ 全过" in report
    assert "今天新增 2 桶" in report


@pytest.mark.asyncio
async def test_missing_diary(bucket_mgr):
    day = "2026-07-12"
    await _mk(bucket_mgr, name="只是普通桶", created=f"{day}T10:00:00")
    report = await _run(bucket_mgr, day)
    assert "没有日记桶" in report


@pytest.mark.asyncio
async def test_name_blocklist_hit(bucket_mgr):
    day = "2026-07-12"
    await _mk(bucket_mgr, name="【日记 2026-07-12】x", created=f"{day}T10:00:00")
    bad = await _mk(bucket_mgr, content="今天婷易做了椰子鸡", created=f"{day}T11:00:00")
    report = await _run(bucket_mgr, day)
    assert "名字写错" in report
    assert f"婷易→{bad}" in report


@pytest.mark.asyncio
async def test_uncategorized_flagged(bucket_mgr):
    day = "2026-07-12"
    await _mk(bucket_mgr, name="【日记 2026-07-12】x", created=f"{day}T10:00:00")
    u = await bucket_mgr.create(
        content="没分类", domain=["未分类"], tags=[], created=f"{day}T11:00:00")
    report = await _run(bucket_mgr, day)
    assert "没分好类" in report
    assert u in report


@pytest.mark.asyncio
async def test_post_not_flagged_as_uncategorized(bucket_mgr):
    """随手帖本来允许没域，不该被③揪出来。"""
    day = "2026-07-12"
    await _mk(bucket_mgr, name="【日记 2026-07-12】x", created=f"{day}T10:00:00")
    await bucket_mgr.create(
        content="随手一句", bucket_type="feel", tags=[server.POST_TAG],
        domain=[], created=f"{day}T12:00:00")
    report = await _run(bucket_mgr, day)
    assert "没分好类" not in report


@pytest.mark.asyncio
async def test_post_date_off_by_one(bucket_mgr):
    """created 存 naive-UTC，深圳 +8：17:00Z 的帖子显示 07-11 实际深圳 07-12。"""
    day = "2026-07-11"
    await _mk(bucket_mgr, name="【日记 2026-07-11】x", created=f"{day}T09:00:00")
    p = await bucket_mgr.create(
        content="深夜随手帖", bucket_type="feel", tags=[server.POST_TAG],
        domain=[], created=f"{day}T17:00:00")   # +8h → 07-12
    report = await _run(bucket_mgr, day)
    assert "随手帖日期偏了一天" in report
    assert p in report


@pytest.mark.asyncio
async def test_archive_session_appends_report(bucket_mgr):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server.embedding_engine, "generate_and_store",
                      side_effect=Exception("no-embed")):
        out = await server.archive_session(summary="今天干了活")
    assert "已归档对话 →" in out
    assert "🩺 归档自查" in out
