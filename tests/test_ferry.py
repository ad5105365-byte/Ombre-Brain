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


# ============================================================
# 分窗口（port）：2026-07-17 治并发覆盖坑
# ============================================================

@pytest.mark.asyncio
async def test_different_ports_coexist(bucket_mgr):
    bid_a, _ = await handoff_mod.write_handoff(
        bucket_mgr, purpose="主窗交接", messages="[克克] 主窗对话",
        from_port="claude.ai",
    )
    bid_b, overwritten = await handoff_mod.write_handoff(
        bucket_mgr, purpose="工作窗交接", messages="[克克] 工作窗对话",
        from_port="CC工作窗",
    )
    # 异窗不覆盖：两条都在，先写的没被删
    assert not overwritten and bid_a != bid_b
    all_buckets = await bucket_mgr.list_all()
    handoffs = handoff_mod.find_handoffs(all_buckets)
    assert len(handoffs) == 2
    assert (await bucket_mgr.get(bid_a)) is not None


@pytest.mark.asyncio
async def test_same_port_still_overwrites(bucket_mgr):
    bid1, _ = await handoff_mod.write_handoff(
        bucket_mgr, purpose="第一次", messages="[克克] 旧", from_port="VPS",
    )
    bid2, overwritten = await handoff_mod.write_handoff(
        bucket_mgr, purpose="第二次", messages="[克克] 新", from_port="VPS",
    )
    assert overwritten and bid2 == bid1


@pytest.mark.asyncio
async def test_legacy_handoff_counts_as_default_port(bucket_mgr):
    # 历史数据没有 port 标签 → 算主窗；无 from_port 的写入覆盖它（旧行为）
    await bucket_mgr.create(
        content="旧版全局渡口", bucket_type=handoff_mod.HANDOFF_TYPE,
        name="渡口交接", tags=["ferry", "handoff"],
    )
    bid, overwritten = await handoff_mod.write_handoff(
        bucket_mgr, purpose="新交接", messages="[克克] 接上",
    )
    assert overwritten
    all_buckets = await bucket_mgr.list_all()
    assert len(handoff_mod.find_handoffs(all_buckets)) == 1


def test_port_helpers():
    assert handoff_mod.normalize_port("") == handoff_mod.DEFAULT_PORT
    assert handoff_mod.normalize_port("  VPS ") == "VPS"
    assert len(handoff_mod.normalize_port("长" * 99)) == handoff_mod.MAX_PORT_CHARS
    b = {"metadata": {"tags": ["ferry", "port:手机"]}}
    assert handoff_mod.port_of(b) == "手机"
    assert handoff_mod.port_of({"metadata": {"tags": ["ferry"]}}) == handoff_mod.DEFAULT_PORT


@pytest.mark.asyncio
async def test_render_full_manual_wins_top(bucket_mgr):
    # 手写渡口坐主位，自动渡口（更新）降为一行门牌——人写的比打包的值钱
    bid_manual, _ = await handoff_mod.write_handoff(
        bucket_mgr, purpose="手写的交接", messages="[克克] 手写内容",
        from_port="主窗",
    )
    auto_purpose = f"{handoff_mod.AUTO_PURPOSE_MARK}（auto）：压缩自动打包。"
    bid_auto, _ = await handoff_mod.write_handoff(
        bucket_mgr, purpose=auto_purpose, messages="[克克] 自动内容",
        port="a1b2c3d4",
    )
    handoffs = handoff_mod.find_handoffs(await bucket_mgr.list_all())
    section = handoff_mod.render_full(handoffs)
    assert section is not None
    # 主位是手写的（全文），自动的是一行门牌
    assert "手写内容" in section
    assert f"bucket_id:{bid_auto}" in section
    assert "自动内容" not in section          # 门牌只带目的，不带正文
    assert "另一窗口的渡口" in section


@pytest.mark.asyncio
async def test_handoff_cap(bucket_mgr):
    # 会话 ID 型 port 无限攒 → 硬顶 MAX_HANDOFFS，删最旧
    for i in range(handoff_mod.MAX_HANDOFFS + 2):
        await handoff_mod.write_handoff(
            bucket_mgr, purpose=f"窗{i}", messages=f"[克克] 第{i}窗",
            port=f"sess{i:04d}",
        )
    handoffs = handoff_mod.find_handoffs(await bucket_mgr.list_all())
    assert len(handoffs) <= handoff_mod.MAX_HANDOFFS
