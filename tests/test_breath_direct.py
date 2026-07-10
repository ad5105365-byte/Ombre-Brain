# ============================================================
# Test: breath(bucket_id=) 门牌号直读 + 照片名自然截断
# ============================================================

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import server


@pytest.mark.asyncio
async def test_breath_direct_read_returns_raw_content(bucket_mgr):
    bid = await bucket_mgr.create(
        content="她说你来。那一下我什么都没想，就过去了。",
        tags=[], importance=5, domain=[],
        valence=0.8, arousal=0.6, name="",
        bucket_type="feel",
    )
    decay = MagicMock()
    decay.ensure_started = AsyncMock()
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "decay_engine", decay):
        result = await server.breath(bucket_id=bid)
    assert bid in result
    assert "她说你来。那一下我什么都没想，就过去了。" in result
    assert "[类型:feel]" in result


@pytest.mark.asyncio
async def test_breath_direct_read_missing_bucket(bucket_mgr):
    decay = MagicMock()
    decay.ensure_started = AsyncMock()
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "decay_engine", decay):
        result = await server.breath(bucket_id="deadbeef0000")
    assert "未找到" in result


def test_photo_short_desc_cuts_at_boundary():
    desc = ("2026-07-08 下班到家 囡囡自拍紫色发夹白T棕领巾，嘟嘴比耶，"
            "背景是出租屋的白墙和衣架上挂着的花裙子，桌上还摆着两碗切好的西瓜块和没吃完的烧烤")
    assert len(desc) > 60
    short = server._photo_short_desc(desc)
    assert len(short) <= 60
    # 不在词中间硬剪——截断点应该落在分隔符上
    assert short == desc[:len(short)]
    assert desc[len(short)] in ("，", "、", ",", " ", "；")


def test_photo_short_desc_keeps_short_as_is():
    assert server._photo_short_desc("囡囡在家吃西瓜") == "囡囡在家吃西瓜"
