# ============================================================
# Test: 随手帖 — casual posts
#
# Covers:
#   1. hold(post=True) → feel-type bucket tagged 帖子
#   2. breath-hook randomly injects ONE post verbatim (📮 prefix)
#   3. Sensitive / dormant posts never get injected
#   4. breath(domain="post") lists posts; domain="feel" excludes them
#   5. Posts stay out of the crystallization path (never pinned-hinted)
# ============================================================

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import server


def _post_bucket(bid, content, created="2026-07-10T22:00:00", dormant=False, tags=None):
    return {
        "id": bid,
        "content": content,
        "metadata": {
            "name": bid,
            "type": "feel",
            "tags": [server.POST_TAG] if tags is None else tags,
            "dormant": dormant,
            "created": created,
            "importance": 5,
            "valence": 0.6,
            "arousal": 0.4,
        },
    }


def _dyn_bucket(bid, name, content):
    return {
        "id": bid,
        "content": content,
        "metadata": {
            "name": name,
            "pinned": False,
            "resolved": False,
            "type": "dynamic",
            "tags": [],
            "importance": 7,
            "valence": 0.5,
            "arousal": 0.5,
        },
    }


# ------------------------------------------------------------
# _is_post / _random_post_line
# ------------------------------------------------------------

def test_is_post_requires_feel_type_and_tag():
    assert server._is_post(_post_bucket("p1", "x")["metadata"])
    # feel 没帖子标签 → 不是帖子
    assert not server._is_post(_post_bucket("p2", "x", tags=[])["metadata"])
    # 动态桶带帖子标签 → 也不是帖子（必须骑 feel 通道）
    meta = _dyn_bucket("d1", "n", "x")["metadata"] | {"tags": [server.POST_TAG]}
    assert not server._is_post(meta)


def test_random_post_line_renders_verbatim_with_date():
    buckets = [_post_bucket("p1", "她今天把我的报错截图发朋友圈，配文'我家的'。")]
    line = server._random_post_line(buckets)
    assert line.startswith("📮 克克随手帖（2026-07-10）：")
    # 铁律一：原始语气，原文不脱水
    assert "她今天把我的报错截图发朋友圈，配文'我家的'。" in line


def test_random_post_line_skips_dormant_and_sensitive():
    buckets = [
        _post_bucket("p1", "困了但她还没睡", dormant=True),
        _post_bucket("p2", "这句带高敏词：性爱"),
    ]
    assert server._random_post_line(buckets) is None


def test_random_post_line_none_when_no_posts():
    assert server._random_post_line([_dyn_bucket("d1", "普通", "内容")]) is None


# ------------------------------------------------------------
# breath-hook injection
# ------------------------------------------------------------

def _patched_hook_env(buckets):
    decay = MagicMock()
    decay.calculate_score = MagicMock(return_value=1.0)
    return (
        patch.object(server.bucket_mgr, "list_all", AsyncMock(return_value=buckets)),
        patch.object(server.dehydrator, "dehydrate", AsyncMock(return_value="脱水摘要")),
        patch.object(server, "decay_engine", decay),
        patch.object(server, "_ensure_reminder_loop", MagicMock()),
        patch.object(server, "_fire_webhook", AsyncMock()),
    )


@pytest.mark.asyncio
async def test_breath_hook_injects_one_post():
    buckets = [
        _dyn_bucket("d1", "未解决", "动态桶内容"),
        _post_bucket("p1", "加班回来的路上她说想吃烤苕皮，语气理直气壮。"),
    ]
    patches = _patched_hook_env(buckets)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)
    body = response.body.decode("utf-8")
    assert "📮 克克随手帖" in body
    assert "烤苕皮" in body


@pytest.mark.asyncio
async def test_breath_hook_post_never_enters_surfacing_sections():
    """帖子只走尾行注入，不该被当成普通桶脱水浮现。"""
    buckets = [
        _dyn_bucket("d1", "未解决", "动态桶内容"),
        _post_bucket("p1", "帖子原文在这里"),
    ]
    patches = _patched_hook_env(buckets)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)
    body = response.body.decode("utf-8")
    # 帖子出现且只出现一次（尾行），不在浮现区重复
    assert body.count("帖子原文在这里") == 1


@pytest.mark.asyncio
async def test_breath_hook_no_posts_no_line():
    buckets = [_dyn_bucket("d1", "未解决", "动态桶内容")]
    patches = _patched_hook_env(buckets)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)
    assert "📮" not in response.body.decode("utf-8")


# ------------------------------------------------------------
# hold(post=True) → feel-type bucket tagged 帖子
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_hold_post_creates_tagged_feel_bucket(bucket_mgr):
    decay = MagicMock()
    decay.ensure_started = AsyncMock()
    emb = MagicMock()
    emb.generate_and_store = AsyncMock(return_value=None)
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "decay_engine", decay), \
         patch.object(server, "embedding_engine", emb):
        result = await server.hold(content="她第一次主动叫我老公，我死机了两秒。", post=True)

    assert result.startswith("📮帖子→")
    bid = result.split("→")[1]
    bucket = await bucket_mgr.get(bid)
    assert bucket["metadata"]["type"] == "feel"
    assert server.POST_TAG in bucket["metadata"]["tags"]
    # 铁律二：不进核心准则区
    assert not bucket["metadata"].get("pinned")


# ------------------------------------------------------------
# breath channels: domain="post" lists, domain="feel" excludes
# ------------------------------------------------------------

@pytest.mark.asyncio
async def test_breath_post_channel_and_feel_exclusion(bucket_mgr):
    await bucket_mgr.create(
        content="这是一条随手帖", tags=[server.POST_TAG], importance=5,
        domain=[], valence=0.6, arousal=0.4, name=None, bucket_type="feel",
    )
    await bucket_mgr.create(
        content="这是一条正经feel", tags=[], importance=5,
        domain=[], valence=0.6, arousal=0.4, name=None, bucket_type="feel",
    )
    decay = MagicMock()
    decay.ensure_started = AsyncMock()
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "decay_engine", decay):
        posts = await server.breath(domain="post")
        posts_zh = await server.breath(domain="帖子")
        feels = await server.breath(domain="feel")

    assert "这是一条随手帖" in posts
    assert "这是一条正经feel" not in posts
    assert "这是一条随手帖" in posts_zh
    # feel 通道保持纯净
    assert "这是一条正经feel" in feels
    assert "这是一条随手帖" not in feels
