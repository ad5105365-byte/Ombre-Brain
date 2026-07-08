# ============================================================
# Test: diary date guard + patrol — 日记门牌守卫与查房
#
# Covers:
#   1. _explicit_diary_date: parses explicit dates, never guesses today
#   2. _merge_or_create refuses to merge diaries across dates (0304 事故)
#   3. _merge_or_create still merges same-date diary content
#   4. _diary_patrol_once stamps missing date prefixes from created time
#   5. patrol leaves dated diaries and non-leading-日记 names alone
# ============================================================

import pytest
from unittest.mock import AsyncMock, patch

import server


# ---------- 1. _explicit_diary_date ----------

def test_explicit_date_from_name():
    assert server._explicit_diary_date("【日记 2026-07-05】亲密时刻") == "2026-07-05"
    assert server._explicit_diary_date("日记 2026-07-06亲密时光") == "2026-07-06"
    assert server._explicit_diary_date("日记 2026.7.5 洗澡") == "2026-07-05"


def test_explicit_date_from_content():
    assert server._explicit_diary_date("", "【日记 2026年7月4日】三封信") == "2026-07-04"


def test_no_guessing_today():
    # A diary without a written date must return '' — never today's date
    assert server._explicit_diary_date("日记 亲密时光") == ""
    assert server._explicit_diary_date("日记 亲密时光", "没有日期的正文") == ""


def test_non_diary_returns_empty():
    assert server._explicit_diary_date("亲密时刻", "2026-07-05 洗完澡") == ""


# ---------- 2/3. cross-date merge guard ----------

def _existing(name, content, score=99):
    return [{
        "id": "old123",
        "score": score,
        "content": content,
        "metadata": {"name": name, "tags": [], "domain": ["日常"],
                     "importance": 7, "valence": 0.8, "arousal": 0.7},
    }]


@pytest.mark.asyncio
async def test_diaries_never_merge_across_dates(bucket_mgr, test_config):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "config", test_config), \
         patch.object(bucket_mgr, "search",
                      AsyncMock(return_value=_existing(
                          "【日记 2026-07-03】破防挑战", "【日记 2026-07-03】晚上玩挑战"))), \
         patch.object(server.embedding_engine, "generate_and_store", AsyncMock()):
        merge_spy = AsyncMock(return_value="merged-content")
        with patch.object(server.dehydrator, "merge", merge_spy):
            _, is_merged = await server._merge_or_create(
                content="【日记 2026-07-04】写了三封信",
                tags=[], importance=7, domain=["日常"],
                valence=0.8, arousal=0.7,
                name="【日记 2026-07-04】三封信",
            )
    assert not is_merged
    merge_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_dated_diary_never_merges_into_dateless_bucket(bucket_mgr, test_config):
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "config", test_config), \
         patch.object(bucket_mgr, "search",
                      AsyncMock(return_value=_existing("亲密时刻", "洗完澡的晚上"))), \
         patch.object(server.embedding_engine, "generate_and_store", AsyncMock()):
        merge_spy = AsyncMock(return_value="merged-content")
        with patch.object(server.dehydrator, "merge", merge_spy):
            _, is_merged = await server._merge_or_create(
                content="【日记 2026-07-05】亲密时刻",
                tags=[], importance=7, domain=["日常"],
                valence=0.9, arousal=0.8,
                name="【日记 2026-07-05】亲密时刻",
            )
    assert not is_merged
    merge_spy.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_date_diary_still_merges(bucket_mgr, test_config):
    # First create a real bucket so update() has a target
    bid = await bucket_mgr.create(
        content="【日记 2026-07-04】上午的部分",
        tags=[], importance=7, domain=["日常"],
        valence=0.8, arousal=0.7, name="【日记 2026-07-04】三封信",
    )
    existing = _existing("【日记 2026-07-04】三封信", "【日记 2026-07-04】上午的部分")
    existing[0]["id"] = bid
    with patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "config", test_config), \
         patch.object(bucket_mgr, "search", AsyncMock(return_value=existing)), \
         patch.object(server.embedding_engine, "generate_and_store", AsyncMock()), \
         patch.object(server.dehydrator, "merge",
                      AsyncMock(return_value="【日记 2026-07-04】上午+下午")):
        _, is_merged = await server._merge_or_create(
            content="【日记 2026-07-04】下午的部分",
            tags=[], importance=7, domain=["日常"],
            valence=0.8, arousal=0.7, name="【日记 2026-07-04】三封信",
        )
    assert is_merged


# ---------- 4/5. diary patrol ----------

@pytest.mark.asyncio
async def test_patrol_stamps_missing_date(bucket_mgr):
    bid = await bucket_mgr.create(
        content="洗完澡后的晚上，没写日期",
        tags=[], importance=7, domain=["日常"],
        valence=0.9, arousal=0.8, name="日记 亲密时光",
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 1
    b = await bucket_mgr.get(bid)
    # sanitize_name strips 【】 — what matters is the date parses for the calendar
    name = b["metadata"]["name"]
    assert server._DIARY_DATE_RE.search(name)
    assert "亲密时光" in name


@pytest.mark.asyncio
async def test_patrol_prefers_content_date_over_created(bucket_mgr):
    bid = await bucket_mgr.create(
        content="【日记 2026-07-05】洗完澡后的晚上",
        tags=[], importance=7, domain=["日常"],
        valence=0.9, arousal=0.8, name="日记 亲密时光",
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 1
    b = await bucket_mgr.get(bid)
    # sanitize_name strips 【】; the content's explicit date must win over created
    assert server._explicit_diary_date(b["metadata"]["name"]) == "2026-07-05"


@pytest.mark.asyncio
async def test_patrol_leaves_good_buckets_alone(bucket_mgr):
    await bucket_mgr.create(
        content="正常日记", tags=[], importance=7, domain=["日常"],
        valence=0.8, arousal=0.5, name="【日记 2026-07-06】亲密时光",
    )
    await bucket_mgr.create(
        content="不是当日日记", tags=[], importance=5, domain=["日常"],
        valence=0.5, arousal=0.3, name="克克日记手册",
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 0
