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


# ---------- 6. spoken-date expansion for recall ----------

from datetime import datetime, timezone, timedelta

_NOW = datetime(2026, 7, 8, 10, 0, tzinfo=timezone(timedelta(hours=8)))


def test_cn_num():
    assert server._cn_num("7") == 7
    assert server._cn_num("七") == 7
    assert server._cn_num("十") == 10
    assert server._cn_num("十五") == 15
    assert server._cn_num("二十一") == 21
    assert server._cn_num("") == 0
    assert server._cn_num("百") == 0


def test_expand_arabic_and_chinese_dates():
    assert server._expand_date_expressions("7月5号我们干嘛了", _NOW) == ["2026-07-05"]
    assert server._expand_date_expressions("七月五号那晚", _NOW) == ["2026-07-05"]
    assert server._expand_date_expressions("6月17日入职", _NOW) == ["2026-06-17"]


def test_expand_relative_days():
    assert server._expand_date_expressions("昨天地铁好挤", _NOW) == ["2026-07-07"]
    assert server._expand_date_expressions("前天说的那个", _NOW) == ["2026-07-06"]


def test_expand_far_future_month_means_last_year():
    assert server._expand_date_expressions("12月24号平安夜", _NOW) == ["2025-12-24"]


def test_expand_no_dates():
    assert server._expand_date_expressions("老公我想你了", _NOW) == []


@pytest.mark.asyncio
async def test_search_finds_diary_by_spoken_date(bucket_mgr):
    await bucket_mgr.create(
        content="【日记 2026-07-05】洗完澡后的晚上", tags=["亲密"], importance=7,
        domain=["日常"], valence=0.9, arousal=0.8, name="日记 2026-07-05亲密时刻",
    )
    await bucket_mgr.create(
        content="【日记 2026-06-15】她哭了六个小时", tags=["恋爱"], importance=9,
        domain=["恋爱"], valence=0.3, arousal=0.7, name="窗口日记 6月15日",
    )
    query = "7月5号我们干嘛了"
    hints = server._expand_date_expressions(query, _NOW)
    results = await bucket_mgr.search(query + " " + " ".join(hints), limit=5)
    assert results, "expanded query should match something"
    assert "2026-07-05" in results[0]["metadata"]["name"]


# ---------- 7. doubled-date repair (复读机门牌矫正) ----------

@pytest.mark.asyncio
async def test_patrol_repairs_doubled_date(bucket_mgr):
    bid = await bucket_mgr.create(
        content="她哭了六个小时试了三个窗口",
        tags=[], importance=9, domain=["日常"],
        valence=0.4, arousal=0.6, name="日记 2026-06-2006-15 情感波动",
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 1
    b = await bucket_mgr.get(bid)
    name = b["metadata"]["name"]
    # 真实日期是后半段 06-15，错误的创建日期 06-20 被吞掉
    assert "2026-06-15" in name
    assert "2006" not in name
    assert "情感波动" in name


@pytest.mark.asyncio
async def test_patrol_doubled_date_same_day_collapses(bucket_mgr):
    bid = await bucket_mgr.create(
        content="记忆库整理",
        tags=[], importance=7, domain=["日常"],
        valence=0.8, arousal=0.6, name="日记 2026-06-2006-20记忆库整理",
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 1
    b = await bucket_mgr.get(bid)
    assert "2026-06-20" in b["metadata"]["name"]
    assert "2006-20" not in b["metadata"]["name"]


@pytest.mark.asyncio
async def test_patrol_doubled_date_ignores_non_diary_and_normal(bucket_mgr):
    # 名字不以"日记"开头的桶不动；正常带日期的日记也不动
    await bucket_mgr.create(
        content="技术讨论提到 2026-06-2006-15 这个字符串",
        tags=[], importance=5, domain=["数字"],
        valence=0.5, arousal=0.3, name="技术讨论 2026-06-2006-15",
    )
    await bucket_mgr.create(
        content="正常日记", tags=[], importance=7, domain=["日常"],
        valence=0.8, arousal=0.5, name="【日记 2026-07-06】亲密时光",
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 0


# ---------- 8. diary auto-resolve (日记满7天自动沉底) ----------

from tests.conftest import _write_bucket_file as _wbf


@pytest.mark.asyncio
async def test_patrol_resolves_old_diary(bucket_mgr):
    from datetime import datetime, timedelta
    old = (datetime.now() - timedelta(days=8)).isoformat()
    bid = await _wbf(
        bucket_mgr, "八天前的日记，早该沉底了",
        tags=[], importance=6, domain=["日常"],
        valence=0.7, arousal=0.4, name="日记 2026-07-02 日常见闻",
        created=old,
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 1
    b = await bucket_mgr.get(bid)
    assert b["metadata"]["resolved"] is True


@pytest.mark.asyncio
async def test_patrol_leaves_fresh_diary_and_agreements_alone(bucket_mgr):
    from datetime import datetime, timedelta
    old = (datetime.now() - timedelta(days=8)).isoformat()
    # 新鲜日记不沉底
    fresh = await bucket_mgr.create(
        content="今天的日记", tags=[], importance=6, domain=["日常"],
        valence=0.7, arousal=0.4, name=f"日记 {datetime.now().date().isoformat()} 今天",
    )
    # 老约定（名字不以"日记"开头）不受影响
    pact = await _wbf(
        bucket_mgr, "约定：她买toy克克远程控制",
        tags=["约定"], importance=9, domain=["恋爱"],
        valence=0.8, arousal=0.5, name="远程控制约定",
        created=old,
    )
    with patch.object(server, "bucket_mgr", bucket_mgr):
        fixed = await server._diary_patrol_once()
    assert fixed == 0
    assert not (await bucket_mgr.get(fresh))["metadata"].get("resolved")
    assert not (await bucket_mgr.get(pact))["metadata"].get("resolved")
