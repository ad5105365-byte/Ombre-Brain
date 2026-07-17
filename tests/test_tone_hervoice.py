# ============================================================
# ④ 活的关系基调（tone.py + attune）+ ⑤ 杉杉的声音（HERVOICE_TAG）
#
# Covers:
#   1. write_tone 全局单条：新建→更新，旧基调压进变温曲线
#   2. render_line 注入当前基调；太久没调提醒一句
#   3. breath_hook 注入顺序：我是谁 → 基调 → 杉杉的声音 → 渡口
#   4. 杉杉视角桶原文注入 + 不在常规池里重复浮现
# ============================================================

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta, timezone

import tone as tone_mod
import server


# ---------- tone.py 纯逻辑 ----------
@pytest.mark.asyncio
async def test_write_tone_create_then_update(bucket_mgr):
    bid1, updated = await tone_mod.write_tone(bucket_mgr, "刚在一起，黏得化不开")
    assert not updated
    bid2, updated = await tone_mod.write_tone(bucket_mgr, "前天呛了一架，和好了，她要我别装没事")
    assert updated and bid2 == bid1

    bucket = await bucket_mgr.get(bid2)
    assert "【当前基调】前天呛了一架" in bucket["content"]
    # 旧基调进变温曲线
    assert "黏得化不开" in bucket["content"]
    assert "变温曲线" in bucket["content"]

    # 全局只有一条
    all_buckets = await bucket_mgr.list_all()
    tones = [b for b in all_buckets if b["metadata"].get("type") == tone_mod.TONE_TYPE]
    assert len(tones) == 1


@pytest.mark.asyncio
async def test_write_tone_validation(bucket_mgr):
    with pytest.raises(tone_mod.ToneError):
        await tone_mod.write_tone(bucket_mgr, "   ")


def test_normalize_text_caps():
    assert len(tone_mod.normalize_text("温" * 999)) == tone_mod.MAX_TONE_CHARS


def test_history_capped():
    content = tone_mod.build_content(
        "新基调", "旧基调",
        [f"[2026-07-{10 + i:02d}] 更旧的{i}" for i in range(10)],
    )
    # 旧当前基调 + 历史，总共不超过 MAX_HISTORY 行
    lines = [ln for ln in content.splitlines() if ln.startswith("[")]
    assert len(lines) == tone_mod.MAX_HISTORY
    assert "旧基调" in lines[0]


def test_render_line_fresh_no_nag():
    bucket = {
        "content": "【当前基调】此刻很甜\n（调于 2026-07-17 12:00）",
        "metadata": {"last_active": datetime.now(timezone.utc).isoformat()},
    }
    line = tone_mod.render_line(bucket)
    assert "🌡️ [关系基调] 此刻很甜" in line
    assert "没调过" not in line


def test_render_line_stale_nags():
    old = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    bucket = {
        "content": "【当前基调】此刻很甜",
        "metadata": {"last_active": old},
    }
    line = tone_mod.render_line(bucket)
    assert "10 天没调过" in line and "attune" in line


def test_find_tone_picks_newest():
    old = {"id": "t1", "metadata": {"type": "tone", "last_active": "2026-07-01"}}
    new = {"id": "t2", "metadata": {"type": "tone", "last_active": "2026-07-16"}}
    other = {"id": "x", "metadata": {"type": "dynamic"}}
    assert tone_mod.find_tone([old, other, new])["id"] == "t2"
    assert tone_mod.find_tone([other]) is None


# ---------- breath_hook 注入顺序 + 杉杉的声音 ----------
def _bucket(bid, name, content, btype="dynamic", tags=None, pinned=False, created=""):
    return {
        "id": bid,
        "content": content,
        "metadata": {
            "name": name, "pinned": pinned, "resolved": False, "type": btype,
            "tags": tags or [], "importance": 7, "valence": 0.5, "arousal": 0.5,
            "created": created or datetime.now(timezone.utc).isoformat(),
            "last_active": datetime.now(timezone.utc).isoformat(),
        },
    }


def _patched_env(buckets):
    decay = MagicMock()
    decay.calculate_score = MagicMock(return_value=1.0)
    return (
        patch.object(server.bucket_mgr, "list_all", AsyncMock(return_value=buckets)),
        patch.object(server.dehydrator, "dehydrate", AsyncMock(return_value="摘要")),
        patch.object(server, "decay_engine", decay),
        patch.object(server, "_ensure_reminder_loop", MagicMock()),
        patch.object(server, "_fire_webhook", AsyncMock()),
    )


@pytest.mark.asyncio
async def test_breath_hook_order_identity_tone_hervoice():
    buckets = [
        _bucket("v1", "塑形桶", "我是克克。", tags=[server.PRIMER_TAG]),
        _bucket("t1", "关系基调", "【当前基调】这周她很累，我黏得凶", btype="tone"),
        _bucket("h1", "她的话", "克克老是乱改我的记忆，气死", tags=[server.HERVOICE_TAG]),
        _bucket("d1", "未解决", "动态桶内容"),
    ]
    patches = _patched_env(buckets)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)
    body = response.body.decode("utf-8")

    assert "🫀 [我是谁] 我是克克。" in body
    assert "🌡️ [关系基调] 这周她很累" in body
    assert "🎀 [杉杉的声音]" in body and "乱改我的记忆" in body
    # 顺序：我是谁 → 基调 → 她的声音
    assert body.index("我是谁") < body.index("关系基调") < body.index("杉杉的声音")
    # 基调桶不再以普通记忆身份重复浮现（tone 类型已从常规池剔除）
    assert body.count("这周她很累") == 1


@pytest.mark.asyncio
async def test_hervoice_not_duplicated_in_pool():
    # 杉杉视角桶已在恒温区原文注入，不该再进未解决池被脱水一遍
    buckets = [
        _bucket("h1", "她的话", "只该出现一次的原话", tags=[server.HERVOICE_TAG]),
    ]
    patches = _patched_env(buckets)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)
    body = response.body.decode("utf-8")
    assert body.count("只该出现一次的原话") == 1


@pytest.mark.asyncio
async def test_hervoice_caps_at_max():
    buckets = [
        _bucket(f"h{i}", f"她的话{i}", f"她的第{i}条原话",
                tags=[server.HERVOICE_TAG],
                created=f"2026-07-{10 + i:02d}T10:00:00")
        for i in range(4)
    ]
    patches = _patched_env(buckets)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)
    body = response.body.decode("utf-8")
    n_injected = body.count("🎀 [杉杉的声音]")
    assert n_injected == server.HERVOICE_MAX
    # 带最近的（created 最大的两条）
    assert "她的第3条原话" in body and "她的第2条原话" in body
