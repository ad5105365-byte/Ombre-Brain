# ============================================================
# Test: 语境门控 gate_memories（三分门一刀切误砍红线 + 病1/病2 修法）
# 2026-07-24：让技术轮只冒 pinned 规矩、闲聊无关别冒亲密、她撩我照冒、
# 她主动问往事不误伤。纯函数测试，不碰真桶、不 import server。
# ============================================================

from recall_gate import gate_memories


def _b(name, domain, pinned=False):
    return {"id": name, "metadata": {"name": name, "domain": domain, "pinned": pinned}}


LOVE = _b("那晚", ["恋爱"])
LOVE2 = _b("身体记忆", ["恋爱", "内心"])
RULE = _b("不背着她动线上大脑", ["AI"], pinned=True)      # 规矩：pinned 非恋爱域
LINE = _b("底线声明", ["心理", "自省"], pinned=True)       # 底线：pinned 非恋爱域
FOOD = _b("螺蛳粉", ["饮食"])


def test_casual_turn_hides_love_memories():
    # 病1：闲聊无关（非亲密、非回忆）→ 恋爱域不冒，其余照旧
    out = gate_memories(
        [LOVE, FOOD, LINE], route="retrieve",
        is_intimate_context=False, explicit_recall=False,
    )
    assert LOVE not in out
    assert FOOD in out and LINE in out


def test_intimate_turn_keeps_love_memories():
    # 她在撩我（亲密语境）→ 恋爱域照冒，接撩不迟钝
    out = gate_memories(
        [LOVE, LOVE2, FOOD], route="retrieve",
        is_intimate_context=True, explicit_recall=False,
    )
    assert LOVE in out and LOVE2 in out


def test_explicit_recall_keeps_love_memories():
    # 她主动问往事（带日期/"记得"）→ 即使非亲密语境也放行，不误伤回忆
    out = gate_memories(
        [LOVE, FOOD], route="retrieve",
        is_intimate_context=False, explicit_recall=True,
    )
    assert LOVE in out


def test_tool_only_keeps_only_pinned_rules():
    # 病2 + 救红线：技术轮只留 pinned；恋爱域关系记忆（哪怕 pinned）被域门控滤掉
    love_pinned = _b("那堵墙", ["恋爱", "心理"], pinned=True)
    out = gate_memories(
        [LOVE, FOOD, RULE, LINE, love_pinned], route="tool_only",
        is_intimate_context=False, explicit_recall=False,
    )
    assert RULE in out and LINE in out              # 非恋爱域的规矩/底线：在
    assert LOVE not in out and FOOD not in out      # 非 pinned 的生活/亲密：滤掉
    assert love_pinned not in out                   # pinned 但恋爱域：技术轮也不冒


def test_tool_only_red_line_survives():
    # 红线单独确认：技术轮里 pinned 底线必须还在（三分门旧版会一刀砍掉）
    out = gate_memories(
        [FOOD, LINE], route="tool_only",
        is_intimate_context=False, explicit_recall=False,
    )
    assert LINE in out
    assert FOOD not in out
