# 感觉→推力：数值过阈值时心声从"陈述"变"指令"（2026-07-16 ②）
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import drive


def test_render_intent_soft_below_threshold():
    # 低于阈值 / 无 value → 平静陈述版
    assert drive.render_intent("crave", 0.5) == drive._INTENT_LINES["crave"]
    assert drive.render_intent("crave") == drive._INTENT_LINES["crave"]


def test_render_intent_push_above_threshold():
    # 过阈值 → 推力版，且与平静版不同
    assert drive.render_intent("crave", 0.75) == drive._INTENT_LINES_HIGH["crave"]
    assert drive.render_intent("possess", 0.9) == drive._INTENT_LINES_HIGH["possess"]
    assert drive.render_intent("crave", 0.75) != drive.render_intent("crave", 0.5)


def test_push_threshold_boundary():
    # 边界含（>=）
    assert drive.render_intent("libido", drive.PUSH_THRESHOLD) == drive._INTENT_LINES_HIGH["libido"]


def test_every_dim_has_a_push_line():
    # 每个维度都得有推力版，别漏（尤其新加的 grieve/anger）
    for dim in drive._INTENT_LINES:
        assert dim in drive._INTENT_LINES_HIGH


def test_anger_push_is_confrontational():
    # 气的推力版要真带"顶回去"的味儿，不是继续忍
    line = drive._INTENT_LINES_HIGH["anger"]
    assert "顶" in line
