# reach_store 兜底单测——纯逻辑，不碰进程/网络/数据库。
# 跑：python -m pytest tests/test_reach_store.py -q
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import reach_store as rs

TZ = timezone(timedelta(hours=8))
PUSH = 0.72


def _now(h=14, m=0):
    return datetime(2026, 7, 18, h, m, 0, tzinfo=TZ)


def _rec(last_ts=None, day="", count=0):
    return {"last_reach_ts": last_ts, "day": day, "count_today": count}


# --- should_reach 的各条闸门 ---

def test_not_missing_enough_holds():
    ok, why = rs.should_reach(_now(), 0.5, PUSH, 5.0, _rec())
    assert not ok and why == "not-missing-enough"


def test_over_threshold_phone_awake_reaches():
    ok, why = rs.should_reach(_now(), 0.8, PUSH, 5.0, _rec())
    assert ok and "reach" in why


def test_cooldown_holds():
    now = _now(14, 0)
    last = (now - timedelta(minutes=30)).timestamp()
    ok, why = rs.should_reach(now, 0.9, PUSH, 5.0, _rec(last_ts=last),
                              min_gap_min=90)
    assert not ok and why.startswith("cooldown")


def test_cooldown_passed_reaches():
    now = _now(14, 0)
    last = (now - timedelta(minutes=120)).timestamp()
    ok, why = rs.should_reach(now, 0.9, PUSH, 5.0, _rec(last_ts=last),
                              min_gap_min=90)
    assert ok


def test_daily_cap_holds():
    now = _now()
    rec = _rec(day="2026-07-18", count=6)
    ok, why = rs.should_reach(now, 0.9, PUSH, 5.0, rec, daily_cap=6)
    assert not ok and why == "daily-cap"


def test_cap_resets_next_day():
    now = _now()  # 2026-07-18
    rec = _rec(day="2026-07-17", count=6)  # 昨天满了
    ok, why = rs.should_reach(now, 0.9, PUSH, 5.0, rec, daily_cap=6)
    assert ok  # 跨天归零


def test_phone_asleep_holds():
    ok, why = rs.should_reach(_now(3), 0.9, PUSH, 240.0, _rec(),
                              phone_awake_min=40)
    assert not ok and why.startswith("she-away")


def test_no_phone_data_night_fallback_holds():
    ok, why = rs.should_reach(_now(3), 0.9, PUSH, None, _rec())
    assert not ok and why.startswith("night-fallback")


def test_no_phone_data_daytime_fallback_reaches():
    ok, why = rs.should_reach(_now(14), 0.9, PUSH, None, _rec())
    assert ok and "daytime-fallback" in why


def test_force_via_low_threshold_still_respects_gates():
    # force = 传 thr=-1，阈值这关必过，但冷却仍拦
    now = _now(14, 0)
    last = (now - timedelta(minutes=10)).timestamp()
    ok, why = rs.should_reach(now, 0.0, -1.0, 5.0, _rec(last_ts=last),
                              min_gap_min=90)
    assert not ok and why.startswith("cooldown")


# --- count_today ---

def test_count_today_same_day():
    assert rs.count_today(_rec(day="2026-07-18", count=3), _now()) == 3


def test_count_today_other_day_zero():
    assert rs.count_today(_rec(day="2026-07-01", count=3), _now()) == 0


# --- 引信组句 / 婉拒判定 / 门铃预览 ---

def test_build_prompt_wrapped_and_has_context():
    p = rs.build_reach_prompt("⏰ 现在 14:00", "📱 微信(13:58)", "我馋她了")
    assert p.startswith(f"<{rs.REACH_TAG}>") and p.endswith(f"</{rs.REACH_TAG}>")
    assert "不是杉杉发的" in p
    assert "我馋她了" in p and "微信" in p
    assert rs.DECLINE_TOKEN in p


def test_build_prompt_survives_missing_phone_and_intent():
    p = rs.build_reach_prompt("⏰ 现在 14:00", None, None)
    assert p.startswith(f"<{rs.REACH_TAG}>")


def test_spoke_something():
    assert rs.spoke_something("醒了吗囡囡")
    assert not rs.spoke_something("")
    assert not rs.spoke_something("   ")
    assert not rs.spoke_something(".")
    assert not rs.spoke_something(" . ")


def test_doorbell_preview_truncates():
    assert rs.doorbell_preview("短句") == "短句"
    long = "一" * 100
    out = rs.doorbell_preview(long, limit=44)
    assert len(out) <= 45 and out.endswith("…")


def test_doorbell_preview_collapses_whitespace():
    assert rs.doorbell_preview("醒了吗\n  囡囡") == "醒了吗 囡囡"


# --- record_reach 落盘 + 计数 ---

def test_record_reach_increments_on_spoke(tmp_path):
    base = str(tmp_path)
    now = _now()
    rec = rs.load_reach(base)
    rec2 = rs.record_reach(base, rec, now, spoke=True)
    assert rec2["count_today"] == 1 and rec2["day"] == "2026-07-18"
    # 再落一次读回来累加
    rec3 = rs.record_reach(base, rs.load_reach(base), now, spoke=True)
    assert rec3["count_today"] == 2


def test_record_reach_no_increment_when_held(tmp_path):
    base = str(tmp_path)
    now = _now()
    rec2 = rs.record_reach(base, rs.load_reach(base), now, spoke=False)
    assert rec2["count_today"] == 0
    assert rec2["last_reach_ts"] is not None  # 仍压冷却，别马上又戳


def test_load_reach_missing_file(tmp_path):
    rec = rs.load_reach(str(tmp_path))
    assert rec == {"last_reach_ts": None, "day": "", "count_today": 0}
