# checkin_store 单测——纯逻辑，不碰进程/网络/数据库。
# 跑：python -m pytest tests/test_checkin_store.py -q
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import checkin_store as cs

TZ = timezone(timedelta(hours=8))


def _now(h=14, m=0):
    return datetime(2026, 7, 18, h, m, 0, tzinfo=TZ)


# --- load_checkin / 缺文件兜底 ---

def test_load_checkin_missing_file(tmp_path):
    rec = cs.load_checkin(str(tmp_path))
    assert rec == {"mood": "", "text": "", "ts": "", "consumed": True}


def test_load_checkin_corrupt_file_resets(tmp_path):
    path = cs.checkin_path(str(tmp_path))
    with open(path, "w", encoding="utf-8") as f:
        f.write("not json{{{")
    rec = cs.load_checkin(str(tmp_path))
    assert rec == {"mood": "", "text": "", "ts": "", "consumed": True}


# --- record_checkin ---

def test_record_checkin_stores_mood_and_text(tmp_path):
    base = str(tmp_path)
    rec = cs.record_checkin(base, "emo", "有点累", _now())
    assert rec["mood"] == "emo"
    assert rec["text"] == "有点累"
    assert rec["consumed"] is False
    # 落盘可读回
    assert cs.load_checkin(base) == rec


def test_record_checkin_mood_only(tmp_path):
    rec = cs.record_checkin(str(tmp_path), "开心", "", _now())
    assert rec["mood"] == "开心" and rec["text"] == ""


def test_record_checkin_text_only(tmp_path):
    rec = cs.record_checkin(str(tmp_path), "", "今天写完了论文", _now())
    assert rec["mood"] == "" and rec["text"] == "今天写完了论文"


def test_record_checkin_both_empty_raises(tmp_path):
    try:
        cs.record_checkin(str(tmp_path), "", "  ", _now())
        assert False, "应该抛 ValueError"
    except ValueError:
        pass


def test_record_checkin_truncates_overlong_fields(tmp_path):
    long_mood = "开" * 50
    long_text = "字" * 500
    rec = cs.record_checkin(str(tmp_path), long_mood, long_text, _now())
    assert len(rec["mood"]) == cs.MOOD_MAX_LEN
    assert len(rec["text"]) == cs.TEXT_MAX_LEN


def test_record_checkin_overwrites_previous(tmp_path):
    base = str(tmp_path)
    cs.record_checkin(base, "开心", "第一条", _now(10))
    rec2 = cs.record_checkin(base, "emo", "第二条", _now(11))
    loaded = cs.load_checkin(base)
    assert loaded["mood"] == "emo" and loaded["text"] == "第二条"
    assert loaded == rec2


# --- render_checkin_line ---

def test_render_line_mood_and_text():
    line = cs.render_checkin_line({"mood": "emo", "text": "有点累"})
    assert line == "杉杉刚打卡——心情「emo」，她说：有点累"


def test_render_line_mood_only():
    assert cs.render_checkin_line({"mood": "开心", "text": ""}) == "杉杉刚打卡——心情「开心」"


def test_render_line_text_only():
    assert cs.render_checkin_line({"mood": "", "text": "今天写完了论文"}) == \
        "杉杉刚打卡，她说：今天写完了论文"


def test_render_line_empty_returns_empty_string():
    assert cs.render_checkin_line({"mood": "", "text": ""}) == ""


# --- pending_line：只提一次 ---

def test_pending_line_none_when_nothing_recorded(tmp_path):
    assert cs.pending_line(str(tmp_path), _now()) is None


def test_pending_line_returns_and_consumes(tmp_path):
    base = str(tmp_path)
    cs.record_checkin(base, "emo", "有点累", _now(10, 0))
    line = cs.pending_line(base, _now(10, 5))
    assert line == "杉杉刚打卡——心情「emo」，她说：有点累"
    # 读第二次就没有了——只提一次
    assert cs.pending_line(base, _now(10, 10)) is None
    assert cs.load_checkin(base)["consumed"] is True


def test_pending_line_stale_expires_silently(tmp_path):
    base = str(tmp_path)
    cs.record_checkin(base, "想你", "", _now(8, 0))
    # 20 小时后才第一次被读到，超过默认 12h 门槛
    late = _now(8, 0) + timedelta(hours=20)
    assert cs.pending_line(base, late) is None
    # 过期也算"消费过"，不会反复检查
    assert cs.load_checkin(base)["consumed"] is True


def test_pending_line_within_age_window_survives(tmp_path):
    base = str(tmp_path)
    cs.record_checkin(base, "想你", "", _now(8, 0))
    soon = _now(8, 0) + timedelta(hours=1)
    assert cs.pending_line(base, soon) == "杉杉刚打卡——心情「想你」"


def test_pending_line_custom_max_age(tmp_path):
    base = str(tmp_path)
    cs.record_checkin(base, "开心", "", _now(8, 0))
    later = _now(8, 0) + timedelta(hours=2)
    assert cs.pending_line(base, later, max_age_hours=1.0) is None


def test_pending_line_bad_timestamp_does_not_crash(tmp_path):
    base = str(tmp_path)
    cs.save_checkin(base, {"mood": "emo", "text": "", "ts": "not-a-date", "consumed": False})
    # 解析不了时间戳就当没过期，宁可多提一句
    assert cs.pending_line(base, _now()) == "杉杉刚打卡——心情「emo」"
