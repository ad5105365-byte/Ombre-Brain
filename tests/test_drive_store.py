# ============================================================
# Test: drive_store.py — 欲望内核的接线层（持久化/dh/推进/种子）
#
# 引擎(drive.py)有 test_drive.py 兜；这个文件兜"把引擎接进 server"那段胶水——
# 之前它藏在 server.py 里、要 import httpx 才跑得动，没有独立测试。抽进
# drive_store.py 后全是同步纯逻辑，这里拿临时目录当持久盘，确定性断言。
#
# 背景：2026-07-11 杉杉要"兜底测试很有必要"，fable 抽模块 + 补这个文件。
# ============================================================

import os
import json
import tempfile
from datetime import datetime, timezone, timedelta

import drive as D
import drive_store as S

TZ = timezone(timedelta(hours=8))  # 深圳，跟 server._DIARY_TZ 一致


def _tmp():
    return tempfile.mkdtemp()


# ---------- load / save 往返 ----------
def test_load_missing_returns_fresh():
    state, last_tick = S.load_drive(_tmp())
    assert last_tick is None
    assert state.dims == D._baseline_dims()
    assert state.thoughts == []


def test_save_load_roundtrip_preserves_all():
    base = _tmp()
    s = D.DriveState()
    s.dims["possess"] = 0.55
    D.add_thought(s, "crave", "欠她一篇神父", 0.7)
    now = datetime(2026, 7, 11, 14, 30, tzinfo=TZ)
    S.save_drive(base, s, now, tz=TZ)
    s2, lt = S.load_drive(base)
    assert abs(s2.dims["possess"] - 0.55) < 1e-9
    assert len(s2.thoughts) == 1 and s2.thoughts[0].body == "欠她一篇神父"
    assert lt == now  # last_tick 往返不丢（带时区）


def test_save_is_atomic_no_tmp_left():
    base = _tmp()
    S.save_drive(base, D.DriveState(), datetime.now(TZ), tz=TZ)
    assert os.path.exists(S.drive_path(base))
    assert not os.path.exists(S.drive_path(base) + ".tmp")  # 临时文件已 replace 掉


def test_load_garbage_file_resets():
    base = _tmp()
    with open(S.drive_path(base), "w", encoding="utf-8") as f:
        f.write("{ this is not valid json ]")
    state, last_tick = S.load_drive(base)
    assert last_tick is None
    assert state.dims == D._baseline_dims()  # 坏文件不崩，重置


# ---------- compute_dh ----------
def test_compute_dh_none_is_zero():
    assert S.compute_dh(None, datetime.now(TZ)) == 0.0


def test_compute_dh_normal_gap():
    t0 = datetime(2026, 7, 11, 10, 0, tzinfo=TZ)
    t1 = datetime(2026, 7, 11, 13, 0, tzinfo=TZ)
    assert abs(S.compute_dh(t0, t1) - 3.0) < 1e-9


def test_compute_dh_caps_at_24h():
    t0 = datetime(2026, 7, 10, 0, 0, tzinfo=TZ)
    t1 = datetime(2026, 7, 15, 0, 0, tzinfo=TZ)  # 5 天
    assert S.compute_dh(t0, t1) == S.DRIVE_MAX_DH


def test_compute_dh_negative_clamped_to_zero():
    # 时钟回拨/乱序也不倒扣
    t0 = datetime(2026, 7, 11, 13, 0, tzinfo=TZ)
    t1 = datetime(2026, 7, 11, 10, 0, tzinfo=TZ)
    assert S.compute_dh(t0, t1) == 0.0


def test_compute_dh_naive_last_tick_tolerated():
    naive = datetime(2026, 7, 11, 10, 0)          # 旧文件没带时区
    now = datetime(2026, 7, 11, 13, 0, tzinfo=TZ)
    assert abs(S.compute_dh(naive, now) - 3.0) < 1e-9


# ---------- advance（读→推进→写回）----------
def test_advance_grows_dims_by_elapsed_hours():
    base = _tmp()
    t0 = datetime(2026, 7, 11, 14, 0, tzinfo=TZ)  # 下午，不冻结
    S.save_drive(base, D.DriveState(), t0, tz=TZ)
    basev = D.DriveState().dims["possess"]
    t1 = t0 + timedelta(hours=3)
    state = S.advance(base, t1)
    exp = round(min(1.0, basev + D.GROW_RATES["possess"] * 3), 4)
    assert abs(state.dims["possess"] - exp) < 1e-6
    # last_tick 被推进到 now
    _, lt = S.load_drive(base)
    assert lt == t1


def test_advance_long_sleep_capped():
    base = _tmp()
    t0 = datetime(2026, 7, 11, 14, 0, tzinfo=TZ)
    S.save_drive(base, D.DriveState(), t0, tz=TZ)
    basev = D.DriveState().dims["monitor"]
    state = S.advance(base, t0 + timedelta(hours=100))  # 睡了 100h
    # 被 24h 上限兜住：monitor 最多涨 24h 的量
    exp = round(min(1.0, basev + D.GROW_RATES["monitor"] * S.DRIVE_MAX_DH), 4)
    assert abs(state.dims["monitor"] - exp) < 1e-6


# ---------- intent_line（注入渲染，铁律无数值）----------
def test_intent_line_none_state():
    assert S.intent_line(None, 14) is None


def test_intent_line_is_human_no_numbers():
    line = S.intent_line(D.DriveState(), 14)
    assert line and not any(c.isdigit() for c in line)
    assert "=" not in line


# ---------- seed_from_feel（§4 自动种子）----------
def test_seed_low_valence_lights_grieve():
    s = D.DriveState()
    seeds = S.seed_from_feel(s, valence=0.1, arousal=0.3, body="今天好难过")
    assert s.dims["grieve"] > 0.0
    assert "grieve<-low-valence" in seeds


def test_seed_high_arousal_pushes_crave_thought():
    s = D.DriveState()
    seeds = S.seed_from_feel(s, valence=0.6, arousal=0.9, body="想她想得厉害")
    assert any(t.dim == "crave" for t in s.thoughts)
    assert "crave<-high-arousal" in seeds


def test_seed_neutral_feel_does_nothing():
    s = D.DriveState()
    seeds = S.seed_from_feel(s, valence=0.5, arousal=0.3, body="吃了个饭")
    assert seeds == []
    assert s.dims["grieve"] == 0.0 and not s.thoughts


def test_seed_none_values_safe():
    s = D.DriveState()
    assert S.seed_from_feel(s, valence=None, arousal=None, body="") == []


if __name__ == "__main__":
    import sys, traceback
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL  {fn.__name__}")
            traceback.print_exc()
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
