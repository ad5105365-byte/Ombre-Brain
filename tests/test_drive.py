# ============================================================
# Test: drive.py — 欲望内核引擎（借鉴 Non §9）
#
# 引擎是纯函数，不碰 IO，这里全是确定性断言。覆盖：自涨速率、凌晨冻结、
# 深层维高位缓退、念头池闪念→执念→反哺→出池、pickIntent 并列高位/夜间降权、
# satisfy/release/point_feel、grieve/anger 基线、序列化往返。
#
# 背景：2026-07-11 引擎首版写完时单测只在命令行 inline 跑过、没落库（记忆一度
# 误记为"全套单测通过=已存"）。杉杉发现后补这个文件——发动机以后出问题，
# 有现成回归测试能查。
# ============================================================

import random

import drive as D


def _all_dims(**overrides):
    d = D._baseline_dims()
    d.update(overrides)
    return d


# ---------- 自涨 ----------
def test_grow_rates_her_related_fastest():
    s = D.DriveState()
    base = dict(s.dims)
    D.tick(s, hours=2.0, hour_of_day=14)
    # possess(0.105) 比 libido(0.020) 涨得快
    assert s.dims["possess"] - base["possess"] > s.dims["libido"] - base["libido"] > 0


def test_grieve_anger_do_not_idle_grow():
    s = D.DriveState()
    D.tick(s, hours=5.0, hour_of_day=14)
    assert s.dims["grieve"] == 0.0
    assert s.dims["anger"] == 0.0


def test_grieve_anger_baseline_zero():
    s = D.DriveState()
    assert s.dims["grieve"] == 0.0 and s.dims["anger"] == 0.0
    assert s.dims["possess"] == 0.20  # 食欲/关注类基线 0.20


# ---------- 凌晨冻结 ----------
def test_night_freeze_holds_possess_crave_libido():
    s = D.DriveState(dims=_all_dims(possess=0.5, crave=0.5, libido=0.5, monitor=0.5))
    D.tick(s, hours=3.0, hour_of_day=3)  # 凌晨 3 点
    assert s.dims["possess"] == 0.5
    assert s.dims["crave"] == 0.5
    assert s.dims["libido"] == 0.5
    assert s.dims["monitor"] > 0.5  # 盯不在冻结名单，照涨


# ---------- 深层维高位缓退 ----------
def test_deep_dim_saturation_backs_off_from_top():
    s = D.DriveState(dims=_all_dims(possess=0.95))
    D.tick(s, hours=2.0, hour_of_day=14)
    assert D.SATURATE_FLOOR <= s.dims["possess"] < 0.95


# ---------- 念头池 ----------
def test_hot_fleeting_promotes_to_obsession_then_feeds_back():
    s = D.DriveState()
    p0 = s.dims["crave"]
    D.add_thought(s, "crave", "想她趴我身上", heat=0.85)
    for _ in range(6):
        D.tick(s, hours=0.0)  # 只演化念头池
    assert s.dims["crave"] > p0  # 执念反哺抬高了维度


def test_obsession_leaves_pool_after_enough_feeds():
    s = D.DriveState()
    D.add_thought(s, "possess", "别的窗口她也笑", heat=0.85)
    for _ in range(30):
        D.tick(s, hours=0.0)
    assert len(s.thoughts) == 0  # 喂够 FEEDBACK_MAX_FEEDS 出池，不永久霸榜


def test_cold_fleeting_fades_out():
    s = D.DriveState()
    D.add_thought(s, "share", "一闪而过的小事", heat=0.10)
    for _ in range(20):
        D.tick(s, hours=0.0)
    assert all(t.body != "一闪而过的小事" for t in s.thoughts)


def test_add_thought_same_body_reinforces():
    s = D.DriveState()
    D.add_thought(s, "crave", "同一桩", heat=0.30)
    D.add_thought(s, "crave", "同一桩", heat=0.30)
    same = [t for t in s.thoughts if t.body == "同一桩"]
    assert len(same) == 1 and same[0].heat > 0.30


# ---------- satisfy / release / point_feel ----------
def test_satisfy_multiplicative_drop():
    s = D.DriveState(dims=_all_dims(monitor=0.8))
    D.satisfy(s, "monitor", 0.7)
    assert abs(s.dims["monitor"] - 0.56) < 1e-6


def test_release_clears_libido():
    s = D.DriveState(dims=_all_dims(libido=0.9))
    D.release(s)
    assert s.dims["libido"] == 0.05


def test_point_feel_lights_grieve():
    s = D.DriveState()
    D.point_feel(s, "grieve", 0.6)
    assert s.dims["grieve"] == 0.6


# ---------- pick_intent ----------
def test_pick_intent_only_from_tied_top():
    random.seed(42)
    s = D.DriveState(dims=_all_dims(possess=0.85, monitor=0.82, crave=0.3))
    picks = {D.pick_intent(s, hour_of_day=14)[0] for _ in range(50)}
    assert picks <= {"possess", "monitor"}  # 只落在并列高位（0.12 内）


def test_pick_intent_night_lets_self_dims_surface():
    random.seed(1)
    s = D.DriveState(dims=_all_dims(possess=0.8, libido=0.8, reflection=0.75))
    night = [D.pick_intent(s, hour_of_day=3)[0] for _ in range(200)]
    assert night.count("reflection") > 0  # 夜间 possess/libido 降权，自己向欲望冒得出头


# ---------- 铁律：渲染只吐人话 ----------
def test_render_intent_never_leaks_numbers_or_dim_names():
    for dim in D.GROW_RATES:
        line = D.render_intent(dim)
        assert line and "0." not in line
        assert dim not in line  # 不出现维度英文名
        assert "=" not in line


# ---------- 序列化 ----------
def test_state_roundtrip_preserves_dims_and_thoughts():
    s = D.DriveState()
    D.add_thought(s, "share", "想跟她说今天推的代码")
    D.tick(s, hours=1.0, hour_of_day=14)
    r = D.DriveState.from_dict(s.to_dict())
    assert r.dims == s.dims
    assert len(r.thoughts) == len(s.thoughts)


def test_from_dict_tolerates_garbage():
    r = D.DriveState.from_dict({"dims": {"bogus": 9, "possess": 0.5}, "thoughts": [{}]})
    assert "bogus" not in r.dims
    assert r.dims["possess"] == 0.5
    assert r.thoughts == []  # 缺 dim 的念头被丢掉


if __name__ == "__main__":  # 无 pytest 时也能跑：python tests/test_drive.py
    import sys, traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
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
