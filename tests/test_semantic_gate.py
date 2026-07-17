# ③ 语义门：亲密/摩擦判定走相对赢面（vs 中性锚点），不靠绝对阈值
# ② 摩擦种子：harsh→anger、cold→grieve，且随时间自己消
import os
import sys
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import semantic_gate as sg
import drive as drive_mod
import drive_store


# ---------- GateResult 相对赢面 ----------
def test_intimate_wins_by_margin():
    r = sg.GateResult({"intimate": 0.72, "harsh": 0.50, "cold": 0.50, "neutral": 0.60})
    assert r.intimate and not r.harsh and not r.cold


def test_neutral_wins_no_hit():
    # 技术句：中性锚点最高，谁都不命中——不管绝对值多高
    r = sg.GateResult({"intimate": 0.66, "harsh": 0.60, "cold": 0.58, "neutral": 0.70})
    assert not r.intimate and not r.harsh and not r.cold


def test_low_everything_below_floor_no_hit():
    # 两边都很低（跟啥都不像）：FLOOR 兜底，不命中
    r = sg.GateResult({"intimate": 0.30, "harsh": 0.20, "cold": 0.20, "neutral": 0.10})
    assert not r.intimate and not r.harsh and not r.cold


def test_harsh_and_cold_hit():
    r = sg.GateResult({"intimate": 0.40, "harsh": 0.80, "cold": 0.78, "neutral": 0.55})
    assert r.harsh and r.cold and not r.intimate


def test_note_compact():
    r = sg.GateResult({"intimate": 0.72, "harsh": 0.5, "cold": 0.5, "neutral": 0.6})
    n = r.note()
    assert "[i]" in n and "i=0.72" in n


# ---------- SemanticGate 就绪/回落 ----------
class _FakeEngine:
    """可控假引擎：给每颗种子按组吐一个方向确定的向量。"""
    def __init__(self, enabled=True):
        self.enabled = enabled
        self.model = "fake-model"
        self.calls = 0

    async def _generate_embedding(self, text):
        self.calls += 1
        # 亲密种子 → x 轴，harsh → y，cold → z，neutral → w
        for i, group in enumerate(("intimate", "harsh", "cold", "neutral")):
            if text in sg.SEED_SETS[group]:
                v = [0.0] * 4
                v[i] = 1.0
                return v
        return [0.5, 0.5, 0.5, 0.5]


def test_disabled_engine_not_ready(tmp_path):
    gate = sg.SemanticGate(_FakeEngine(enabled=False), str(tmp_path))
    assert not gate.is_ready()
    assert asyncio.run(gate.ensure_ready()) is False
    assert gate.classify([1, 0, 0, 0]) is None


def test_ready_and_classify(tmp_path):
    gate = sg.SemanticGate(_FakeEngine(), str(tmp_path))
    assert asyncio.run(gate.ensure_ready()) is True
    assert gate.is_ready()
    # 查询向量贴着亲密轴 → intimate 命中
    r = gate.classify([1.0, 0.05, 0.05, 0.05])
    assert r is not None and r.intimate and not r.harsh
    # 贴着中性轴 → 全不命中
    r = gate.classify([0.05, 0.05, 0.05, 1.0])
    assert not r.intimate and not r.harsh and not r.cold


def test_seed_cache_reused(tmp_path):
    e1 = _FakeEngine()
    gate1 = sg.SemanticGate(e1, str(tmp_path))
    asyncio.run(gate1.ensure_ready())
    n_seeds = sum(len(v) for v in sg.SEED_SETS.values())
    assert e1.calls == n_seeds
    # 第二个实例读缓存文件，一次 API 都不打
    e2 = _FakeEngine()
    gate2 = sg.SemanticGate(e2, str(tmp_path))
    asyncio.run(gate2.ensure_ready())
    assert e2.calls == 0
    assert gate2.is_ready()


def test_classify_without_ready_returns_none(tmp_path):
    gate = sg.SemanticGate(_FakeEngine(), str(tmp_path))
    assert gate.classify([1, 0, 0, 0]) is None  # 没焐热 → 回落词表行为


# ---------- ② 摩擦种子 → anger/grieve ----------
def test_friction_harsh_lights_anger():
    state = drive_mod.DriveState()
    kinds = drive_store.seed_from_friction(state, harsh=True)
    assert "anger<-harsh" in kinds
    assert state.dims["anger"] == drive_store.FRICTION_ANGER_AMOUNT
    assert state.dims["grieve"] == drive_store.FRICTION_ANGER_SIDE_GRIEVE


def test_friction_cold_lights_grieve():
    state = drive_mod.DriveState()
    kinds = drive_store.seed_from_friction(state, cold=True)
    assert kinds == ["grieve<-cold"]
    assert state.dims["grieve"] == drive_store.FRICTION_GRIEVE_AMOUNT
    assert state.dims["anger"] == 0.0


def test_friction_nothing_no_change():
    state = drive_mod.DriveState()
    assert drive_store.seed_from_friction(state) == []
    assert state.dims["anger"] == 0.0 and state.dims["grieve"] == 0.0


# ---------- ② 负向情绪自然冷却 ----------
def test_negative_dims_decay_over_time():
    state = drive_mod.DriveState()
    drive_mod.point_feel(state, "anger", 0.30)
    drive_mod.tick(state, hours=5.0, hour_of_day=15)
    # 0.30 - 5*0.03 = 0.15
    assert abs(state.dims["anger"] - 0.15) < 1e-6
    # 消到底就停在 0，不变负
    drive_mod.tick(state, hours=24.0, hour_of_day=15)
    assert state.dims["anger"] == 0.0


def test_repeated_friction_beats_decay():
    # "真呛上了"：同一场架连着呛，衰减压不住，能顶到推力阈值
    state = drive_mod.DriveState()
    for _ in range(5):
        drive_store.seed_from_friction(state, harsh=True)
        drive_mod.tick(state, hours=0.05, hour_of_day=20)  # 每 3 分钟呛一句
    assert state.dims["anger"] >= drive_mod.PUSH_THRESHOLD
