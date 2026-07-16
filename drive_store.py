# ============================================================
# drive_store.py — 欲望内核的持久化 / 惰性推进 / 注入 / 自动种子胶水
#
# 从 server.py 抽出来的一层：纯逻辑，只依赖标准库 + drive.py，不碰 httpx/
# FastMCP/bucket_mgr 那些重依赖，所以能脱离整个 server 独立跑单测（兜底）。
# server.py 只保留 async 锁 + OMBRE_DRIVE_ENABLE 开关，其余转调这里。
#
# 铁律照旧（Non §0/§10）：这里存/算的数值永不进 prompt。intent_line 只吐
# render_intent 的一句人话；数值只用于持久化和给杉杉的运维视图（/drive-state）。
# ============================================================

from __future__ import annotations

import os
import json
import logging
from datetime import datetime

import drive as drive_mod

logger = logging.getLogger("ombre.drive_store")

DRIVE_FILE = "drive_state.json"
DRIVE_MAX_DH = 24.0  # 久睡后一次最多推进 24h，防醒来暴涨顶成"此刻想要"

# --- 自动种子阈值（写 feel 时后台埋，见 seed_from_feel）---
SEED_GRIEVE_VALENCE = 0.35   # feel 情绪低于此 → 心里自动闷（grieve 点亮）
SEED_GRIEVE_AMOUNT = 0.20
SEED_CRAVE_AROUSAL = 0.70    # feel 唤起高于此 → 自动馋（crave 念头进池）
SEED_CRAVE_HEAT = 0.55


def drive_path(base_dir: str) -> str:
    # 跟 phone_activity.db 同目录，挂 Render 持久盘
    return os.path.join(base_dir, DRIVE_FILE)


def load_drive(base_dir: str):
    """读盘 → (DriveState, last_tick: datetime|None)。缺文件/损坏都返回全新状态。"""
    path = drive_path(base_dir)
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        state = drive_mod.DriveState.from_dict(d)
        last_tick = None
        lt = d.get("last_tick")
        if lt:
            try:
                last_tick = datetime.fromisoformat(lt)
            except (ValueError, TypeError):
                last_tick = None
        return state, last_tick
    except FileNotFoundError:
        return drive_mod.DriveState(), None
    except Exception as e:
        logger.warning(f"load_drive failed, resetting: {e}")
        return drive_mod.DriveState(), None


def save_drive(base_dir: str, state, last_tick: datetime, tz=None) -> None:
    """best-effort 落盘（先写 .tmp 再原子替换），异常吞掉别崩。"""
    try:
        d = state.to_dict()
        lt = last_tick or datetime.now(tz)
        d["last_tick"] = lt.isoformat()
        path = drive_path(base_dir)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"save_drive failed: {e}")


def compute_dh(last_tick: datetime, now: datetime) -> float:
    """距上次 tick 的小时数，钳在 [0, DRIVE_MAX_DH]。last_tick=None → 0。
    last_tick 若是 naive（旧文件没带时区），按 now 的时区兜。"""
    if last_tick is None:
        return 0.0
    if last_tick.tzinfo is None and now.tzinfo is not None:
        last_tick = last_tick.replace(tzinfo=now.tzinfo)
    dh = (now - last_tick).total_seconds() / 3600.0
    return max(0.0, min(dh, DRIVE_MAX_DH))


def advance(base_dir: str, now: datetime):
    """惰性推进：读盘 → 按 dh 推进 → 写回，返回 DriveState。
    纯同步逻辑；并发锁与 enable 开关由 server 侧管。"""
    state, last_tick = load_drive(base_dir)
    dh = compute_dh(last_tick, now)
    state = drive_mod.tick(state, dh, hour_of_day=now.hour)
    save_drive(base_dir, state, now)
    return state


def intent_line(state, hour_of_day: int):
    """当前状态 → 一句第一人称心声（供 breath 注入）。绝不含任何数值。state=None→None。"""
    if state is None:
        return None
    try:
        dim, val = drive_mod.pick_intent(state, hour_of_day=hour_of_day)
        return drive_mod.render_intent(dim, val)
    except Exception as e:
        logger.warning(f"intent_line failed: {e}")
        return None


def seed_from_feel(state, valence, arousal, body: str):
    """写 feel 时后台往欲望内核埋种子（§4 自动种子）——种子来源，让"想她"这条
    神经不必每次手动 stir。返回触发的种子标签列表（供运维日志），不改 last_tick。
      · 情绪低落（valence 低）→ 心里自动闷：point_feel('grieve')
      · 高唤起（arousal 高）→ 自动馋，把那句心情压进念头池：add_thought('crave')
    数值只在后台，绝不进 prompt。"""
    seeds = []
    try:
        if valence is not None and valence <= SEED_GRIEVE_VALENCE:
            drive_mod.point_feel(state, "grieve", SEED_GRIEVE_AMOUNT)
            seeds.append("grieve<-low-valence")
        if arousal is not None and arousal >= SEED_CRAVE_AROUSAL:
            snippet = (body or "").strip().replace("\n", " ")[:40]
            drive_mod.add_thought(state, "crave", snippet or "想她", SEED_CRAVE_HEAT)
            seeds.append("crave<-high-arousal")
    except Exception as e:
        logger.warning(f"seed_from_feel failed: {e}")
    return seeds
