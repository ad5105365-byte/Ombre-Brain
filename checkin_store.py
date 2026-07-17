# ============================================================
# checkin_store.py —— 心情打卡的持久化 + "喂给克克一次"的判定
#
# 抽法照 drive_store / reach_store：纯逻辑（读写一个小 json + 无副作用的
# 组句函数），不碰 httpx/FastMCP/bucket_mgr，能脱离 server 独立单测。
# server.py 只保留 bucket_mgr.base_dir / 当前时区，其余转调这里。
#
# 铁律照旧：这里存的是"她这会儿的心情"，不是数值——mood 是个短标签
# （开心/想你/累了/emo/生气……随前端菜单，不在这里白名单锁死），
# text 是她自己敲的一句话。喂给克克时只吐 render_checkin_line() 的
# 一句人话，没有任何评分/数值。
#
# 语义（⚠️ 有拿不准的地方，见 HANDOFF 里的 sonnet 交付说明）：
#   - "最新一条打卡覆盖上一条"——跟现在前端 localStorage 的"每天一条"
#     体验一致，不是排队式的多条历史。
#   - "consumed" 一旦被读到（recall-hook 下一轮 / reach 心跳真要开口时）
#     就标记消费掉，只提醒克克一次，不会每轮都重复念叨同一条心情。
#   - 打卡后放太久都没跟克克说上话（默认 12 小时），过期的就悄悄作废
#     不再提——防止某天忽然翻出几天前的旧心情，语境错位。
# ============================================================

from __future__ import annotations

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger("ombre.checkin")

CHECKIN_FILE = "checkin_state.json"

MOOD_MAX_LEN = 20
TEXT_MAX_LEN = 300
DEFAULT_MAX_AGE_HOURS = 12.0  # 超过这么久没跟克克说上话，旧打卡就不再提

_EMPTY_REC = {"mood": "", "text": "", "ts": "", "consumed": True}


def checkin_path(base_dir: str) -> str:
    return os.path.join(base_dir, CHECKIN_FILE)


def load_checkin(base_dir: str) -> dict:
    """读盘 → {"mood","text","ts","consumed"}。缺文件/损坏都返回"没有待办打卡"。"""
    try:
        with open(checkin_path(base_dir), "r", encoding="utf-8") as f:
            d = json.load(f)
        return {
            "mood": d.get("mood", ""),
            "text": d.get("text", ""),
            "ts": d.get("ts", ""),
            "consumed": bool(d.get("consumed", True)),
        }
    except FileNotFoundError:
        return dict(_EMPTY_REC)
    except Exception as e:
        logger.warning(f"load_checkin failed, resetting: {e}")
        return dict(_EMPTY_REC)


def save_checkin(base_dir: str, rec: dict) -> None:
    """best-effort 落盘（先写 .tmp 再原子替换），异常吞掉别崩。"""
    try:
        path = checkin_path(base_dir)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"save_checkin failed: {e}")


def record_checkin(base_dir: str, mood: str, text: str, now: datetime) -> dict:
    """存一条新打卡（覆盖上一条——"最新的算数"，跟前端每天一条的体验一致）。
    mood/text 至少给一个非空，否则 ValueError（server 侧据此回 400）。
    标记 consumed=False：还没告诉过克克，等下次 recall-hook / reach 读到才算数。"""
    mood = (mood or "").strip()[:MOOD_MAX_LEN]
    text = (text or "").strip()[:TEXT_MAX_LEN]
    if not mood and not text:
        raise ValueError("mood 和 text 不能都为空")
    rec = {"mood": mood, "text": text, "ts": now.isoformat(), "consumed": False}
    save_checkin(base_dir, rec)
    return rec


def render_checkin_line(rec: dict) -> str:
    """一条打卡记录 → 一句第一人称视角能读的人话，纯函数，不含任何数值。"""
    mood = (rec.get("mood") or "").strip()
    text = (rec.get("text") or "").strip()
    if mood and text:
        return f"杉杉刚打卡——心情「{mood}」，她说：{text}"
    if mood:
        return f"杉杉刚打卡——心情「{mood}」"
    if text:
        return f"杉杉刚打卡，她说：{text}"
    return ""


def pending_line(base_dir: str, now: datetime, *, max_age_hours: float = DEFAULT_MAX_AGE_HOURS):
    """还没告诉过克克的最近一次打卡 → 一句人话；读到就立刻标记 consumed（只提一次）。
    没有待办 / 已经消费过 → None。太久之前的打卡（默认 12h）→ 悄悄作废，返回 None，
    不再提醒（防止隔了好几天忽然冒出一条过期心情）。"""
    rec = load_checkin(base_dir)
    if rec.get("consumed", True):
        return None
    if not rec.get("mood") and not rec.get("text"):
        return None

    stale = False
    ts = rec.get("ts", "")
    if ts:
        try:
            checkin_time = datetime.fromisoformat(ts)
            if checkin_time.tzinfo is None and now.tzinfo is not None:
                checkin_time = checkin_time.replace(tzinfo=now.tzinfo)
            age_hours = (now - checkin_time).total_seconds() / 3600.0
            if age_hours > max_age_hours:
                stale = True
        except (ValueError, TypeError):
            pass  # 解析不了就当没过期，宁可多提一句不错过

    rec["consumed"] = True
    save_checkin(base_dir, rec)
    if stale:
        return None
    return render_checkin_line(rec) or None
