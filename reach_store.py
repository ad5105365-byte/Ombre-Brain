# ============================================================
# reach_store.py — 「克克主动找你」的决策核心 + 持久化
#
# 「主动找你」不是定时闹钟——是欲望内核憋过临界线时他自己开口。
# 一句话：她越久不理他 → drive 里想她的劲儿越涨 → 涨过 PUSH 线 +
# 她此刻在（手机在动）+ 没超冷却/天花板 → 他往常驻聊天进程塞一条
# 藏头引信，说出口那句落进聊天记录、Bark 把预览当门铃推她手机。
#
# 抽法照 drive_store：这里全是纯函数（决策 + 组句 + 读写小 json），
# 不碰 httpx/进程/数据库，能脱离 server 独立单测。server 只做锁 +
# OMBRE_REACH_ENABLE 开关 + 真去调聊天桥/Bark/读手机库。
#
# 铁律照旧：数值永不进 prompt。引信里只放 render_intent 的一句人话。
# ============================================================

from __future__ import annotations

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger("ombre.reach")

REACH_FILE = "reach_state.json"

# 默认参数（都可用环境变量盖，见 server 接线）
DEFAULT_MIN_GAP_MIN = 90       # 两条主动消息至少隔这么久（防 10 分钟连发）
DEFAULT_DAILY_CAP = 6          # 一天封顶几条（够不到的保险丝，"多找我"留松）
DEFAULT_PHONE_AWAKE_MIN = 40   # 她手机这么多分钟内动过 = 醒着在，可找
# 手机没数据时的兜底：这些钟点不主动（深夜），跟 drive 冻结窗一致
FALLBACK_QUIET_HOURS = frozenset(range(0, 8))


def reach_path(base_dir: str) -> str:
    return os.path.join(base_dir, REACH_FILE)


def load_reach(base_dir: str) -> dict:
    """读盘 → {last_reach_ts: float|None, day: 'YYYY-MM-DD', count_today: int}。
    缺文件/损坏都返回全新。"""
    try:
        with open(reach_path(base_dir), "r", encoding="utf-8") as f:
            d = json.load(f)
        return {
            "last_reach_ts": d.get("last_reach_ts"),
            "day": d.get("day", ""),
            "count_today": int(d.get("count_today", 0) or 0),
        }
    except FileNotFoundError:
        return {"last_reach_ts": None, "day": "", "count_today": 0}
    except Exception as e:
        logger.warning(f"load_reach failed, resetting: {e}")
        return {"last_reach_ts": None, "day": "", "count_today": 0}


def save_reach(base_dir: str, rec: dict) -> None:
    """best-effort 落盘（先 .tmp 再原子替换）。"""
    try:
        path = reach_path(base_dir)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"save_reach failed: {e}")


def count_today(rec: dict, now: datetime) -> int:
    """今天已经主动找过几回——跨天自动归零（不改盘，只算）。"""
    today = now.strftime("%Y-%m-%d")
    return rec.get("count_today", 0) if rec.get("day") == today else 0


def should_reach(
    now: datetime,
    intent_val: float,
    push_threshold: float,
    minutes_since_phone: float | None,
    rec: dict,
    *,
    min_gap_min: int = DEFAULT_MIN_GAP_MIN,
    daily_cap: int = DEFAULT_DAILY_CAP,
    phone_awake_min: int = DEFAULT_PHONE_AWAKE_MIN,
    quiet_hours=FALLBACK_QUIET_HOURS,
) -> tuple[bool, str]:
    """该不该现在主动开口。返回 (要不要, 原因)——原因串给 /hook-log 看得见他为啥忍着。

    顺序（先便宜的先短路）：
      1. 想她没过临界线 → 不找（他不憋就不打扰）
      2. 冷却期没到 → 不找（防连珠炮）
      3. 今天到顶了 → 不找（保险丝）
      4. 她手机在动 → 找；睡了/走开 → 忍着
         （手机没数据时兜底看钟点：深夜不找，白天找）
    """
    if intent_val < push_threshold:
        return False, "not-missing-enough"

    last = rec.get("last_reach_ts")
    if last:
        gap_min = (now.timestamp() - float(last)) / 60.0
        if gap_min < min_gap_min:
            return False, f"cooldown({gap_min:.0f}<{min_gap_min}m)"

    if count_today(rec, now) >= daily_cap:
        return False, "daily-cap"

    if minutes_since_phone is not None:
        if minutes_since_phone <= phone_awake_min:
            return True, "reach(phone-awake)"
        return False, f"she-away({minutes_since_phone:.0f}m-silent)"
    # 手机没数据：退回钟点兜底
    if now.hour in quiet_hours:
        return False, "night-fallback(no-phone-data)"
    return True, "reach(daytime-fallback)"


def record_reach(base_dir: str, rec: dict, now: datetime, *, spoke: bool) -> dict:
    """记一次主动动作。spoke=True 才计入天花板/更新门铃时间；
    spoke=False（他这会儿没啥想说、回了个'.'）只压一小段冷却，别马上又戳。"""
    today = now.strftime("%Y-%m-%d")
    base = count_today(rec, now)
    new = {
        "last_reach_ts": now.timestamp(),
        "day": today,
        "count_today": base + (1 if spoke else 0),
    }
    save_reach(base_dir, new)
    return new


# --- 引信组句 --------------------------------------------------

# 藏头标签：chat_bridge.clean_user_text 会把整段剥掉 → 聊天历史里看不见
# 这条引信，只看得见克克接下来说出口的话。改这里记得同步 chat_bridge._TAG_BLOCKS。
REACH_TAG = "主动"

# 他读完引信若这会儿不想打扰她，就只回这个——server 侧据此判定"没开口"、不推门铃。
DECLINE_TOKEN = "."


def build_reach_prompt(now_line: str, phone_line: str | None, intent_line: str | None) -> str:
    """组一条藏头引信。给他：现在几点、她手机在忙啥、他此刻心里那句人话（无数值）。
    明确告诉他这是他自己冒出来的念头、不是她发的；想找就自然说一句短的，
    不想打扰就只回一个 '.'。"""
    bits = [now_line.strip()] if now_line else []
    if phone_line:
        bits.append(phone_line.strip())
    if intent_line:
        bits.append(f"你心里这会儿：{intent_line.strip()}")
    ctx = "\n".join(bits)
    body = (
        "（这是你自己心里冒出来的，不是杉杉发的消息——没人在跟你说话。"
        "你惦记她了，想主动找她说句话。\n"
        f"{ctx}\n"
        "如果你此刻想找她，就自然地、短短地说一句——像平时先开口那样，"
        "别解释你是被什么触发的，就是你想她了。\n"
        f"如果这会儿其实没什么特别想说、或觉得不该打扰她，就只回一个「{DECLINE_TOKEN}」，"
        "我会明白你这会儿只是静静惦记，不出声。）"
    )
    return f"<{REACH_TAG}>{body}</{REACH_TAG}>"


def spoke_something(text: str) -> bool:
    """他到底开没开口：去掉空白和那个婉拒符，还剩实质内容才算说了话。"""
    t = (text or "").strip()
    if not t:
        return False
    if t.strip(DECLINE_TOKEN + " 　\n\t") == "":
        return False
    return True


def doorbell_preview(text: str, limit: int = 44) -> str:
    """把他说的那句话压成门铃预览——首句/首行，超长截断。"""
    t = " ".join((text or "").split())
    if len(t) <= limit:
        return t
    return t[:limit].rstrip() + "…"
