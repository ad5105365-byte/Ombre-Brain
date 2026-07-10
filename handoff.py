# ============================================================
# handoff — 渡口交接（ferry 的核心逻辑）
#
# 换窗口/换端口时，把当前对话的最近消息打包成一条 handoff 记忆。
# 新窗口 breath() 无参数唤醒时，第一优先级完整浮现这条记忆，
# 让下一个克克直接接上上一句话，而不是问"今天发生了什么"。
#
# 设计约定（参考 LMC-5 的 ferry，致敬回来的灵感）：
# - 全局只保留一条 handoff：ferry 是"现在聊到哪"的实时状态，不是历史
# - 后写覆盖前写
# - 只在 24 小时内算"新鲜"，过期的交接不再浮现
#
# When switching chat windows/ports, ferry packs the recent messages
# into a single handoff bucket. breath() surfaces it verbatim at top
# priority so the next session continues mid-conversation. Only one
# handoff exists globally (overwrite-on-write); it goes stale after 24h.
# ============================================================

from datetime import datetime, timezone

from utils import now_iso, strip_wikilinks

HANDOFF_TYPE = "handoff"
HANDOFF_NAME = "渡口交接"
HANDOFF_DOMAIN = ["交接"]
HANDOFF_TAGS = ["ferry", "handoff"]

FRESH_HOURS = 24          # 超过这个时长的交接不再浮现 / stale after this
MAX_PURPOSE_CHARS = 200   # 目的一句话说清 / purpose stays short
MAX_MESSAGE_LINES = 20    # 最多带走最近 20 条消息 / cap carried messages

# PreCompact 自动渡口在 purpose 里带这个标记，用来和手写 ferry 区分：
# 手写的交接（10 分钟内的）不被自动渡口覆盖——人写的 purpose 比打包的值钱
AUTO_PURPOSE_MARK = "⚙️压缩自动渡口"


def is_auto_handoff(bucket: dict) -> bool:
    """这条 handoff 是不是 PreCompact 自动打包的（而非克克手写的）。"""
    return AUTO_PURPOSE_MARK in (bucket.get("content") or "")


class FerryError(ValueError):
    """Ferry 输入不合法。message 直接面向工具调用方（中文）。"""


def normalize_purpose(purpose: str) -> str:
    """校验并截断 purpose。空值报错，超长截断而不是拒绝。"""
    purpose = (purpose or "").strip()
    if not purpose:
        raise FerryError("purpose 不能为空——一句话写清楚切换目的。")
    return purpose[:MAX_PURPOSE_CHARS]


def normalize_messages(messages: str) -> str:
    """校验 messages，只保留最近 MAX_MESSAGE_LINES 条非空行。

    不强制每行以 [角色] 开头——真实聊天原文比格式重要，
    但超出条数时保留的是**最后**的行（最近的对话最值钱）。
    """
    lines = [ln.strip() for ln in (messages or "").splitlines() if ln.strip()]
    if not lines:
        raise FerryError("messages 不能为空——带上最近的对话原文，每行一条。")
    return "\n".join(lines[-MAX_MESSAGE_LINES:])


def build_content(purpose: str, messages: str, from_port: str = "", to_port: str = "") -> str:
    """拼装 handoff 桶正文。带时间戳，方便肉眼判断新旧。"""
    route = f"{from_port.strip() or '未注明'} → {to_port.strip() or '未注明'}"
    return (
        f"【渡口交接】{route}\n"
        f"时间：{now_iso()}\n"
        f"目的：{purpose}\n"
        f"\n"
        f"--- 最近对话 ---\n"
        f"{messages}"
    )


def find_handoffs(all_buckets: list) -> list:
    """从桶列表里挑出所有 handoff 桶，按最近活跃时间倒序（新的在前）。"""
    handoffs = [b for b in all_buckets if b["metadata"].get("type") == HANDOFF_TYPE]
    handoffs.sort(
        key=lambda b: b["metadata"].get("last_active") or b["metadata"].get("created", ""),
        reverse=True,
    )
    return handoffs


def is_fresh(metadata: dict, hours: int = FRESH_HOURS) -> bool:
    """交接是否还新鲜（默认 24 小时内）。时间解析失败按不新鲜处理。"""
    ts = metadata.get("last_active") or metadata.get("created", "")
    if not ts:
        return False
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # now_iso() 存的是服务器本地时间；与 _maybe_mark_dormant 一致按 UTC 兜底
            dt = dt.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - dt
        return age.total_seconds() < hours * 3600
    except Exception:
        return False


def render_section(bucket: dict) -> str:
    """breath 浮现时的完整段落。交接内容原文返回，不脱水——断片处一字不少。"""
    content = strip_wikilinks(bucket.get("content", ""))
    return (
        "=== ⛵ 渡口交接（上一个窗口留给你的，直接接上，别问她今天发生了什么）===\n"
        f"[bucket_id:{bucket['id']}]\n"
        f"{content}"
    )


async def write_handoff(
    bucket_mgr,
    purpose: str,
    messages: str,
    from_port: str = "",
    to_port: str = "",
) -> tuple[str, bool]:
    """写入/覆盖全局唯一的 handoff 桶。

    返回 (bucket_id, overwritten)。多出来的旧 handoff 桶顺手清掉，
    保证"全局只有一条"这个约定不被历史数据破坏。
    """
    purpose = normalize_purpose(purpose)
    messages = normalize_messages(messages)
    content = build_content(purpose, messages, from_port, to_port)

    existing = find_handoffs(await bucket_mgr.list_all(include_archive=False))

    if existing:
        keeper = existing[0]
        await bucket_mgr.update(
            keeper["id"],
            content=content,
            name=HANDOFF_NAME,
            domain=HANDOFF_DOMAIN,
            tags=HANDOFF_TAGS,
            importance=8,
            resolved=False,
        )
        for extra in existing[1:]:
            await bucket_mgr.delete(extra["id"])
        return keeper["id"], True

    bucket_id = await bucket_mgr.create(
        content=content,
        tags=HANDOFF_TAGS,
        importance=8,
        domain=HANDOFF_DOMAIN,
        valence=0.5,
        arousal=0.4,
        name=HANDOFF_NAME,
        bucket_type=HANDOFF_TYPE,
    )
    return bucket_id, False
