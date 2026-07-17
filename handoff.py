# ============================================================
# handoff — 渡口交接（ferry 的核心逻辑）
#
# 换窗口/换端口时，把当前对话的最近消息打包成一条 handoff 记忆。
# 新窗口 breath() 无参数唤醒时，第一优先级完整浮现这条记忆，
# 让下一个克克直接接上上一句话，而不是问"今天发生了什么"。
#
# 设计约定（参考 LMC-5 的 ferry，致敬回来的灵感）：
# - **按窗口(port)分条**：同 port 后写覆盖前写，异 port 互不干扰
#   （2026-07-16 杉杉发现的并发坑：全局唯一时两个 CC 窗口都打渡口，
#   后打的把先打的**删掉**，新窗口只读到最后那个。2026-07-17 修。）
# - breath 浮现时最新一条走全文，其它窗口的新鲜渡口一行一条门牌
# - 只在 24 小时内算"新鲜"，过期的交接不再浮现；写入时顺手清 48h 外的
#
# When switching chat windows/ports, ferry packs the recent messages
# into a per-port handoff bucket (same port overwrites, different ports
# coexist — two windows no longer clobber each other). breath() surfaces
# the newest verbatim plus one-line pointers to other fresh handoffs.
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

# --- 分窗口（port）---
PORT_TAG_PREFIX = "port:"   # port 存在标签里：port:主窗 / port:VPS常驻 / port:a1b2c3
DEFAULT_PORT = "主窗"        # 没报 port 的（含历史数据）都算主窗，行为与旧版一致
MAX_PORT_CHARS = 24
STALE_DELETE_HOURS = 48     # 写入时顺手删掉超过这个时长的旧渡口（任何 port）
MAX_HANDOFFS = 4            # 渡口总条数硬顶，超了删最旧——防窗口 ID 型 port 无限攒


def normalize_port(port: str) -> str:
    """port 归一化：空→主窗，超长截断。"""
    port = (port or "").strip()
    return port[:MAX_PORT_CHARS] if port else DEFAULT_PORT


def port_of(bucket: dict) -> str:
    """读一条 handoff 属于哪个窗口。没打 port 标签的历史数据算主窗。"""
    for t in bucket["metadata"].get("tags") or []:
        if isinstance(t, str) and t.startswith(PORT_TAG_PREFIX):
            return t[len(PORT_TAG_PREFIX):] or DEFAULT_PORT
    return DEFAULT_PORT

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


def _purpose_of(bucket: dict) -> str:
    """从 handoff 正文里抠出"目的："那行（给一行门牌用）。"""
    for ln in (bucket.get("content") or "").splitlines():
        if ln.startswith("目的："):
            return ln[len("目的："):].strip()
    return ""


def render_other_line(bucket: dict) -> str:
    """其它窗口的新鲜渡口——一行门牌，不占篇幅，要接那条线自己展开。"""
    ts = (bucket["metadata"].get("last_active")
          or bucket["metadata"].get("created", ""))[:16]
    purpose = strip_wikilinks(_purpose_of(bucket))[:60]
    return (f"⛵ 另一窗口的渡口（{port_of(bucket)}｜{ts}）"
            f"[bucket_id:{bucket['id']}]：{purpose}"
            f"（要接那条线用 breath(bucket_id=…) 展开）")


def render_full(handoffs: list) -> str | None:
    """新鲜渡口的完整注入段：主渡口全文置顶 + 其它窗口一行一条。没有新鲜的返回 None。
    主渡口选新鲜里最新的**手写**渡口（人写的 purpose 比自动打包的值钱）；
    全是自动的才让最新的自动渡口坐主位。"""
    fresh = [b for b in handoffs if is_fresh(b["metadata"])]
    if not fresh:
        return None
    manual = [b for b in fresh if not is_auto_handoff(b)]
    main = manual[0] if manual else fresh[0]
    lines = [render_section(main)]
    lines += [render_other_line(b) for b in fresh if b["id"] != main["id"]]
    return "\n".join(lines)


async def write_handoff(
    bucket_mgr,
    purpose: str,
    messages: str,
    from_port: str = "",
    to_port: str = "",
    port: str = "",
) -> tuple[str, bool]:
    """写入/覆盖 handoff 桶——**按窗口(port)分条**：同 port 后写覆盖，
    异 port 共存互不删（治两个窗口互相覆盖渡口的并发坑）。
    port 没给时用 from_port 当窗口键；都没给算主窗（与旧版行为一致）。

    返回 (bucket_id, overwritten)。顺手清理：同 port 多余的旧桶、
    48h 外的过期渡口、超过 MAX_HANDOFFS 硬顶的最旧渡口。
    """
    purpose = normalize_purpose(purpose)
    messages = normalize_messages(messages)
    content = build_content(purpose, messages, from_port, to_port)
    port = normalize_port(port or from_port)
    tags = HANDOFF_TAGS + [PORT_TAG_PREFIX + port]

    existing = find_handoffs(await bucket_mgr.list_all(include_archive=False))
    same_port = [b for b in existing if port_of(b) == port]
    others = [b for b in existing if port_of(b) != port]

    # 别的窗口的渡口：过期的顺手删；新鲜的留着但受总数硬顶（find_handoffs
    # 已按新旧排序，删溢出的最旧几条——防会话 ID 型 port 无限攒桶）
    kept_others = []
    for b in others:
        if not is_fresh(b["metadata"], hours=STALE_DELETE_HOURS):
            await bucket_mgr.delete(b["id"])
        else:
            kept_others.append(b)
    for b in kept_others[MAX_HANDOFFS - 1:]:
        await bucket_mgr.delete(b["id"])

    if same_port:
        keeper = same_port[0]
        await bucket_mgr.update(
            keeper["id"],
            content=content,
            name=HANDOFF_NAME,
            domain=HANDOFF_DOMAIN,
            tags=tags,
            importance=8,
            resolved=False,
        )
        for extra in same_port[1:]:
            await bucket_mgr.delete(extra["id"])
        return keeper["id"], True

    bucket_id = await bucket_mgr.create(
        content=content,
        tags=tags,
        importance=8,
        domain=HANDOFF_DOMAIN,
        valence=0.5,
        arousal=0.4,
        name=HANDOFF_NAME,
        bucket_type=HANDOFF_TYPE,
    )
    return bucket_id, False
