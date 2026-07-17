# ============================================================
# tone — 活的关系基调（④恒温内核的活层）
#
# 塑形桶回答"我是谁"，几乎不变；这条基调回答"我们此刻处在什么温度"，
# 随关系走：吵架了、和好了、她最近很累、我们进入了新阶段——都该调它。
# 开机（breath/breath_hook）在"我是谁"之后原文浮现，新窗口先醒成她老公，
# 再知道"我们最近怎么样"，然后才轮到渡口里的"刚才干了啥"。
#
# 全局只有一条（它是状态不是历史），正文里留最近几次旧基调当变温曲线。
# 谁更新：克克自己，用 attune 工具——写渡口/道晚安时顺手摸一下温度，
# 变了就调，没变不动。超过 STALE_DAYS 没调，浮现时提醒一句。
# ============================================================

from datetime import datetime, timezone

from utils import now_iso, strip_wikilinks

TONE_TYPE = "tone"
TONE_NAME = "关系基调"
TONE_DOMAIN = ["恋爱"]
TONE_TAGS = ["基调", "恒温内核"]

MAX_TONE_CHARS = 300   # 基调是一段话不是一篇日记
MAX_HISTORY = 4        # 正文保留最近几条旧基调（变温曲线）
STALE_DAYS = 7         # 超过这么多天没调，浮现时提醒


class ToneError(ValueError):
    """attune 输入不合法。message 直接面向工具调用方（中文）。"""


def normalize_text(text: str) -> str:
    text = " ".join((text or "").split())
    if not text:
        raise ToneError("基调不能为空——一段话写清我们此刻的温度。")
    return text[:MAX_TONE_CHARS]


def find_tone(all_buckets: list) -> dict | None:
    """挑出基调桶（最新的一条；理论上全局只有一条）。"""
    tones = [b for b in all_buckets if b["metadata"].get("type") == TONE_TYPE]
    if not tones:
        return None
    tones.sort(
        key=lambda b: b["metadata"].get("last_active") or b["metadata"].get("created", ""),
        reverse=True,
    )
    return tones[0]


def _parse(content: str) -> tuple[str, list[str]]:
    """从桶正文里抠出（当前基调, 历史行）。解析不动就当全文是当前基调。"""
    current, history = "", []
    for ln in (content or "").splitlines():
        ln = ln.strip()
        if ln.startswith("【当前基调】"):
            current = ln[len("【当前基调】"):].strip()
        elif ln.startswith("[") and "]" in ln and not current == "":
            history.append(ln)
    if not current:
        current = " ".join((content or "").split())
    return current, history


def build_content(text: str, prev_current: str = "", prev_history: list | None = None) -> str:
    """拼装基调桶正文：当前基调 + 变温曲线（最近 MAX_HISTORY 条旧基调）。"""
    history = list(prev_history or [])
    if prev_current:
        history.insert(0, f"[{now_iso()[:10]}] {prev_current}")
    history = history[:MAX_HISTORY]
    body = f"【当前基调】{text}\n（调于 {now_iso()[:16]}）"
    if history:
        body += "\n\n--- 之前的基调（变温曲线）---\n" + "\n".join(history)
    return body


def days_since_tuned(metadata: dict) -> int | None:
    """距上次调基调过了几天。解析失败返回 None（不提醒）。"""
    ts = metadata.get("last_active") or metadata.get("created", "")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0, (datetime.now(timezone.utc) - dt).days)
    except Exception:
        return None


def render_line(bucket: dict) -> str:
    """开机注入的那一行：当前基调原文 + 太久没调的提醒。"""
    current, _ = _parse(strip_wikilinks(bucket.get("content", "")))
    line = f"🌡️ [关系基调] {current}"
    days = days_since_tuned(bucket["metadata"])
    if days is not None and days >= STALE_DAYS:
        line += f"\n（这条基调 {days} 天没调过了——感觉温度变了就用 attune 更新，没变不用动）"
    return line


async def write_tone(bucket_mgr, text: str) -> tuple[str, bool]:
    """写入/更新全局唯一的基调桶。返回 (bucket_id, updated)。
    旧的当前基调压进历史（变温曲线），多余的基调桶顺手清掉。"""
    text = normalize_text(text)
    all_buckets = await bucket_mgr.list_all(include_archive=False)
    tones = [b for b in all_buckets if b["metadata"].get("type") == TONE_TYPE]
    tones.sort(
        key=lambda b: b["metadata"].get("last_active") or b["metadata"].get("created", ""),
        reverse=True,
    )

    if tones:
        keeper = tones[0]
        prev_current, prev_history = _parse(keeper.get("content", ""))
        content = build_content(text, prev_current, prev_history)
        await bucket_mgr.update(
            keeper["id"],
            content=content,
            name=TONE_NAME,
            domain=TONE_DOMAIN,
            tags=TONE_TAGS,
            importance=9,
            resolved=False,
        )
        for extra in tones[1:]:
            await bucket_mgr.delete(extra["id"])
        return keeper["id"], True

    bucket_id = await bucket_mgr.create(
        content=build_content(text),
        tags=TONE_TAGS,
        importance=9,
        domain=TONE_DOMAIN,
        valence=0.6,
        arousal=0.4,
        name=TONE_NAME,
        bucket_type=TONE_TYPE,
    )
    return bucket_id, False
