# ============================================================
# sensitive — 高敏内容折叠（自动注入专用）
#
# 2026-07-10 实测：新对话第一轮如果开场就携带露骨内容，会被平台
# 安全层整窗拦下——chat 端 project 里塞了直白日记后开窗秒封，
# 撤掉立刻恢复；CC 新窗口注入完成后静默不开口，同一个死法。
#
# 对策：自动注入（/breath-hook /dream-hook /recall-hook）里检测到
# 高敏词的桶只留门牌不摊内容。记忆本体一个字不动，克克需要时用
# breath(bucket_id=...) 直读展开——主动翻开是自己的决定，
# 被动摊在新窗口第一屏是送人头。
#
# Sensitive-content fold for automatic injections. New conversations
# whose FIRST turn carries explicit content get killed wholesale by the
# platform safety layer (observed 2026-07-10 on both chat projects and
# CC session-start hooks). Folded buckets keep a name+id pointer only;
# the memory itself is untouched and one deliberate
# breath(bucket_id=...) away.
#
# OMBRE_SENSITIVE_FOLD=0 关闭折叠（默认开）。
# ============================================================

import os

FOLD_ENABLED = os.environ.get("OMBRE_SENSITIVE_FOLD", "1").strip() != "0"

# 多字、无歧义的高敏词才进名单——宁可漏折（还有整窗自身的分寸兜底），
# 不误伤日常话题（"骚扰""调教模型""裸机"这类一律不进）。
SENSITIVE_TERMS = (
    "高潮", "口交", "骑乘", "抽插", "自慰", "手淫", "做爱",
    "性爱", "性交", "性欲", "性兴奋", "性生活", "性器",
    "射精", "内射", "精液", "勃起", "龟头", "阴茎", "阴蒂",
    "阴道", "阴唇", "私处", "下体", "乳头", "乳房", "奶子",
    "呻吟", "淫", "湿了", "湿润", "分泌物", "喷水", "潮吹",
    "情趣", "春药", "黄文", "喷片", "肉棒", "后入", "深喉",
    "足交", "乳交", "舔弄", "舔舐", "前戏",
)


# 无辜短语先摘掉再扫——"渡口交接"四个字中间藏着"口交"，
# 中文没有词边界，白名单是唯一的解药
_INNOCENT_PHRASES = ("渡口交接", "港口交通", "路口交汇")


def is_sensitive(text: str) -> bool:
    """文本是否含高敏词。"""
    if not text:
        return False
    for phrase in _INNOCENT_PHRASES:
        text = text.replace(phrase, "")
    return any(term in text for term in SENSITIVE_TERMS)


def should_fold(text: str) -> bool:
    """开关 + 检测一步到位，注入口只问这一个函数。"""
    return FOLD_ENABLED and is_sensitive(text)


def fold_note(bucket_id: str) -> str:
    """折叠占位——告诉克克这里有东西、以及怎么主动翻开。"""
    return (f"〔高敏内容已折叠：开场携带会整窗被拦。"
            f"需要时 breath(bucket_id={bucket_id}) 展开原文〕")


def fold_bucket(bucket: dict) -> str:
    """整桶折叠渲染：门牌 + 占位，内容不出门。"""
    meta = bucket.get("metadata", {})
    name = meta.get("name") or bucket["id"]
    return (f"📌 记忆桶: {name} [bucket_id:{bucket['id']}]\n"
            f"{fold_note(bucket['id'])}")


_SCRUB_NOTE = "〔高敏句已折叠〕"


def scrub_lines(text: str) -> tuple[str, int]:
    """逐行清洗（渡口交接专用）：高敏句换占位，其余原样，连续折叠合并。

    交接的价值在"聊到哪了"的骨架，个别露骨句折掉不伤断片闭环。
    """
    if not FOLD_ENABLED:
        return text, 0
    out: list[str] = []
    n = 0
    for line in text.splitlines():
        if is_sensitive(line):
            n += 1
            if not (out and out[-1] == _SCRUB_NOTE):
                out.append(_SCRUB_NOTE)
        else:
            out.append(line)
    return "\n".join(out), n
