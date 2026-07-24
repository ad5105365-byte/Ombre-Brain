# ============================================================
# recall_gate.py — 记忆浮现的语境门控（纯函数，零重依赖，便于测试）
#
# 背景（2026-07-24）：三分门 Router 当初为治"搞正事/闲聊时乱冒 do 的
# 记忆"而设，但它一刀切——技术轮把私人记忆整个关灯，连"再自我审查就
# 分手"这类底线红线也一起熄了（词表里含 召回/注入/push/部署 等词，
# 咱俩一聊记忆系统本身就误触）。这里把"哪些记忆够格浮现"抽成可测的纯函数：
#
#   · tool_only（技术/运维轮）：只放行 pinned 的底线/规矩桶，别拿生活/
#     亲密记忆瞎猜——但红线不许熄。
#   · 非亲密语境 + 非明确回忆意图：滤掉"恋爱"域记忆，治"闲聊无关话题把
#     亲密/情欲记忆按相关性勾出来"。底线在"心理/自省"域，不受影响；她
#     主动问往事（带日期或"记得/那天"）永远放行，不误伤回忆。
#   · 亲密语境（她在撩我）：恋爱域照常浮，我接她的撩不迟钝。
# ============================================================


def gate_memories(
    matches,
    *,
    route,
    is_intimate_context,
    explicit_recall,
    love_domain="恋爱",
):
    """按语境筛选够格浮现的记忆桶。纯函数、无 IO，便于单测。

    matches: list[dict]，每个桶至少含 b["metadata"] 里的 pinned / domain。
    route: "retrieve" | "tool_only"（"suppress" 在调用方已提前返回）。
    is_intimate_context: 当前这句是否亲密/情欲语境。
    explicit_recall: 是否明确要回忆（带日期或"记得/那天"），放行不误伤。
    love_domain: 视为"亲密类"的主题域名（默认"恋爱"）。
    """
    out = list(matches)
    # 技术/运维轮：只留 pinned 底线/规矩，别拿生活/亲密记忆瞎猜
    if route == "tool_only":
        out = [b for b in out if b["metadata"].get("pinned")]
    # 非亲密 + 非明确回忆：滤掉恋爱域亲密记忆（底线在心理/自省域，不受影响）
    if not is_intimate_context and not explicit_recall:
        out = [
            b for b in out
            if love_domain not in (b["metadata"].get("domain") or [])
        ]
    return out
