# ============================================================
# Test: recall 语境门控 + breath 声音桶判定（server.py 纯 helper）
#
# 语境门控（借鉴 Non §7）：冷场时 hot 记忆只保留字面强匹配。这里测三个纯
# 判定函数 _is_intimate_context / _is_hot / _is_primer——门控的判定核心。
# （召回过滤本身是 async 端点，见 test_now_injection.py 那类；这里只钉判定。）
#
# 注：import server 需项目测试环境（含 mcp 依赖），随 pytest 一起跑。
# ============================================================

import server


# ---------- _is_intimate_context ----------
def test_intimate_cue_words_trigger():
    # 真·亲密/情欲内容才算
    for msg in ("老公我想你了", "抱抱我", "撩我一下", "想你", "亲亲"):
        assert server._is_intimate_context(msg) is True


def test_address_terms_alone_not_intimate():
    # 2026-07-16 改：称呼词"老公/囡囡"是"叫我"不是"要我"，单独出现不算亲密语境
    # （旧版把它们算亲密，导致她每句都带、门永远开着）
    for msg in ("囡囡", "老公", "老公在吗", "囡囡帮我看日志"):
        assert server._is_intimate_context(msg) is False


def test_sensitive_message_is_intimate():
    # 含高敏词的消息算亲密语境（走 sensitive.is_sensitive）
    assert server._is_intimate_context("我下面湿了") is True


def test_work_message_not_intimate():
    for msg in ("今天方案又被打回来了", "帮我看下这段报错日志", "几点开会"):
        assert server._is_intimate_context(msg) is False


# ---------- _is_hot ----------
def test_hot_by_arousal_threshold():
    assert server._is_hot({"arousal": 0.9}) is True
    assert server._is_hot({"arousal": server.HOT_AROUSAL}) is True  # 边界含
    assert server._is_hot({"arousal": 0.5}) is False


def test_hot_tolerates_missing_or_bad_arousal():
    assert server._is_hot({}) is False
    assert server._is_hot({"arousal": None}) is False
    assert server._is_hot({"arousal": "oops"}) is False


# ---------- _is_primer（声音桶判定）----------
def test_primer_by_tag():
    assert server._is_primer({"tags": [server.PRIMER_TAG]}) is True
    assert server._is_primer({"tags": ["帖子"]}) is False
    assert server._is_primer({}) is False


def test_primer_by_id_fallback():
    for bid in server.PRIMER_BUCKET_IDS:
        assert server._is_primer({"tags": []}, bid) is True
    assert server._is_primer({"tags": []}, "not-a-primer-id") is False


def test_primer_tag_or_id():
    # 标签或 id 命中其一即算
    assert server._is_primer({"tags": [server.PRIMER_TAG]}, "not-a-primer-id") is True
