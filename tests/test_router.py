# 三分门 Router 测试（安珩反射弧借鉴）
# 核心要防的回归：称呼词"老公"不能把技术消息判成亲密，导致亲密记忆狂冒。
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server


def test_pure_address_and_greeting_suppress():
    assert server._route_query("嗯嗯") == "suppress"
    assert server._route_query("哈哈哈") == "suppress"
    assert server._route_query("老公") == "suppress"        # 纯称呼
    assert server._route_query("好呀老公") == "suppress"
    assert server._route_query("😊😊") == "suppress"
    assert server._route_query("？？") == "suppress"


def test_technical_tool_only():
    assert server._route_query("老公你把 nginx 重启一下") == "tool_only"
    assert server._route_query("部署好了没") == "tool_only"
    assert server._route_query("这个 bug 怎么修") == "tool_only"
    assert server._route_query("VPS 上跑起来了吗") == "tool_only"


def test_intimate_retrieve():
    assert server._route_query("老公我好想你") == "retrieve"
    assert server._route_query("抱抱我") == "retrieve"


def test_relational_retrieve():
    assert server._route_query("你还记得我们第一次见面吗") == "retrieve"
    assert server._route_query("莉莉姐今天来找我了") == "retrieve"
    assert server._route_query("我今天有点难过") == "retrieve"


def test_recall_intent_beats_tech():
    # 明确要回忆，即使夹着技术词也要召回
    assert server._route_query("老公你还记得上次那个 bug 我们怎么修的吗") == "retrieve"


def test_address_alone_is_not_intimate():
    # 关键回归：带称呼的技术消息不能被判成亲密语境
    assert not server._is_intimate_context("老公你部署好没")
    assert not server._is_intimate_context("囡囡帮我看下日志")
    # 真有亲密内容才算
    assert server._is_intimate_context("老公抱抱我")
