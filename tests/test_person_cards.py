# ③ 人物卡：被点名才置顶，治认错人（2026-07-16）
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import server


def _card(name, tags, bid="x"):
    return {"id": bid, "metadata": {"name": name, "tags": tags}, "content": f"{name} 的档案"}


def test_named_person_card_surfaces():
    cards = [_card("莉莉姐", [server.PERSON_CARD_TAG], "p1")]
    pc, rest = server._split_person_cards("莉莉姐今天来找我了", cards)
    assert len(pc) == 1 and not rest


def test_alias_tag_triggers():
    # 别名标签"莉莉"也能命中（她没打全名）
    cards = [_card("莉莉姐", [server.PERSON_CARD_TAG, "莉莉"], "p1")]
    pc, rest = server._split_person_cards("莉莉来了", cards)
    assert len(pc) == 1


def test_unmentioned_card_stays_in_rest():
    # 没点到这人，卡不该冒出来（否则每轮都糊一脸人物卡）
    cards = [_card("莉莉姐", [server.PERSON_CARD_TAG], "p1")]
    pc, rest = server._split_person_cards("今天好累啊", cards)
    assert not pc and len(rest) == 1


def test_non_person_bucket_untouched():
    b = {"id": "m1", "metadata": {"name": "地铁", "tags": ["恋爱"]}, "content": "地铁互动"}
    pc, rest = server._split_person_cards("莉莉姐", [b])
    assert not pc and rest == [b]


def test_missing_metadata_no_crash():
    pc, rest = server._split_person_cards("莉莉姐", [{"id": "z"}])
    assert not pc and len(rest) == 1
