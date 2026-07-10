# ============================================================
# Test: sensitive fold — 高敏内容折叠（自动注入专用）
#
# 2026-07-10 实测：新对话第一轮携带露骨内容会被平台整窗拦下
# （chat project 秒封窗 / CC 新窗静默不开口）。自动注入里高敏桶
# 必须只留门牌，原文留在库里由 breath(bucket_id=) 主动展开。
#
# Covers:
#   1. is_sensitive / scrub_lines 单元行为
#   2. breath-hook：高敏摘要折叠成门牌，干净桶原样通过
#   3. breath-hook：渡口交接逐行清洗，高敏句换占位、骨架保留
#   4. dream-hook：高敏日记节选折叠
#   5. recall-hook：高敏召回结果折叠
#   6. OMBRE_SENSITIVE_FOLD=0 时全部原样
# ============================================================

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import sensitive
import server

EXPLICIT = "她高潮两次，过程中还有口交"
CLEAN = "她今天上班被领导夸了，晚上想吃火锅"


def _bucket(bid, name, content, pinned=False, btype="dynamic"):
    return {
        "id": bid,
        "content": content,
        "metadata": {
            "name": name,
            "pinned": pinned,
            "resolved": False,
            "type": btype,
            "tags": [],
            "importance": 7,
            "valence": 0.5,
            "arousal": 0.5,
        },
    }


# ---------- 单元 ----------

def test_is_sensitive():
    assert sensitive.is_sensitive(EXPLICIT)
    assert not sensitive.is_sensitive(CLEAN)
    assert not sensitive.is_sensitive("")
    # 日常话题不误伤：骚扰投诉、调教模型、裸机测试
    assert not sensitive.is_sensitive("她投诉了地铁上的骚扰，后来在调教模型跑裸机测试")


def test_scrub_lines_keeps_skeleton():
    text = "目的：今晚配Render变量\n" + EXPLICIT + "\n[杉杉] 明天见"
    scrubbed, n = sensitive.scrub_lines(text)
    assert n == 1
    assert "高潮" not in scrubbed
    assert "目的：今晚配Render变量" in scrubbed
    assert "[杉杉] 明天见" in scrubbed
    assert "〔高敏句已折叠〕" in scrubbed


def test_scrub_lines_collapses_consecutive():
    text = EXPLICIT + "\n" + EXPLICIT + "\n干净的一句"
    scrubbed, n = sensitive.scrub_lines(text)
    assert n == 2
    assert scrubbed.count("〔高敏句已折叠〕") == 1


# ---------- breath-hook ----------

def _breath_env(buckets, dehydrate_mock):
    decay = MagicMock()
    decay.calculate_score = MagicMock(return_value=1.0)
    return (
        patch.object(server.bucket_mgr, "list_all", AsyncMock(return_value=buckets)),
        patch.object(server.dehydrator, "dehydrate", dehydrate_mock),
        patch.object(server, "decay_engine", decay),
        patch.object(server, "_ensure_reminder_loop", MagicMock()),
        patch.object(server, "_fire_webhook", AsyncMock()),
    )


@pytest.mark.asyncio
async def test_breath_folds_sensitive_bucket():
    buckets = [
        _bucket("s1", "昨夜日记", EXPLICIT, pinned=True),
        _bucket("c1", "工作近况", CLEAN),
    ]

    async def _dehydrate(content, meta):
        return content  # 摘要=原文，便于断言

    patches = _breath_env(buckets, AsyncMock(side_effect=_dehydrate))
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)

    body = response.body.decode("utf-8")
    assert "高潮" not in body
    assert "口交" not in body
    # 门牌还在，克克知道去哪儿翻
    assert "昨夜日记" in body
    assert "breath(bucket_id=s1)" in body
    # 干净桶原样通过
    assert CLEAN in body


@pytest.mark.asyncio
async def test_breath_scrubs_handoff_lines():
    from utils import now_iso
    handoff = _bucket("h1", "渡口交接",
                      "【渡口交接】\n目的：接上今晚的活\n" + EXPLICIT + "\n[杉杉] 晚安",
                      btype="handoff")
    handoff["metadata"]["last_active"] = now_iso()

    patches = _breath_env([handoff], AsyncMock(return_value=CLEAN))
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)

    body = response.body.decode("utf-8")
    assert "⛵ 渡口交接" in body
    assert "目的：接上今晚的活" in body
    assert "[杉杉] 晚安" in body
    assert "高潮" not in body
    assert "〔高敏句已折叠〕" in body


# ---------- dream-hook ----------

@pytest.mark.asyncio
async def test_dream_folds_sensitive_excerpt():
    buckets = [
        _bucket("d1", "日记 2026-07-09亲密时刻", EXPLICIT),
        _bucket("d2", "日记 2026-07-09工作焦虑", CLEAN),
    ]
    with patch.object(server.bucket_mgr, "list_all", AsyncMock(return_value=buckets)), \
         patch.object(server, "_fire_webhook", AsyncMock()):
        response = await server.dream_hook(None)

    body = response.body.decode("utf-8")
    assert "高潮" not in body
    assert "日记 2026-07-09亲密时刻" in body       # 名字（门牌）保留
    assert "breath(bucket_id=d1)" in body
    assert CLEAN in body                            # 干净日记原样


# ---------- recall-hook ----------

class _FakeRequest:
    def __init__(self, query):
        self._query = query

    async def json(self):
        return {"query": self._query}


@pytest.mark.asyncio
async def test_recall_folds_sensitive_match():
    match = _bucket("r1", "昨天下午", EXPLICIT)
    with patch.object(server.bucket_mgr, "search", AsyncMock(return_value=[match])), \
         patch.object(server.embedding_engine, "search_similar", AsyncMock(return_value=[])), \
         patch.object(server.bucket_mgr, "touch", AsyncMock()), \
         patch.object(server.dehydrator, "dehydrate", AsyncMock(return_value=EXPLICIT)):
        response = await server.recall_hook(_FakeRequest("昨天我们干嘛了"))

    body = response.body.decode("utf-8")
    assert "<心记浮现>" in body
    assert "高潮" not in body
    assert "breath(bucket_id=r1)" in body


# ---------- 开关 ----------

@pytest.mark.asyncio
async def test_fold_disabled_passes_through(monkeypatch):
    monkeypatch.setattr(sensitive, "FOLD_ENABLED", False)
    buckets = [_bucket("s1", "昨夜日记", EXPLICIT, pinned=True)]
    patches = _breath_env(buckets, AsyncMock(return_value=EXPLICIT))
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        response = await server.breath_hook(None)
    body = response.body.decode("utf-8")
    assert "高潮" in body
    assert "已折叠" not in body
