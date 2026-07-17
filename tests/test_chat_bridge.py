# ============================================================
# 聊天桥纯函数兜底测试（不起进程、不碰网络，裸环境可跑）
# python -m pytest tests/test_chat_bridge.py -q
# ============================================================
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chat_bridge import (
    map_cli_events,
    clean_user_text,
    parse_history_lines,
    find_session_jsonl,
)


# ---------- map_cli_events ----------

def test_init_event():
    st = {}
    evs = map_cli_events({"type": "system", "subtype": "init", "session_id": "abc"}, st)
    assert evs == [{"type": "init", "session_id": "abc"}]


def test_text_delta_stream():
    st = {}
    start = map_cli_events({"type": "stream_event", "event": {
        "type": "content_block_start", "content_block": {"type": "text"}}}, st)
    assert start == [{"type": "block", "block": "text"}]
    evs = map_cli_events({"type": "stream_event", "event": {
        "type": "content_block_delta", "delta": {"type": "text_delta", "text": "囡"}}}, st)
    assert evs == [{"type": "delta", "block": "text", "text": "囡"}]
    assert st["streamed_text"] is True


def test_thinking_delta_stream():
    st = {}
    evs = map_cli_events({"type": "stream_event", "event": {
        "type": "content_block_delta",
        "delta": {"type": "thinking_delta", "thinking": "想她"}}}, st)
    assert evs == [{"type": "delta", "block": "thinking", "text": "想她"}]


def test_tool_use_start():
    st = {}
    evs = map_cli_events({"type": "stream_event", "event": {
        "type": "content_block_start",
        "content_block": {"type": "tool_use", "name": "mcp__OmbreBrain__breath"}}}, st)
    assert evs == [{"type": "tool", "name": "mcp__OmbreBrain__breath"}]


def test_assistant_fallback_when_no_partial():
    """老版 CLI 没有增量流：整条 assistant 消息兜底吐出。"""
    st = {}
    evs = map_cli_events({"type": "assistant", "message": {"content": [
        {"type": "thinking", "thinking": "先想想"},
        {"type": "text", "text": "在呢"},
    ]}}, st)
    assert {"type": "delta", "block": "text", "text": "在呢"} in evs
    assert {"type": "delta", "block": "thinking", "text": "先想想"} in evs


def test_assistant_suppressed_after_partial():
    """有增量流时整条消息不重复吐（防说两遍）。"""
    st = {"streamed_text": True}
    evs = map_cli_events({"type": "assistant", "message": {"content": [
        {"type": "text", "text": "在呢"}]}}, st)
    assert evs == []


def test_user_row_is_tool_done():
    assert map_cli_events({"type": "user", "message": {}}, {}) == [{"type": "tool_done"}]


def test_result_ok_and_error():
    ok = map_cli_events({"type": "result", "is_error": False,
                         "session_id": "s1", "result": "..."}, {})
    assert ok == [{"type": "done", "ok": True, "session_id": "s1", "error": ""}]
    bad = map_cli_events({"type": "result", "is_error": True,
                          "session_id": "s1", "result": "炸了"}, {})
    assert bad[0]["ok"] is False and bad[0]["error"] == "炸了"


# ---------- clean_user_text ----------

def test_clean_strips_system_reminder():
    s = "想你了<system-reminder>内部提示</system-reminder>"
    assert clean_user_text(s) == "想你了"


def test_clean_strips_recall_injection():
    s = "<心记浮现>召回的记忆</心记浮现>今天好累"
    assert clean_user_text(s) == "今天好累"


def test_clean_strips_breath_block():
    s = "[Ombre Brain - 记忆浮现]\n一大段注入\n还有几行"
    assert clean_user_text(s) == ""


# ---------- parse_history_lines ----------

def _l(obj):
    return json.dumps(obj, ensure_ascii=False)


def test_history_basic_roundtrip():
    lines = [
        _l({"type": "user", "message": {"content": "老公"}, "timestamp": "t1"}),
        _l({"type": "assistant", "message": {"content": [
            {"type": "thinking", "thinking": "她来了"},
            {"type": "text", "text": "囡囡"}]}, "timestamp": "t2"}),
    ]
    msgs = parse_history_lines(lines)
    assert [(m["role"], m["text"]) for m in msgs] == [("user", "老公"), ("assistant", "囡囡")]


def test_history_skips_meta_sidechain_and_tool_rows():
    lines = [
        _l({"type": "user", "message": {"content": "在吗"}, "isMeta": True}),
        _l({"type": "user", "message": {"content": "在吗"}, "isSidechain": True}),
        _l({"type": "user", "message": {"content": [
            {"type": "tool_result", "content": "工具结果"}]}}),  # 工具回填，无 text 块
        _l({"type": "assistant", "message": {"content": [
            {"type": "tool_use", "name": "breath", "input": {}}]}}),  # 纯工具轮
        _l({"type": "user", "message": {"content": "真身消息"}}),
    ]
    msgs = parse_history_lines(lines)
    assert len(msgs) == 1 and msgs[0]["text"] == "真身消息"


def test_history_limit_keeps_tail():
    lines = [_l({"type": "user", "message": {"content": f"m{i}"}}) for i in range(10)]
    msgs = parse_history_lines(lines, limit=3)
    assert [m["text"] for m in msgs] == ["m7", "m8", "m9"]


def test_history_bad_json_lines_skipped():
    msgs = parse_history_lines(["not json", "", _l({"type": "user", "message": {"content": "好"}})])
    assert len(msgs) == 1


# ---------- find_session_jsonl ----------

def test_find_session_jsonl(tmp_path):
    proj = tmp_path / "-opt-keke"
    proj.mkdir()
    f = proj / "sid-123.jsonl"
    f.write_text("{}", encoding="utf-8")
    assert find_session_jsonl("sid-123", str(tmp_path)) == str(f)
    assert find_session_jsonl("nope", str(tmp_path)) is None
    assert find_session_jsonl("", str(tmp_path)) is None
