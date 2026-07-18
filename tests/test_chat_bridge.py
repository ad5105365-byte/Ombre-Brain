# ============================================================
# 聊天桥纯函数兜底测试（不起进程、不碰网络，裸环境可跑）
# python -m pytest tests/test_chat_bridge.py -q
# ============================================================
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import chat_bridge as chat_bridge_mod
from chat_bridge import (
    ChatBridge,
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


# ---------- 功能1：会话登记册 / 多会话切换 ----------

def test_upsert_session_then_list_sorted_with_lazy_title(tmp_path, monkeypatch):
    """新会话 upsert 进册；缺标题的 list_sessions() 惰性补一个（取第一条用户消息前20字）；
    按 last_ts 倒序，最新在前。"""
    bridge = ChatBridge(state_dir=str(tmp_path))

    proj = tmp_path / "-fake-project"
    proj.mkdir()
    jsonl_path = proj / "sid-old.jsonl"
    jsonl_path.write_text(
        json.dumps({"type": "user", "message": {"content": "今天吃了火锅超级开心开心开心开心"}},
                   ensure_ascii=False) + "\n",
        encoding="utf-8")

    def fake_find(session_id, projects_root=None):
        return str(jsonl_path) if session_id == "sid-old" else None

    monkeypatch.setattr(chat_bridge_mod, "find_session_jsonl", fake_find)

    bridge._upsert_session("sid-old")
    bridge._upsert_session("sid-old")  # 重复 upsert：只刷新 last_ts，不重复追加
    import time as _time
    _time.sleep(0.01)
    bridge._upsert_session("sid-new")  # 后来的，last_ts 更大

    regs_raw = bridge._load_sessions_registry()
    assert len(regs_raw) == 2  # 没有重复条目

    sessions = bridge.list_sessions()
    assert [s["session_id"] for s in sessions] == ["sid-new", "sid-old"]  # 最新在前
    old_entry = [s for s in sessions if s["session_id"] == "sid-old"][0]
    assert old_entry["title"] == "今天吃了火锅超级开心开心开心开心"[:20]  # 前20字截断
    new_entry = [s for s in sessions if s["session_id"] == "sid-new"][0]
    assert new_entry["title"] == "（新对话）"  # 找不到 jsonl，兜底文案
    assert all(s["active"] is False for s in sessions)  # 还没 activate 过谁


def test_active_flag_reflects_load_session(tmp_path):
    bridge = ChatBridge(state_dir=str(tmp_path))
    bridge._upsert_session("sid-a")
    bridge._upsert_session("sid-b")
    bridge.save_session("sid-b")  # 模拟 init 事件后 active 指针指向 sid-b
    sessions = bridge.list_sessions()
    by_id = {s["session_id"]: s for s in sessions}
    assert by_id["sid-b"]["active"] is True
    assert by_id["sid-a"]["active"] is False


def test_activate_session_fails_when_jsonl_missing(tmp_path, monkeypatch):
    bridge = ChatBridge(state_dir=str(tmp_path))
    monkeypatch.setattr(chat_bridge_mod, "find_session_jsonl", lambda sid, projects_root=None: None)
    ok = asyncio.run(bridge.activate_session("no-such-session"))
    assert ok is False
    assert bridge.load_session() == ""


def test_activate_session_switches_active_pointer(tmp_path, monkeypatch):
    bridge = ChatBridge(state_dir=str(tmp_path))
    monkeypatch.setattr(
        chat_bridge_mod, "find_session_jsonl",
        lambda sid, projects_root=None: "/fake/sid-2.jsonl" if sid == "sid-2" else None)
    ok = asyncio.run(bridge.activate_session("sid-2"))
    assert ok is True
    assert bridge.load_session() == "sid-2"
    # 也该已经进了登记册
    assert any(e["session_id"] == "sid-2" for e in bridge._load_sessions_registry())


def test_rename_session(tmp_path):
    bridge = ChatBridge(state_dir=str(tmp_path))
    bridge._upsert_session("sid-x", title="旧标题")
    assert bridge.rename_session("sid-x", "新标题") is True
    sessions = bridge.list_sessions()
    assert sessions[0]["title"] == "新标题"
    assert bridge.rename_session("sid-not-exist", "无效") is False


def test_reset_clears_active_pointer_but_keeps_registry(tmp_path):
    """新对话 = 掐 active 指针，但登记册留着旧会话，能从会话列表切回去。"""
    bridge = ChatBridge(state_dir=str(tmp_path))
    bridge.save_session("sid-9")
    bridge._upsert_session("sid-9")
    asyncio.run(bridge.reset())
    assert bridge.load_session() == ""
    regs = bridge._load_sessions_registry()
    assert any(e["session_id"] == "sid-9" for e in regs)


# ---------- 功能2：聊天页直接贴图（inline base64 image block） ----------

class _FakeStdin:
    def __init__(self):
        self.written = b""

    def write(self, data):
        self.written += data

    async def drain(self):
        pass


class _FakeProc:
    def __init__(self):
        self.returncode = None
        self.stdin = _FakeStdin()


def test_send_user_plain_text_stays_string_without_images(tmp_path):
    """不带图时 content 仍是纯字符串——向后兼容，别破坏老行为。"""
    bridge = ChatBridge(state_dir=str(tmp_path))
    bridge.proc = _FakeProc()
    ok = asyncio.run(bridge._send_user("你好囡囡"))
    assert ok is True
    sent = json.loads(bridge.proc.stdin.written.decode("utf-8"))
    assert sent["message"]["content"] == "你好囡囡"


def test_send_user_with_images_builds_content_array(tmp_path):
    """贴图时 content 变成 Anthropic Messages API 的数组格式：
    [{"type":"text",...}, {"type":"image","source":{...}}]，跟 CC 自己粘图同一套。"""
    bridge = ChatBridge(state_dir=str(tmp_path))
    bridge.proc = _FakeProc()
    images = [{"media_type": "image/png", "data": "QUFBQQ=="}]
    ok = asyncio.run(bridge._send_user("看这张图", images=images))
    assert ok is True
    sent = json.loads(bridge.proc.stdin.written.decode("utf-8"))
    content = sent["message"]["content"]
    assert isinstance(content, list)
    assert content[0] == {"type": "text", "text": "看这张图"}
    assert content[1] == {
        "type": "image",
        "source": {"type": "base64", "media_type": "image/png", "data": "QUFBQQ=="},
    }


def test_send_user_images_without_text_omits_text_block(tmp_path):
    """text 为空但有图：content 数组里不该有空文本块，只有图。"""
    bridge = ChatBridge(state_dir=str(tmp_path))
    bridge.proc = _FakeProc()
    images = [{"media_type": "image/jpeg", "data": "//4A"}]
    ok = asyncio.run(bridge._send_user("", images=images))
    assert ok is True
    sent = json.loads(bridge.proc.stdin.written.decode("utf-8"))
    content = sent["message"]["content"]
    assert content == [{
        "type": "image",
        "source": {"type": "base64", "media_type": "image/jpeg", "data": "//4A"},
    }]


def test_send_user_multiple_images(tmp_path):
    bridge = ChatBridge(state_dir=str(tmp_path))
    bridge.proc = _FakeProc()
    images = [
        {"media_type": "image/png", "data": "AAA="},
        {"media_type": "image/png", "data": "BBB="},
    ]
    ok = asyncio.run(bridge._send_user("两张图", images=images))
    assert ok is True
    sent = json.loads(bridge.proc.stdin.written.decode("utf-8"))
    content = sent["message"]["content"]
    assert len(content) == 3  # 1 个 text + 2 个 image
    assert [c["type"] for c in content] == ["text", "image", "image"]
