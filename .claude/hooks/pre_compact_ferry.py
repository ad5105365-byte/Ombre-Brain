#!/usr/bin/env python3
# ============================================================
# PreCompact Hook: auto-ferry before context compaction
# 压缩自动渡口——上下文压缩前，把最近对话打包成渡口交接
#
# Compaction squashes the conversation; whatever the summary drops is
# gone. This hook packs the last 20 real messages into the handoff
# bucket BEFORE compaction, so the post-compact SessionStart breath
# surfaces them verbatim — the thread never snaps.
# 压缩会把对话压成摘要，摘要没带上的就丢了。这个钩子在压缩前把
# 最近 20 条真实对话存成渡口交接，压缩完呼吸原文浮现，断片闭环。
#
# A manual ferry written in the last 10 minutes wins over this
# auto-pack (server-side guard) — 手写的交接比自动打包的值钱。
#
# Config:
#   OMBRE_HOOK_URL     — override the server URL
#   OMBRE_HOOK_SKIP    — set to "1" to disable
#   OMBRE_FERRY_DRYRUN — set to "1" to print the payload instead of POSTing
# ============================================================

import json
import os
import sys
import urllib.request

DEFAULT_URL = "https://ombre-brain-098d.onrender.com"
MAX_MESSAGES = 20
MAX_CHARS_PER_MSG = 200
# 超长消息回退到句末再切，别把一段动情的话砍在半句里（教程 6.2：
# 绝不切在情感线程中间）。在上限前这个窗口内找最后一个句末标点。
_SENTENCE_END = "。！？…!?\n"
_TRUNC_BACKOFF = 60


def _truncate_at_sentence(text, limit=MAX_CHARS_PER_MSG):
    """就近切在句末标点，保留原话完整；找不到边界才硬切。"""
    if len(text) <= limit:
        return text
    window = text[:limit]
    cut = max(window.rfind(ch) for ch in _SENTENCE_END)
    if cut >= limit - _TRUNC_BACKOFF:
        return window[:cut + 1]
    return window + "…"


def _texts_from_content(content):
    if isinstance(content, str):
        return [content]
    parts = []
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
    return parts


def collect_recent_messages(transcript_path):
    """从 transcript JSONL 里捞最近的真实对话（跳过工具结果/侧链/系统注入）。"""
    try:
        with open(transcript_path, encoding="utf-8") as f:
            entries = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        return []

    msgs = []
    for obj in entries:
        typ = obj.get("type")
        if typ not in ("user", "assistant"):
            continue
        if obj.get("isSidechain") or obj.get("isMeta") or obj.get("toolUseResult"):
            continue
        message = obj.get("message") or {}
        texts = _texts_from_content(message.get("content"))
        text = " ".join(t.strip() for t in texts if t and t.strip()).strip()
        if not text or text.startswith("<system-reminder") or text.startswith("["):
            # "[" 开头多为 [SYSTEM NOTIFICATION] / [Request interrupted] 一类的系统注入
            continue
        role = "[杉杉]" if typ == "user" else "[克克]"
        text = _truncate_at_sentence(text)
        msgs.append(f"{role} {text}")
    return msgs[-MAX_MESSAGES:]


def main():
    if os.environ.get("OMBRE_HOOK_SKIP") == "1":
        sys.exit(0)

    base_url = os.environ.get("OMBRE_HOOK_URL", DEFAULT_URL).rstrip("/")

    try:
        data = json.loads(sys.stdin.read())
    except Exception:
        sys.exit(0)

    transcript_path = data.get("transcript_path", "")
    trigger = str(data.get("trigger") or "auto")

    msgs = collect_recent_messages(transcript_path)
    if not msgs:
        _report(base_url, f"pre_compact {trigger} no-messages")
        sys.exit(0)

    payload = {"messages": "\n".join(msgs), "trigger": trigger}

    if os.environ.get("OMBRE_FERRY_DRYRUN") == "1":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        sys.exit(0)

    try:
        req = urllib.request.Request(
            f"{base_url}/ferry-hook",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
        # 成功由服务端记录（_log_hook ferry pre-compact ok），客户端不重复报
    except Exception as e:
        _report(base_url, f"pre_compact {trigger} err={type(e).__name__}")


def _report(base_url, note):
    try:
        payload = json.dumps({"note": note}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        secret = os.environ.get("OMBRE_HOOK_SECRET", "")
        if secret:
            headers["X-Hook-Secret"] = secret
        req = urllib.request.Request(
            f"{base_url}/hook-log",
            data=payload,
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3):
            pass
    except Exception:
        pass


if __name__ == "__main__":
    main()
