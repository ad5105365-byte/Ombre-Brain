#!/usr/bin/env python3
# ============================================================
# SessionStart Hook: auto-breath + dreaming on session start
# 对话开始钩子：自动浮现记忆 + 触发 dreaming
#
# On SessionStart, this script calls the Ombre Brain MCP server's
# breath-hook and dream-hook endpoints, printing results to stdout
# so Claude sees them as session context.
#
# Sequence: breath → dream → feel
# 顺序：呼吸浮现 → 做梦消化 → 读取 feel
#
# Every run ends with a best-effort POST to /hook-log so the flight
# recorder shows client-side executions (and failures) too — a hook
# that dies silently is indistinguishable from a hook that never ran.
# 每次执行结束都向 /hook-log 报到：静默死掉和从没跑过，从此分得清。
#
# Config:
#   OMBRE_HOOK_URL  — override the server URL (default: Render deployment)
#   OMBRE_HOOK_SKIP — set to "1" to disable the hook temporarily
# ============================================================

import json
import os
import sys
import time
import urllib.request
import urllib.error

DEFAULT_URL = "https://ombre-brain-098d.onrender.com"


def main():
    if os.environ.get("OMBRE_HOOK_SKIP") == "1":
        sys.exit(0)

    base_url = os.environ.get("OMBRE_HOOK_URL", DEFAULT_URL).rstrip("/")

    # stdin carries the SessionStart payload: source is one of
    # startup|resume|clear|compact (or something new on remote runners)
    source = ""
    try:
        raw = sys.stdin.read()
        if raw.strip():
            source = str(json.loads(raw).get("source", ""))
    except Exception:
        pass

    t0 = time.monotonic()
    notes = [f"session_breath source={source or '?'}"]

    # --- Step 1: Breath — surface unresolved memories ---
    try:
        output = _call_endpoint(base_url, "/breath-hook")
        if output:
            print(output)
        notes.append(f"breath={len(output)}ch")
    except Exception as e:
        notes.append(f"breath-err={type(e).__name__}")

    # --- Step 2: Dream — digest recent memories ---
    # clear/compact 不重复做梦：消化是新会话/恢复会话才需要的事
    if source in ("", "startup", "resume"):
        try:
            output = _call_endpoint(base_url, "/dream-hook")
            if output:
                print(output)
            notes.append(f"dream={len(output)}ch")
        except Exception as e:
            notes.append(f"dream-err={type(e).__name__}")

    notes.append(f"ms={int((time.monotonic() - t0) * 1000)}")
    _report(base_url, " ".join(notes))


def _call_endpoint(base_url, path):
    req = urllib.request.Request(
        f"{base_url}{path}",
        headers={"Accept": "text/plain"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=25) as response:
        return response.read().decode("utf-8").strip()


def _report(base_url, note):
    try:
        payload = json.dumps({"note": note}).encode("utf-8")
        req = urllib.request.Request(
            f"{base_url}/hook-log",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception:
        pass


if __name__ == "__main__":
    main()
