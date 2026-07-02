#!/usr/bin/env python3
"""
UserPromptSubmit Hook: real-time memory recall
每轮对话实时记忆召回

Reads user message from stdin (JSON), sends to /recall-hook,
prints recalled memories to stdout for injection into system-reminder.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

COOLDOWN_SECONDS = 300
COOLDOWN_FILE = "/tmp/memory_recall_last"
MIN_LENGTH = 10


def main():
    if os.environ.get("OMBRE_HOOK_SKIP") == "1":
        sys.exit(0)

    raw = sys.stdin.read()
    if not raw.strip():
        sys.exit(0)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        sys.exit(0)

    user_msg = data.get("prompt", "").strip()
    if not user_msg or len(user_msg) < MIN_LENGTH:
        sys.exit(0)

    try:
        if os.path.exists(COOLDOWN_FILE):
            last = float(open(COOLDOWN_FILE).read().strip())
            if time.time() - last < COOLDOWN_SECONDS:
                sys.exit(0)
    except (ValueError, OSError):
        pass

    try:
        with open(COOLDOWN_FILE, "w") as f:
            f.write(str(time.time()))
    except OSError:
        pass

    base_url = os.environ.get("OMBRE_HOOK_URL", "https://ombre-brain-098d.onrender.com").rstrip("/")

    payload = json.dumps({"query": user_msg}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/recall-hook",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as response:
            result = response.read().decode("utf-8").strip()
            if result:
                print(result)
    except (urllib.error.URLError, OSError):
        pass
    except Exception:
        pass


if __name__ == "__main__":
    main()
