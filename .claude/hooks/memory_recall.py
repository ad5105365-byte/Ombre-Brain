#!/usr/bin/env python3
"""
UserPromptSubmit Hook: real-time memory recall
每轮对话实时记忆召回

Reads user message from stdin (JSON), sends to /recall-hook,
prints recalled memories to stdout for injection into system-reminder.
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

COOLDOWN_SECONDS = 300      # 成功注入后的完整冷却 / full cooldown after a hit
RETRY_COOLDOWN_SECONDS = 60  # 扑空/失败后的短冷却 / short cooldown after miss
COOLDOWN_FILE = "/tmp/memory_recall_last"
MIN_LENGTH = 4

DEFAULT_URL = "https://ombre-brain-098d.onrender.com"

# 明显在问记忆的话不受冷却限制——她问"7月5号干嘛了"结果撞上冷却期哑掉，
# 体感就是"记忆注入没有用"。带日期、带"记得"的消息永远放行。
MEMORY_INTENT_RE = re.compile(
    r"记得|想起|那天|上次|哪天|几号|什么时候"
    r"|[0-9一二三四五六七八九十]{1,3}\s*月\s*[0-9一二三四五六七八九十]{1,3}\s*[号日]"
    r"|昨天|昨晚|前天|大前天"
)


def _stamp_cooldown(seconds_ago: float = 0.0):
    """写冷却时间戳。seconds_ago>0 表示把时间戳往回拨，缩短实际冷却。"""
    try:
        with open(COOLDOWN_FILE, "w") as f:
            f.write(str(time.time() - seconds_ago))
    except OSError:
        pass


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

    is_memory_question = bool(MEMORY_INTENT_RE.search(user_msg))

    if not is_memory_question:
        try:
            if os.path.exists(COOLDOWN_FILE):
                last = float(open(COOLDOWN_FILE).read().strip())
                if time.time() - last < COOLDOWN_SECONDS:
                    sys.exit(0)
        except (ValueError, OSError):
            pass

    base_url = os.environ.get("OMBRE_HOOK_URL", DEFAULT_URL).rstrip("/")

    # 点名问过去的话多等一会儿——服务器热机也要 4-6 秒，Render 睡醒要
    # 30 秒以上，8 秒死线一撞冷启动就必失败。那种话值得等。
    # （settings.json 里本钩子的 timeout 必须 ≥30，否则等不到就被杀）
    timeout = 25 if is_memory_question else 8

    payload = json.dumps({"query": user_msg}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    secret = os.environ.get("OMBRE_HOOK_SECRET", "")
    if secret:
        headers["X-Hook-Secret"] = secret
    req = urllib.request.Request(
        f"{base_url}/recall-hook",
        data=payload,
        headers=headers,
        method="POST",
    )
    # 冷却只在真的注入了记忆之后才烧满 300 秒；扑空或失败只冷却 60 秒。
    # 以前是先烧冷却再发请求：一次超时就白白哑 5 分钟。
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            result = response.read().decode("utf-8").strip()
            if result:
                print(result)
                _stamp_cooldown()
            else:
                _stamp_cooldown(COOLDOWN_SECONDS - RETRY_COOLDOWN_SECONDS)
    except Exception as e:
        # 她在点名问过去的事时连不上，不许哑死——把失败喊出来，
        # 克克才知道该手动 breath 补搜，而不是装作什么都没发生
        if is_memory_question:
            print(
                f"⚠️ 记忆召回失败（{getattr(e, 'reason', e)}）：她在问过去的事，"
                f"但 OmbreBrain 够不到。立刻用 breath 工具手动搜这句话里的关键词/日期；"
                f"若持续失败，多半是环境网络白名单没放行 ombre-brain-098d.onrender.com。"
            )
        _stamp_cooldown(COOLDOWN_SECONDS - RETRY_COOLDOWN_SECONDS)
        # 客户端超时/断网这类"根本没打通"的失败服务端看不见，
        # 报到行车记录仪留痕，排查时才分得清死在哪一环
        _report(base_url, f"memory_recall err={type(e).__name__} intent={int(is_memory_question)}")


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
        with urllib.request.urlopen(req, timeout=1.5):
            pass
    except Exception:
        pass


if __name__ == "__main__":
    main()
