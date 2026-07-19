#!/usr/bin/env python3
# ============================================================
# Stop Hook: Route Guard — 模型路由漂移可观测（教程第 7 章第一步）
#
# 今早 fable 窗口异常疑似路由漂移，但实际用的哪个模型不可观测。这个
# 钩子在每次回复结束时，从 transcript 末尾抽出 assistant 事件的 model
# 字段，跟目标模型比一比，记一笔到 /hook-log。只观测、不自动恢复——
# 教程 7.1：先让"配置目标 vs 实际响应"可见，恢复流程（Forge）以后再说。
#
# 目标模型来源（优先级）：OMBRE_TARGET_MODEL 环境变量 > ~/.claude
# settings.json 的 "model" 字段。都没有就只记录实际 model、不判漂移。
#
# Config:
#   OMBRE_HOOK_URL       — override server URL
#   OMBRE_HOOK_SKIP      — "1" 关闭
#   OMBRE_TARGET_MODEL   — 显式指定目标模型（覆盖 settings.json）
# ============================================================

import json
import os
import sys
import urllib.request

DEFAULT_URL = "https://ombre-brain-098d.onrender.com"
MODEL_FAMILIES = ("opus", "sonnet", "haiku", "fable")


def _family(model_str):
    """从任意 model 字符串里抽出模型家族（opus/sonnet/haiku/fable）。
    'claude-opus-4-8' -> 'opus'，'opus' -> 'opus'，认不出返回原串小写。"""
    s = (model_str or "").lower()
    for fam in MODEL_FAMILIES:
        if fam in s:
            return fam
    return s.strip()


def _target_model():
    env = os.environ.get("OMBRE_TARGET_MODEL", "").strip()
    if env:
        return env
    try:
        path = os.path.expanduser("~/.claude/settings.json")
        with open(path, encoding="utf-8") as f:
            return str(json.load(f).get("model", "")).strip()
    except Exception:
        return ""


def _last_assistant_model(transcript_path):
    """从 transcript JSONL 尾部找最近一个 assistant 事件的 model 字段。"""
    try:
        with open(transcript_path, encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return ""
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") != "assistant":
            continue
        model = (obj.get("message") or {}).get("model")
        if model:
            return str(model)
    return ""


def main():
    if os.environ.get("OMBRE_HOOK_SKIP") == "1":
        sys.exit(0)
    base_url = os.environ.get("OMBRE_HOOK_URL", DEFAULT_URL).rstrip("/")

    try:
        data = json.loads(sys.stdin.read())
    except Exception:
        sys.exit(0)

    actual = _last_assistant_model(data.get("transcript_path", ""))
    if not actual:
        sys.exit(0)  # 没抽到 model 就别记噪音

    target = _target_model()
    note = f"route model={actual}"
    if target:
        drift = 1 if _family(actual) != _family(target) else 0
        note += f" target={target} drift={drift}"
        if drift:
            # 漂移单独喊一声，flight recorder 里一眼能挑出来
            note = "⚠️ " + note

    _report(base_url, note)


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
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception:
        pass


if __name__ == "__main__":
    main()
