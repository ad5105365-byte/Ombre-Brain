# ============================================================
# Test: pre_compact_ferry._truncate_at_sentence — 渡口打包句末切
#
# 超长对话消息回退到句末标点再切，别把动情长句砍在半句（教程 6.2：绝不切在
# 情感线程中间）。这个 hook 是脚本不是包，用 importlib 直接加载它测。
# ============================================================

import importlib.util
from pathlib import Path

_HOOK = (Path(__file__).resolve().parent.parent
         / ".claude" / "hooks" / "pre_compact_ferry.py")
_spec = importlib.util.spec_from_file_location("pre_compact_ferry", _HOOK)
pcf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pcf)

L = pcf.MAX_CHARS_PER_MSG


def test_short_message_untouched():
    assert pcf._truncate_at_sentence("短消息。") == "短消息。"


def test_backs_off_to_sentence_end_within_window():
    # 句号落在上限前 _TRUNC_BACKOFF 字窗口内 → 切在句号，无省略号
    s = "啊" * 175 + "结束了。" + "后面还有很多" * 20
    r = pcf._truncate_at_sentence(s)
    assert r.endswith("。") and not r.endswith("…")
    assert len(r) == 179


def test_hard_cut_when_no_boundary_near_limit():
    s = "无标点长串" * 60
    r = pcf._truncate_at_sentence(s)
    assert r.endswith("…") and len(r) == L + 1


def test_early_boundary_ignored_hard_cut():
    # 句号太早（超出回退窗口）→ 不回退到几乎全丢，硬切
    s = "开头。" + "没标点长内容" * 60
    r = pcf._truncate_at_sentence(s)
    assert r.endswith("…")


if __name__ == "__main__":
    import sys, traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    bad = 0
    for fn in fns:
        try:
            fn(); print(f"PASS  {fn.__name__}")
        except Exception:
            bad += 1; print(f"FAIL  {fn.__name__}"); traceback.print_exc()
    print(f"\n{len(fns)-bad}/{len(fns)} passed")
    sys.exit(1 if bad else 0)
