# ============================================================
# Module: MCP Server Entry Point (server.py)
# 模块：MCP 服务器主入口
#
# Starts the Ombre Brain MCP service and registers memory
# operation tools for Claude to call.
# 启动 Ombre Brain MCP 服务，注册记忆操作工具供 Claude 调用。
#
# Core responsibilities:
# 核心职责：
#   - Initialize config, bucket manager, dehydrator, decay engine
#     初始化配置、记忆桶管理器、脱水器、衰减引擎
#   - Expose 6 MCP tools:
#     暴露 6 个 MCP 工具：
#       breath — Surface unresolved memories or search by keyword
#                浮现未解决记忆 或 按关键词检索
#       hold   — Store a single memory (or write a `feel` reflection)
#                存储单条记忆（或写 feel 反思）
#       grow   — Diary digest, auto-split into multiple buckets
#                日记归档，自动拆分多桶
#       trace  — Modify metadata / resolved / delete
#                修改元数据 / resolved 标记 / 删除
#       pulse  — System status + bucket listing
#                系统状态 + 所有桶列表
#       dream  — Surface recent dynamic buckets for self-digestion
#                返回最近桶 供模型自省/写 feel
#       ferry  — Pack recent conversation into the global handoff bucket
#                渡口交接：换窗口时打包最近对话，新窗口 breath 置顶浮现
#
# Startup:
# 启动方式：
#   Local:  python server.py
#   Remote: OMBRE_TRANSPORT=streamable-http python server.py
#   Docker: docker-compose up
# ============================================================

import os
import re
import sys
import random
import logging
import asyncio
import hashlib
import hmac
import secrets
import time
import json as _json_lib
import base64
from datetime import datetime, timezone, timedelta
import httpx


# --- Ensure same-directory modules can be imported ---
# --- 确保同目录下的模块能被正确导入 ---
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mcp.server.fastmcp import FastMCP

from bucket_manager import BucketManager
from dehydrator import Dehydrator
from decay_engine import DecayEngine
from embedding_engine import EmbeddingEngine
from import_memory import ImportEngine
from utils import load_config, setup_logging, strip_wikilinks, count_tokens_approx
import handoff as handoff_mod

# --- Load config & init logging / 加载配置 & 初始化日志 ---
config = load_config()
setup_logging(config.get("log_level", "INFO"))
logger = logging.getLogger("ombre_brain")

# --- Runtime env vars (port + webhook) / 运行时环境变量 ---
# OMBRE_PORT: HTTP/SSE 监听端口，默认 8000
try:
    OMBRE_PORT = int(os.environ.get("OMBRE_PORT", "8000") or "8000")
except ValueError:
    logger.warning("OMBRE_PORT 不是合法整数，回退到 8000")
    OMBRE_PORT = 8000

# OMBRE_HOOK_URL: 在 breath/dream 被调用后推送事件到该 URL（POST JSON）。
# OMBRE_HOOK_SKIP: 设为 true/1/yes 跳过推送。
# 详见 ENV_VARS.md。
OMBRE_HOOK_URL = os.environ.get("OMBRE_HOOK_URL", "").strip()
OMBRE_HOOK_SKIP = os.environ.get("OMBRE_HOOK_SKIP", "").strip().lower() in ("1", "true", "yes", "on")


async def _fire_webhook(event: str, payload: dict) -> None:
    """
    Fire-and-forget POST to OMBRE_HOOK_URL with the given event payload.
    Failures are logged at WARNING level only — never propagated to the caller.
    """
    if OMBRE_HOOK_SKIP or not OMBRE_HOOK_URL:
        return
    try:
        body = {
            "event": event,
            "timestamp": time.time(),
            "payload": payload,
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(OMBRE_HOOK_URL, json=body)
    except Exception as e:
        logger.warning(f"Webhook push failed ({event} → {OMBRE_HOOK_URL}): {e}")


async def _maybe_mark_dormant(bucket: dict) -> bool:
    """Auto-mark a bucket dormant if: inactive >30 days AND importance <3 AND not pinned."""
    meta = bucket["metadata"]
    if (meta.get("pinned") or meta.get("protected") or meta.get("dormant")
            or int(meta.get("importance", 5)) >= 3):
        return False
    last_active = meta.get("last_active") or meta.get("created", "")
    if not last_active:
        return False
    try:
        dt = datetime.fromisoformat(last_active.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - dt).days > 30:
            await bucket_mgr.update(bucket["id"], dormant=True)
            meta["dormant"] = True
            return True
    except Exception:
        pass
    return False


def _bucket_summary_line(b: dict, score: float = 0.0) -> str:
    """One-line summary for a bucket (used in summary mode)."""
    meta = b["metadata"]
    name = meta.get("name", b["id"])
    domains = ",".join(meta.get("domain", [])) or "-"
    val = meta.get("valence", 0.5)
    aro = meta.get("arousal", 0.3)
    imp = meta.get("importance", "?")
    updated = (meta.get("last_active") or meta.get("created", ""))[:10]
    flags = []
    if meta.get("resolved"):
        flags.append("已解决")
    if meta.get("dormant"):
        flags.append("休眠")
    flag_str = f" [{','.join(flags)}]" if flags else ""
    return (
        f"[bucket_id:{b['id']}] {name}{flag_str} | "
        f"主题:{domains} | V{val:.1f}/A{aro:.1f} | 重要:{imp} | 更新:{updated}"
    )


# --- 云同步：启动前从数据库还原记忆 ---
from cloud_sync import restore_buckets, start_background_sync, get_config, set_config
restore_buckets(config["buckets_dir"])

# --- 从数据库恢复 Bark device key（Render 重启后本地文件会丢失）---
_restored_bark = get_config("bark_device_key")
if _restored_bark:
    _bark_file = os.path.join(config["buckets_dir"], ".bark_key")
    try:
        os.makedirs(os.path.dirname(_bark_file), exist_ok=True)
        with open(_bark_file, "w") as _f:
            _f.write(_restored_bark)
        logger.info("Bark device key 已从数据库恢复")
    except Exception as _e:
        logger.warning(f"Bark device key 恢复失败: {_e}")

# --- Initialize core components / 初始化核心组件 ---
embedding_engine = EmbeddingEngine(config)            # Embedding engine first (BucketManager depends on it)
bucket_mgr = BucketManager(config, embedding_engine=embedding_engine)  # Bucket manager / 记忆桶管理器
dehydrator = Dehydrator(config)                      # Dehydrator / 脱水器
decay_engine = DecayEngine(config, bucket_mgr)       # Decay engine / 衰减引擎
import_engine = ImportEngine(config, bucket_mgr, dehydrator, embedding_engine)  # Import engine / 导入引擎

# --- Create MCP server instance / 创建 MCP 服务器实例 ---
# host="0.0.0.0" so Docker container's SSE is externally reachable
# stdio mode ignores host (no network)
mcp = FastMCP(
    "Ombre Brain",
    host="0.0.0.0",
    port=OMBRE_PORT,
)


# =============================================================
# Dashboard Auth — simple cookie-based session auth
# Dashboard 认证 —— 基于 Cookie 的会话认证
#
# Env var OMBRE_DASHBOARD_PASSWORD overrides file-stored password.
# First visit with no password set → forced setup wizard.
# Sessions stored in memory (lost on restart, 7-day expiry).
# =============================================================
_sessions: dict[str, float] = {}  # {token: expiry_timestamp}


def _get_auth_file() -> str:
    return os.path.join(config["buckets_dir"], ".dashboard_auth.json")


def _load_password_hash() -> str | None:
    try:
        auth_file = _get_auth_file()
        if os.path.exists(auth_file):
            with open(auth_file, "r", encoding="utf-8") as f:
                return _json_lib.load(f).get("password_hash")
    except Exception:
        pass
    return None


def _save_password_hash(password: str) -> None:
    salt = secrets.token_hex(16)
    h = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    auth_file = _get_auth_file()
    os.makedirs(os.path.dirname(auth_file), exist_ok=True)
    with open(auth_file, "w", encoding="utf-8") as f:
        _json_lib.dump({"password_hash": f"{salt}:{h}"}, f)


def _verify_password_hash(password: str, stored: str) -> bool:
    if ":" not in stored:
        return False
    salt, h = stored.split(":", 1)
    return hmac.compare_digest(
        h, hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    )


def _is_setup_needed() -> bool:
    """True if no password is configured (env var or file)."""
    if os.environ.get("OMBRE_DASHBOARD_PASSWORD", ""):
        return False
    return _load_password_hash() is None


def _verify_any_password(password: str) -> bool:
    """Check password against env var (first) or stored hash."""
    env_pwd = os.environ.get("OMBRE_DASHBOARD_PASSWORD", "")
    if env_pwd:
        return hmac.compare_digest(password, env_pwd)
    stored = _load_password_hash()
    if not stored:
        return False
    return _verify_password_hash(password, stored)


def _create_session() -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = time.time() + 86400 * 7  # 7-day expiry
    return token


def _is_authenticated(request) -> bool:
    token = request.cookies.get("ombre_session")
    if not token:
        return False
    expiry = _sessions.get(token)
    if expiry is None or time.time() > expiry:
        _sessions.pop(token, None)
        return False
    return True


def _require_auth(request):
    """Return JSONResponse(401) if not authenticated, else None."""
    from starlette.responses import JSONResponse
    if not _is_authenticated(request):
        return JSONResponse(
            {"error": "Unauthorized", "setup_needed": _is_setup_needed()},
            status_code=401,
        )
    return None


# --- Auth endpoints ---
@mcp.custom_route("/auth/status", methods=["GET"])
async def auth_status(request):
    """Return auth state (authenticated, setup_needed)."""
    from starlette.responses import JSONResponse
    return JSONResponse({
        "authenticated": _is_authenticated(request),
        "setup_needed": _is_setup_needed(),
    })


@mcp.custom_route("/auth/setup", methods=["POST"])
async def auth_setup_endpoint(request):
    """Initial password setup (only when no password is configured)."""
    from starlette.responses import JSONResponse
    if not _is_setup_needed():
        return JSONResponse({"error": "Already configured"}, status_code=400)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    password = body.get("password", "").strip()
    if len(password) < 6:
        return JSONResponse({"error": "密码不能少于6位"}, status_code=400)
    _save_password_hash(password)
    token = _create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie("ombre_session", token, httponly=True, samesite="lax", max_age=86400 * 7)
    return resp


@mcp.custom_route("/auth/login", methods=["POST"])
async def auth_login(request):
    """Login with password."""
    from starlette.responses import JSONResponse
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    password = body.get("password", "")
    if _verify_any_password(password):
        token = _create_session()
        resp = JSONResponse({"ok": True})
        resp.set_cookie("ombre_session", token, httponly=True, samesite="lax", max_age=86400 * 7)
        return resp
    return JSONResponse({"error": "密码错误"}, status_code=401)


@mcp.custom_route("/auth/logout", methods=["POST"])
async def auth_logout(request):
    """Invalidate session."""
    from starlette.responses import JSONResponse
    token = request.cookies.get("ombre_session")
    if token:
        _sessions.pop(token, None)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("ombre_session")
    return resp


@mcp.custom_route("/auth/change-password", methods=["POST"])
async def auth_change_password(request):
    """Change dashboard password (requires current password)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    if os.environ.get("OMBRE_DASHBOARD_PASSWORD", ""):
        return JSONResponse({"error": "当前使用环境变量密码，请直接修改 OMBRE_DASHBOARD_PASSWORD"}, status_code=400)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    current = body.get("current", "")
    new_pwd = body.get("new", "").strip()
    if not _verify_any_password(current):
        return JSONResponse({"error": "当前密码错误"}, status_code=401)
    if len(new_pwd) < 6:
        return JSONResponse({"error": "新密码不能少于6位"}, status_code=400)
    _save_password_hash(new_pwd)
    _sessions.clear()
    token = _create_session()
    resp = JSONResponse({"ok": True})
    resp.set_cookie("ombre_session", token, httponly=True, samesite="lax", max_age=86400 * 7)
    return resp


# =============================================================
# /health endpoint: lightweight keepalive
# 轻量保活接口
# For Cloudflare Tunnel or reverse proxy to ping, preventing idle timeout
# 供 Cloudflare Tunnel 或反代定期 ping，防止空闲超时断连
# =============================================================
@mcp.custom_route("/", methods=["GET"])
async def root_redirect(request):
    from starlette.responses import RedirectResponse
    return RedirectResponse(url="/dashboard")


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    from starlette.responses import JSONResponse
    _ensure_reminder_loop()
    try:
        stats = await bucket_mgr.get_stats()
        return JSONResponse({
            "status": "ok",
            "buckets": stats["permanent_count"] + stats["dynamic_count"],
            "decay_engine": "running" if decay_engine.is_running else "stopped",
        })
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=500)


# =============================================================
# /breath-hook endpoint: Dedicated hook for SessionStart
# 会话启动专用挂载点
# =============================================================
@mcp.custom_route("/breath-hook", methods=["GET"])
async def breath_hook(request):
    from starlette.responses import PlainTextResponse
    _ensure_reminder_loop()
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        # handoff (ferry): fresh handoff goes first, verbatim / 新鲜交接原文置顶
        handoff_section = None
        handoffs = handoff_mod.find_handoffs(all_buckets)
        if handoffs and handoff_mod.is_fresh(handoffs[0]["metadata"]):
            handoff_section = handoff_mod.render_section(handoffs[0])
        all_buckets = [b for b in all_buckets if b["metadata"].get("type") != handoff_mod.HANDOFF_TYPE]
        # pinned
        pinned = [b for b in all_buckets if b["metadata"].get("pinned") or b["metadata"].get("protected")]
        # top 2 unresolved by score
        unresolved = [b for b in all_buckets
                      if not b["metadata"].get("resolved", False)
                      and b["metadata"].get("type") not in ("permanent", "feel")
                      and not b["metadata"].get("pinned")
                      and not b["metadata"].get("protected")]
        scored = sorted(unresolved, key=lambda b: decay_engine.calculate_score(b["metadata"]), reverse=True)

        parts = []
        token_budget = 2500
        if handoff_section:
            parts.append(handoff_section)
            token_budget -= count_tokens_approx(handoff_section)

        # Diversity: top-1 fixed + shuffle rest from top-20
        candidates = list(scored)
        if len(candidates) > 1:
            top1 = [candidates[0]]
            pool = candidates[1:min(20, len(candidates))]
            random.shuffle(pool)
            candidates = top1 + pool + candidates[min(20, len(candidates)):]
        # Hard cap: max 8 surfacing buckets in hook
        candidates = candidates[:8]

        # Dehydrate concurrently: up to 30 serial API calls blow past the
        # client hook timeout, so fan out with a cap to respect API rate limits
        # 并行脱水：串行调 30 次 API 会超过客户端 hook 超时，限流并发
        sem = asyncio.Semaphore(8)

        async def _summarize(b):
            async with sem:
                return await dehydrator.dehydrate(
                    strip_wikilinks(b["content"]),
                    {k: v for k, v in b["metadata"].items() if k != "tags"},
                )

        summaries = await asyncio.gather(
            *(_summarize(b) for b in pinned + candidates),
            return_exceptions=True,
        )
        pinned_summaries = summaries[:len(pinned)]
        candidate_summaries = summaries[len(pinned):]

        for summary in pinned_summaries:
            if isinstance(summary, BaseException):
                logger.warning(f"Breath hook dehydrate failed: {summary}")
                continue
            parts.append(f"📌 [核心准则] {summary}")
            token_budget -= count_tokens_approx(summary)

        for summary in candidate_summaries:
            if token_budget <= 0:
                break
            if isinstance(summary, BaseException):
                logger.warning(f"Breath hook dehydrate failed: {summary}")
                continue
            summary_tokens = count_tokens_approx(summary)
            if summary_tokens > token_budget:
                break
            parts.append(summary)
            token_budget -= summary_tokens

        if not parts:
            await _fire_webhook("breath_hook", {"surfaced": 0})
            return PlainTextResponse("")
        body_text = "[Ombre Brain - 记忆浮现]\n" + "\n---\n".join(parts)
        await _fire_webhook("breath_hook", {"surfaced": len(parts), "chars": len(body_text)})
        return PlainTextResponse(body_text)
    except Exception as e:
        logger.warning(f"Breath hook failed: {e}")
        return PlainTextResponse("")


# =============================================================
# /recall-hook endpoint: Real-time memory recall per user message
# 每轮对话实时记忆召回（UserPromptSubmit hook 调用）
# =============================================================
@mcp.custom_route("/recall-hook", methods=["POST"])
async def recall_hook(request):
    from starlette.responses import PlainTextResponse
    try:
        body = await request.json()
        user_msg = (body.get("query") or "").strip()
        if not user_msg or len(user_msg) < 2:
            return PlainTextResponse("")

        # "7月5号干嘛了" must find the bucket named 2026-07-05 —
        # translate spoken dates to ISO and feed both to keyword search
        # 口语日期翻成 ISO 一起搜，否则按日期问永远搜不到带门牌的日记
        date_hints = _expand_date_expressions(user_msg)
        search_msg = user_msg + (" " + " ".join(date_hints) if date_hints else "")

        # Search via keyword + vector dual channel — run both concurrently:
        # each takes seconds on its own, and serially they eat half the
        # client hook's 8s budget before dehydration even starts
        # 关键词 + 向量双通道并排跑：串行会在脱水开始前就吃掉一半时间预算
        async def _safe_vector_search():
            try:
                return await embedding_engine.search_similar(user_msg, top_k=20)
            except Exception:
                return []

        matches, vector_results = await asyncio.gather(
            bucket_mgr.search(search_msg, limit=20),
            _safe_vector_search(),
        )
        # Exclude dormant
        matches = [b for b in matches if not b["metadata"].get("dormant")]

        # Merge vector channel results
        matched_ids = {b["id"] for b in matches}
        try:
            for bucket_id, sim_score in vector_results:
                if bucket_id not in matched_ids and sim_score > 0.5:
                    bucket = await bucket_mgr.get(bucket_id)
                    if bucket:
                        if bucket["metadata"].get("dormant"):
                            continue
                        bucket["score"] = round(sim_score * 100, 2)
                        bucket["vector_match"] = True
                        matches.append(bucket)
                        matched_ids.add(bucket_id)
        except Exception:
            pass

        # Take top 3
        matches = matches[:3]
        if not matches:
            return PlainTextResponse("")

        # --- Dehydrate in parallel with a hard deadline ---
        # --- 并行脱水 + 死线兜底 ---
        # The client hook gives up after 8s; serial dehydration of 3 uncached
        # buckets takes 30s+ and the recall silently dies. Run summaries
        # concurrently, and past the deadline fall back to a raw excerpt so
        # the recall ALWAYS answers in time. Slow tasks keep running in the
        # background to warm the dehydration cache for the next recall.
        # 客户端钩子 8 秒就放弃；3 个未缓存桶串行脱水要 30 秒以上，召回全部
        # 静默失败。改为并行脱水，超过死线就注入原文节选保证按时交卷；
        # 超时的任务继续后台跑完，把缓存焐热，下次召回就快了。
        async def _summarize(b):
            clean_meta = {k: v for k, v in b["metadata"].items() if k != "tags"}
            return await dehydrator.dehydrate(strip_wikilinks(b["content"]), clean_meta)

        tasks = {b["id"]: asyncio.create_task(_summarize(b)) for b in matches}
        # 2.5s deadline: search floor is ~3-4s, total must stay under the
        # client's 8s / 死线 2.5 秒：搜索本身已占 3-4 秒，总时长必须 <8 秒
        _, pending = await asyncio.wait(tasks.values(), timeout=2.5)
        for t in pending:
            t.add_done_callback(lambda ft: ft.cancelled() or ft.exception())

        parts = []
        token_budget = 1500
        for b in matches:
            if token_budget <= 0:
                break
            try:
                task = tasks[b["id"]]
                if task.done() and not task.exception():
                    summary = task.result()
                else:
                    summary = strip_wikilinks(b["content"]).strip()[:300]
                summary_tokens = count_tokens_approx(summary)
                if summary_tokens > token_budget:
                    break
                await bucket_mgr.touch(b["id"])
                prefix = "[语义关联] " if b.get("vector_match") else ""
                parts.append(f"{prefix}{summary}")
                token_budget -= summary_tokens
            except Exception:
                continue

        if not parts:
            return PlainTextResponse("")

        result = "<心记浮现>\n" + "\n---\n".join(parts) + "\n</心记浮现>"
        return PlainTextResponse(result)
    except Exception as e:
        logger.warning(f"Recall hook failed: {e}")
        return PlainTextResponse("")


# =============================================================
# /dream-hook endpoint: Dedicated hook for Dreaming
# Dreaming 专用挂载点
# =============================================================
@mcp.custom_route("/dream-hook", methods=["GET"])
async def dream_hook(request):
    from starlette.responses import PlainTextResponse
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        candidates = [
            b for b in all_buckets
            if b["metadata"].get("type") not in ("permanent", "feel", handoff_mod.HANDOFF_TYPE)
            and not b["metadata"].get("pinned", False)
            and not b["metadata"].get("protected", False)
        ]
        candidates.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
        recent = candidates[:10]

        if not recent:
            return PlainTextResponse("")

        parts = []
        for b in recent:
            meta = b["metadata"]
            resolved_tag = "[已解决]" if meta.get("resolved", False) else "[未解决]"
            parts.append(
                f"{meta.get('name', b['id'])} {resolved_tag} "
                f"V{meta.get('valence', 0.5):.1f}/A{meta.get('arousal', 0.3):.1f}\n"
                f"{strip_wikilinks(b['content'][:200])}"
            )

        body_text = "[Ombre Brain - Dreaming]\n" + "\n---\n".join(parts)
        await _fire_webhook("dream_hook", {"surfaced": len(parts), "chars": len(body_text)})
        return PlainTextResponse(body_text)
    except Exception as e:
        logger.warning(f"Dream hook failed: {e}")
        return PlainTextResponse("")


# =============================================================
# Internal helper: merge-or-create
# 内部辅助：检查是否可合并，可以则合并，否则新建
# Shared by hold and grow to avoid duplicate logic
# hold 和 grow 共用，避免重复逻辑
# =============================================================
async def _merge_or_create(
    content: str,
    tags: list,
    importance: int,
    domain: list,
    valence: float,
    arousal: float,
    name: str = "",
) -> tuple[str, bool]:
    """
    Check if a similar bucket exists for merging; merge if so, create if not.
    Returns (bucket_id_or_name, is_merged).
    检查是否有相似桶可合并，有则合并，无则新建。
    返回 (桶ID或名称, 是否合并)。
    """
    try:
        existing = await bucket_mgr.search(content, limit=1, domain_filter=domain or None)
    except Exception as e:
        logger.warning(f"Search for merge failed, creating new / 合并搜索失败，新建: {e}")
        existing = []

    if existing and existing[0].get("score", 0) > config.get("merge_threshold", 75):
        bucket = existing[0]
        # --- Diaries never merge across dates: the date is the bucket's identity.
        #     One side being a dated diary is enough to block — merging would keep
        #     the other side's name and knock a day off the dashboard calendar. ---
        # --- 不同日期的日记不许合并：日期是日记的身份证。只要有一边是带日期的
        #     日记就拦——合并会沿用另一边的名字，日历上就缺一天（0304事故）。---
        new_date = _explicit_diary_date(name, content)
        old_date = _explicit_diary_date(
            bucket["metadata"].get("name", ""), bucket.get("content", "")
        )
        cross_date_diary = (new_date or old_date) and new_date != old_date
        # --- Never merge into pinned/protected buckets ---
        # --- 不合并到钉选/保护桶 ---
        if not cross_date_diary and not (
            bucket["metadata"].get("pinned") or bucket["metadata"].get("protected")
        ):
            try:
                merged = await dehydrator.merge(bucket["content"], content)
                old_v = bucket["metadata"].get("valence", 0.5)
                old_a = bucket["metadata"].get("arousal", 0.3)
                merged_valence = round((old_v + valence) / 2, 2)
                merged_arousal = round((old_a + arousal) / 2, 2)
                await bucket_mgr.update(
                    bucket["id"],
                    content=merged,
                    tags=list(set(bucket["metadata"].get("tags", []) + tags)),
                    importance=max(bucket["metadata"].get("importance", 5), importance),
                    domain=list(set(bucket["metadata"].get("domain", []) + domain)),
                    valence=merged_valence,
                    arousal=merged_arousal,
                )
                # --- Update embedding after merge ---
                try:
                    await embedding_engine.generate_and_store(bucket["id"], merged)
                except Exception:
                    pass
                return bucket["metadata"].get("name", bucket["id"]), True
            except Exception as e:
                logger.warning(f"Merge failed, creating new / 合并失败，新建: {e}")

    bucket_id = await bucket_mgr.create(
        content=content,
        tags=tags,
        importance=importance,
        domain=domain,
        valence=valence,
        arousal=arousal,
        name=name or None,
    )
    # --- Generate embedding for new bucket ---
    try:
        await embedding_engine.generate_and_store(bucket_id, content)
    except Exception:
        pass
    return bucket_id, False


# =============================================================
# Tool 1: breath — Breathe
# 工具 1：breath — 呼吸
#
# No args: surface highest-weight unresolved memories (active push)
# 无参数：浮现权重最高的未解决记忆
# With args: search by keyword + emotion coordinates
# 有参数：按关键词+情感坐标检索记忆
# =============================================================
@mcp.tool()
async def breath(
    query: str = "",
    max_tokens: int = 10000,
    domain: str = "",
    valence: float = -1,
    arousal: float = -1,
    max_results: int = 5,
    importance_min: int = -1,
    mode: str = "summary",
    date_from: str = "",
    date_to: str = "",
    include_dormant: bool = False,
    emotion_trend: bool = False,
) -> str:
    """检索/浮现记忆。mode=summary(默认)每桶一行摘要,mode=full返回全文。有query时忽略mode始终full。max_results默认5,超出部分注明数量。date_from/date_to按YYYY-MM-DD过滤。include_dormant=True含休眠桶。emotion_trend=True附情绪时间线。importance_min>=1按重要度批量拉取。"""
    await decay_engine.ensure_started()
    max_results = min(max_results, 50)
    max_tokens = min(max_tokens, 20000)

    # --- importance_min mode: bulk fetch by importance threshold ---
    # --- 重要度批量拉取模式：跳过语义搜索，按 importance 降序返回 ---
    if importance_min >= 1:
        try:
            all_buckets = await bucket_mgr.list_all(include_archive=False)
        except Exception as e:
            return f"记忆系统暂时无法访问: {e}"
        filtered = [
            b for b in all_buckets
            if int(b["metadata"].get("importance", 0)) >= importance_min
            and b["metadata"].get("type") not in ("feel", handoff_mod.HANDOFF_TYPE)
        ]
        filtered.sort(key=lambda b: int(b["metadata"].get("importance", 0)), reverse=True)
        filtered = filtered[:20]
        if not filtered:
            return f"没有重要度 >= {importance_min} 的记忆。"
        results = []
        token_used = 0
        for b in filtered:
            if token_used >= max_tokens:
                break
            try:
                clean_meta = {k: v for k, v in b["metadata"].items() if k != "tags"}
                summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), clean_meta)
                t = count_tokens_approx(summary)
                if token_used + t > max_tokens:
                    break
                imp = b["metadata"].get("importance", 0)
                results.append(f"[importance:{imp}] [bucket_id:{b['id']}] {summary}")
                token_used += t
            except Exception as e:
                logger.warning(f"importance_min dehydrate failed: {e}")
        return "\n---\n".join(results) if results else "没有可以展示的记忆。"

    # --- No args or empty query: surfacing mode (weight pool active push) ---
    # --- 无参数或空query：浮现模式（权重池主动推送）---
    if not query or not query.strip():
        try:
            all_buckets = await bucket_mgr.list_all(include_archive=False)
        except Exception as e:
            logger.error(f"Failed to list buckets for surfacing / 浮现列桶失败: {e}")
            return "记忆系统暂时无法访问。"

        # --- Auto-dormant: lazily mark qualifying buckets ---
        for b in all_buckets:
            if not b["metadata"].get("dormant"):
                await _maybe_mark_dormant(b)

        # --- Handoff (ferry): fresh handoff surfaces verbatim at top priority ---
        # --- 渡口交接：24小时内的 ferry 记录原文置顶浮现，其余流程不见它 ---
        handoff_section = None
        handoffs = handoff_mod.find_handoffs(all_buckets)
        if handoffs and handoff_mod.is_fresh(handoffs[0]["metadata"]):
            handoff_section = handoff_mod.render_section(handoffs[0])
        all_buckets = [
            b for b in all_buckets
            if b["metadata"].get("type") != handoff_mod.HANDOFF_TYPE
        ]

        # --- Filter dormant unless requested ---
        if not include_dormant:
            all_buckets = [b for b in all_buckets if not b["metadata"].get("dormant")]

        # --- Date filter ---
        if date_from or date_to:
            def _in_date_range(b):
                ts = (b["metadata"].get("last_active") or b["metadata"].get("created", ""))[:10]
                if date_from and ts < date_from:
                    return False
                if date_to and ts > date_to:
                    return False
                return True
            all_buckets = [b for b in all_buckets if _in_date_range(b)]

        # --- Pinned/protected buckets: always surface as core principles ---
        # --- 钉选桶：作为核心准则，始终浮现 ---
        pinned_buckets = [
            b for b in all_buckets
            if b["metadata"].get("pinned") or b["metadata"].get("protected")
        ]

        # --- Summary mode for pinned ---
        pinned_results = []
        for b in pinned_buckets:
            if mode == "summary":
                try:
                    score = decay_engine.calculate_score(b["metadata"])
                except Exception:
                    score = 0.0
                pinned_results.append(f"📌 [核心准则] {_bucket_summary_line(b, score)}")
            else:
                try:
                    clean_meta = {k: v for k, v in b["metadata"].items() if k != "tags"}
                    summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), clean_meta)
                    pinned_results.append(f"📌 [核心准则] [bucket_id:{b['id']}] {summary}")
                except Exception as e:
                    logger.warning(f"Failed to dehydrate pinned bucket / 钉选桶脱水失败: {e}")
                    continue

        # --- Unresolved buckets: surface top N by weight ---
        # --- 未解决桶：按权重浮现前 N 条 ---
        unresolved = [
            b for b in all_buckets
            if not b["metadata"].get("resolved", False)
            and b["metadata"].get("type") not in ("permanent", "feel")
            and not b["metadata"].get("pinned", False)
            and not b["metadata"].get("protected", False)
        ]

        logger.info(
            f"Breath surfacing: {len(all_buckets)} total, "
            f"{len(pinned_buckets)} pinned, {len(unresolved)} unresolved"
        )

        scored = sorted(
            unresolved,
            key=lambda b: decay_engine.calculate_score(b["metadata"]),
            reverse=True,
        )

        if scored:
            top_scores = [(b["metadata"].get("name", b["id"]), decay_engine.calculate_score(b["metadata"])) for b in scored[:5]]
            logger.info(f"Top unresolved scores: {top_scores}")

        # --- Cold-start detection: never-seen important buckets surface first ---
        # --- 冷启动检测：从未被访问过且重要度>=8的桶优先插入最前面（最多2个）---
        cold_start = [
            b for b in unresolved
            if int(b["metadata"].get("activation_count", 0)) == 0
            and int(b["metadata"].get("importance", 0)) >= 8
        ][:2]
        cold_start_ids = {b["id"] for b in cold_start}
        scored_deduped = [b for b in scored if b["id"] not in cold_start_ids]
        scored_with_cold = cold_start + scored_deduped

        # --- Token-budgeted surfacing with diversity + hard cap ---
        token_budget = max_tokens
        if handoff_section:
            token_budget -= count_tokens_approx(handoff_section)
        for r in pinned_results:
            token_budget -= count_tokens_approx(r)

        candidates = list(scored_with_cold)
        if len(candidates) > 1:
            n_cold = len(cold_start)
            non_cold = candidates[n_cold:]
            if len(non_cold) > 1:
                top1 = [non_cold[0]]
                pool = non_cold[1:min(20, len(non_cold))]
                random.shuffle(pool)
                non_cold = top1 + pool + non_cold[min(20, len(non_cold)):]
            candidates = cold_start + non_cold

        total_candidates = len(candidates)
        candidates = candidates[:max_results]
        overflow = total_candidates - len(candidates)

        dynamic_results = []
        for b in candidates:
            if token_budget <= 0:
                break
            try:
                score = decay_engine.calculate_score(b["metadata"])
                if mode == "summary":
                    line = _bucket_summary_line(b, score)
                    dynamic_results.append(f"[权重:{score:.2f}] {line}")
                    token_budget -= count_tokens_approx(line)
                else:
                    clean_meta = {k: v for k, v in b["metadata"].items() if k != "tags"}
                    summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), clean_meta)
                    summary_tokens = count_tokens_approx(summary)
                    if summary_tokens > token_budget:
                        break
                    dynamic_results.append(f"[权重:{score:.2f}] [bucket_id:{b['id']}] {summary}")
                    token_budget -= summary_tokens
            except Exception as e:
                logger.warning(f"Failed to surface bucket / 浮现失败: {e}")
                continue

        if not handoff_section and not pinned_results and not dynamic_results:
            return "权重池平静，没有需要处理的记忆。"

        parts = []
        if handoff_section:
            parts.append(handoff_section)
        if pinned_results:
            parts.append("=== 核心准则 ===\n" + "\n---\n".join(pinned_results))
        if dynamic_results:
            section = "=== 浮现记忆 ===\n" + "\n---\n".join(dynamic_results)
            if overflow > 0:
                section += f"\n（还有 {overflow} 个相关桶未显示，可用 max_results 或 query 精确检索）"
            parts.append(section)

        # --- Emotion trend ---
        if emotion_trend:
            try:
                feels = [b for b in await bucket_mgr.list_all(include_archive=True)
                         if b["metadata"].get("type") == "feel" and b["metadata"].get("valence") is not None]
                feels.sort(key=lambda b: b["metadata"].get("created", ""))
                if feels:
                    trend_lines = []
                    for f in feels[-10:]:
                        ts = f["metadata"].get("created", "")[:10]
                        v = f["metadata"].get("valence", 0.5)
                        a = f["metadata"].get("arousal", 0.3)
                        trend_lines.append(f"  {ts} V{v:.2f}/A{a:.2f}")
                    parts.append("=== 情绪时间线（最近10条feel）===\n" + "\n".join(trend_lines))
            except Exception as e:
                logger.warning(f"Emotion trend failed: {e}")

        return "\n\n".join(parts)

    # --- Feel retrieval: domain="feel" is a special channel ---
    # --- Feel 检索：domain="feel" 是独立入口 ---
    if domain.strip().lower() == "feel":
        try:
            all_buckets = await bucket_mgr.list_all(include_archive=False)
            feels = [b for b in all_buckets if b["metadata"].get("type") == "feel"]
            feels.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
            if not feels:
                return "没有留下过 feel。"
            results = []
            for f in feels:
                created = f["metadata"].get("created", "")
                entry = f"[{created}] [bucket_id:{f['id']}]\n{strip_wikilinks(f['content'])}"
                results.append(entry)
                if count_tokens_approx("\n---\n".join(results)) > max_tokens:
                    break
            return "=== 你留下的 feel ===\n" + "\n---\n".join(results)
        except Exception as e:
            logger.error(f"Feel retrieval failed: {e}")
            return "读取 feel 失败。"

    # --- With args: search mode (keyword + vector dual channel) ---
    # --- 有参数：检索模式（关键词 + 向量双通道）---
    domain_filter = [d.strip() for d in domain.split(",") if d.strip()] or None
    q_valence = valence if 0 <= valence <= 1 else None
    q_arousal = arousal if 0 <= arousal <= 1 else None

    try:
        matches = await bucket_mgr.search(
            query,
            limit=max(max_results * 4, 20),
            domain_filter=domain_filter,
            query_valence=q_valence,
            query_arousal=q_arousal,
        )
    except Exception as e:
        logger.error(f"Search failed / 检索失败: {e}")
        return "检索过程出错，请稍后重试。"

    # --- Pinned/protected: keep in query search results ---
    # 主动搜索时不排除钉选桶，用户搜了就该找得到

    # --- Exclude dormant unless requested ---
    if not include_dormant:
        matches = [b for b in matches if not b["metadata"].get("dormant")]

    # --- Date filter ---
    if date_from or date_to:
        def _in_range(b):
            ts = (b["metadata"].get("last_active") or b["metadata"].get("created", ""))[:10]
            if date_from and ts < date_from:
                return False
            if date_to and ts > date_to:
                return False
            return True
        matches = [b for b in matches if _in_range(b)]

    # --- Vector similarity channel: find semantically related buckets ---
    matched_ids = {b["id"] for b in matches}
    try:
        vector_results = await embedding_engine.search_similar(query, top_k=max(max_results * 4, 20))
        for bucket_id, sim_score in vector_results:
            if bucket_id not in matched_ids and sim_score > 0.5:
                bucket = await bucket_mgr.get(bucket_id)
                if bucket:
                    if not include_dormant and bucket["metadata"].get("dormant"):
                        continue
                    bucket["score"] = round(sim_score * 100, 2)
                    bucket["vector_match"] = True
                    matches.append(bucket)
                    matched_ids.add(bucket_id)
    except Exception as e:
        logger.warning(f"Vector search failed, using keyword only / 向量搜索失败: {e}")

    # --- Enforce max_results (pinned not counted) ---
    total_matches = len(matches)
    matches = matches[:max_results]
    overflow = total_matches - len(matches)

    results = []
    token_used = 0
    for bucket in matches:
        if token_used >= max_tokens:
            break
        try:
            clean_meta = {k: v for k, v in bucket["metadata"].items() if k != "tags"}
            if q_valence is not None and "valence" in clean_meta:
                original_v = float(clean_meta.get("valence", 0.5))
                shift = (q_valence - 0.5) * 0.2
                clean_meta["valence"] = max(0.0, min(1.0, original_v + shift))
            summary = await dehydrator.dehydrate(strip_wikilinks(bucket["content"]), clean_meta)
            summary_tokens = count_tokens_approx(summary)
            if token_used + summary_tokens > max_tokens:
                break
            await bucket_mgr.touch(bucket["id"])
            if bucket["metadata"].get("dormant"):
                await bucket_mgr.update(bucket["id"], dormant=False)
            if bucket.get("vector_match"):
                summary = f"[语义关联] [bucket_id:{bucket['id']}] {summary}"
            else:
                summary = f"[bucket_id:{bucket['id']}] {summary}"
            results.append(summary)
            token_used += summary_tokens
        except Exception as e:
            logger.warning(f"Failed to dehydrate search result / 检索结果脱水失败: {e}")
            continue

    # --- Overflow hint ---
    if overflow > 0:
        results.append(f"（还有 {overflow} 个相关桶未显示，可增大 max_results 查看更多）")

    # --- Random surfacing: when search returns < 3, 40% chance to float old memories ---
    if len(matches) < 3 and random.random() < 0.4:
        try:
            all_buckets = await bucket_mgr.list_all(include_archive=False)
            matched_ids_set = {b["id"] for b in matches}
            low_weight = [
                b for b in all_buckets
                if b["id"] not in matched_ids_set
                and not b["metadata"].get("dormant")
                and decay_engine.calculate_score(b["metadata"]) < 2.0
            ]
            if low_weight:
                drifted = random.sample(low_weight, min(random.randint(1, 3), len(low_weight)))
                drift_results = []
                for b in drifted:
                    clean_meta = {k: v for k, v in b["metadata"].items() if k != "tags"}
                    summary = await dehydrator.dehydrate(strip_wikilinks(b["content"]), clean_meta)
                    drift_results.append(f"[surface_type: random]\n{summary}")
                results.append("--- 忽然想起来 ---\n" + "\n---\n".join(drift_results))
        except Exception as e:
            logger.warning(f"Random surfacing failed / 随机浮现失败: {e}")

    if not results:
        await _fire_webhook("breath", {"mode": "empty", "matches": 0})
        return "未找到相关记忆。"

    final_text = "\n---\n".join(results)
    await _fire_webhook("breath", {"mode": "ok", "matches": len(matches), "chars": len(final_text)})
    return final_text


# =============================================================
# Tool 2: hold — Hold on to this
# 工具 2：hold — 握住，留下来
# =============================================================
@mcp.tool()
async def hold(
    content: str,
    tags: str = "",
    importance: int = 5,
    pinned: bool = False,
    feel: bool = False,
    source_bucket: str = "",    valence: float = -1,
    arousal: float = -1,
) -> str:
    """存储单条记忆,自动打标+合并。tags逗号分隔,importance 1-10。pinned=True创建永久钉选桶。feel=True存储你的第一人称感受(不参与普通浮现)。source_bucket=被消化的记忆桶ID(feel模式下,标记源记忆为已消化)。"""
    await decay_engine.ensure_started()

    # --- Input validation / 输入校验 ---
    if not content or not content.strip():
        return "内容为空，无法存储。"

    importance = max(1, min(10, importance))
    extra_tags = [t.strip() for t in tags.split(",") if t.strip()]

    # --- Feel mode: store as feel type, minimal metadata ---
    # --- Feel 模式：存为 feel 类型，最少元数据 ---
    if feel:
        # Feel valence/arousal = model's own perspective
        feel_valence = valence if 0 <= valence <= 1 else 0.5
        feel_arousal = arousal if 0 <= arousal <= 1 else 0.3
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=[],
            importance=5,
            domain=[],
            valence=feel_valence,
            arousal=feel_arousal,
            name=None,
            bucket_type="feel",
        )
        try:
            await embedding_engine.generate_and_store(bucket_id, content)
        except Exception:
            pass
        # --- Mark source memory as digested + store model's valence perspective ---
        # --- 标记源记忆为已消化 + 存储模型视角的 valence ---
        if source_bucket and source_bucket.strip():
            try:
                update_kwargs = {"digested": True}
                if 0 <= valence <= 1:
                    update_kwargs["model_valence"] = feel_valence
                await bucket_mgr.update(source_bucket.strip(), **update_kwargs)
            except Exception as e:
                logger.warning(f"Failed to mark source as digested / 标记已消化失败: {e}")
        return f"🫧feel→{bucket_id}"

    # --- Step 1: auto-tagging / 自动打标 ---
    try:
        analysis = await dehydrator.analyze(content)
    except Exception as e:
        logger.warning(f"Auto-tagging failed, using defaults / 自动打标失败: {e}")
        analysis = {
            "domain": ["未分类"], "valence": 0.5, "arousal": 0.3,
            "tags": [], "suggested_name": "",
        }

    domain = analysis["domain"]
    auto_valence = analysis["valence"]
    auto_arousal = analysis["arousal"]
    auto_tags = analysis["tags"]
    suggested_name = analysis.get("suggested_name", "")

    # --- User-supplied valence/arousal takes priority over analyze() result ---
    # --- 用户显式传入的 valence/arousal 优先，analyze() 结果作为 fallback ---
    final_valence = valence if 0 <= valence <= 1 else auto_valence
    final_arousal = arousal if 0 <= arousal <= 1 else auto_arousal

    all_tags = list(dict.fromkeys(auto_tags + extra_tags))

    # --- Pinned buckets bypass merge and are created directly in permanent dir ---
    # --- 钉选桶跳过合并，直接新建到 permanent 目录 ---
    if pinned:
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=all_tags,
            importance=10,
            domain=domain,
            valence=final_valence,
            arousal=final_arousal,
            name=suggested_name or None,
            bucket_type="permanent",
            pinned=True,
        )
        try:
            await embedding_engine.generate_and_store(bucket_id, content)
        except Exception:
            pass
        return f"📌钉选→{bucket_id} {','.join(domain)}"

    # --- Step 2: merge or create / 合并或新建 ---
    result_name, is_merged = await _merge_or_create(
        content=content,
        tags=all_tags,
        importance=importance,
        domain=domain,
        valence=final_valence,
        arousal=final_arousal,
        name=suggested_name,
    )

    action = "合并→" if is_merged else "新建→"
    return f"{action}{result_name} {','.join(domain)}"


# =============================================================
# Tool 3: grow — Grow, fragments become memories
# 工具 3：grow — 生长，一天的碎片长成记忆
# =============================================================
_DIARY_TZ = timezone(timedelta(hours=8))  # 杉杉在深圳 / dashboard dates are hers


def _extract_diary_date(content: str) -> str:
    """Return YYYY-MM-DD when content opens with a diary marker, else ''."""
    head = content.strip()[:80]
    # Must OPEN with the marker — merely mentioning 日记 mid-text doesn't count
    # 必须以日记标记开头——正文里提到"日记"两个字不算
    if not re.match(r"^【?\s*日记", head):
        return ""
    m = re.search(r"(\d{4})\s*[-./年]\s*(\d{1,2})\s*[-./月]\s*(\d{1,2})", head)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    now = datetime.now(_DIARY_TZ)
    m = re.search(r"(\d{1,2})\s*[-./月]\s*(\d{1,2})", head)
    if m:
        return f"{now.year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return now.strftime("%Y-%m-%d")


def _diary_name(name: str, diary_date: str) -> str:
    """Re-attach the calendar prefix digest tends to strip from bucket names."""
    if not diary_date or "日记" in (name or ""):
        return name
    return f"【日记 {diary_date}】{name or ''}".strip()


_DIARY_DATE_RE = re.compile(r"(\d{4})\s*[-./年]\s*(\d{1,2})\s*[-./月]\s*(\d{1,2})")


def _explicit_diary_date(name: str, content: str = "") -> str:
    """Explicit YYYY-MM-DD of a diary bucket; '' when not a diary or no date written out.

    Unlike _extract_diary_date this never falls back to today —
    a guessed date must not veto a merge or stamp a rename.
    跟 _extract_diary_date 不同，这里绝不用"今天"兜底——
    猜出来的日期没资格否决合并或盖到桶名上。
    """
    for text in (name or "", (content or "").strip()[:80]):
        if "日记" not in text:
            continue
        m = _DIARY_DATE_RE.search(text)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return ""


# --- Date expressions in questions vs dates on bucket names ---
# 她问"7月5号我们干嘛了"，桶名写的是 2026-07-05——两种写法互相认不出，
# 召回就拿最像的错桶交差。这里把口语日期翻译成 ISO 再喂给搜索。
_CN_DIGITS = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
              "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}

_DATE_EXPR_RE = re.compile(
    r"([0-9一二三四五六七八九十]{1,3})\s*月\s*([0-9一二三四五六七八九十]{1,3})\s*[号日]"
)

_RELATIVE_DAYS = {"今天": 0, "今晚": 0, "昨天": 1, "昨晚": 1, "前天": 2, "大前天": 3}


def _cn_num(s: str) -> int:
    """'7'→7, '七'→7, '十五'→15, '二十一'→21; 0 when unparseable."""
    if s.isdigit():
        return int(s)
    if not s:
        return 0
    if "十" in s:
        tens_part, _, ones_part = s.partition("十")
        tens = _CN_DIGITS.get(tens_part, 0) if tens_part else 1
        ones = _CN_DIGITS.get(ones_part, 0) if ones_part else 0
        if tens_part and tens == 0:
            return 0
        return tens * 10 + ones
    return _CN_DIGITS.get(s, 0)


def _expand_date_expressions(text: str, now: datetime | None = None) -> list[str]:
    """ISO dates mentioned in text: '7月5号'/'七月五号'/'昨天' → ['2026-07-05', …]."""
    now = now or datetime.now(_DIARY_TZ)
    found = []
    for m in _DATE_EXPR_RE.finditer(text):
        mo, day = _cn_num(m.group(1)), _cn_num(m.group(2))
        if 1 <= mo <= 12 and 1 <= day <= 31:
            year = now.year
            # Asking about a month far ahead of now means last year's
            # 问一个远在未来的月份，多半说的是去年
            if mo - now.month > 1:
                year -= 1
            found.append(f"{year}-{mo:02d}-{day:02d}")
    for word, delta in _RELATIVE_DAYS.items():
        if word in text:
            iso = (now - timedelta(days=delta)).strftime("%Y-%m-%d")
            if iso not in found:
                found.append(iso)
    return found


@mcp.tool()
async def grow(content: str) -> str:
    """日记归档,自动拆分为多桶。短内容(<30字)走快速路径。"""
    await decay_engine.ensure_started()

    if not content or not content.strip():
        return "内容为空，无法整理。"

    # Detect diary marker up front: digest rewrites bucket names and used to
    # drop the 【日记 YYYY-MM-DD】 prefix the dashboard calendar keys on
    # 提前识别日记标记：digest 重写桶名时会吃掉日历依赖的日期前缀，这里焊回去
    diary_date = _extract_diary_date(content)

    # --- Short content fast path: skip digest, use hold logic directly ---
    # --- 短内容快速路径：跳过 digest 拆分，直接走 hold 逻辑省一次 API ---
    # For very short inputs (like "1"), calling digest is wasteful:
    # it sends the full DIGEST_PROMPT (~800 tokens) to DeepSeek for nothing.
    # Instead, run analyze + create directly.
    if len(content.strip()) < 30:
        logger.info(f"grow short-content fast path: {len(content.strip())} chars")
        try:
            analysis = await dehydrator.analyze(content)
        except Exception as e:
            logger.warning(f"Fast-path analyze failed / 快速路径打标失败: {e}")
            analysis = {
                "domain": ["未分类"], "valence": 0.5, "arousal": 0.3,
                "tags": [], "suggested_name": "",
            }
        result_name, is_merged = await _merge_or_create(
            content=content.strip(),
            tags=analysis.get("tags", []),
            importance=analysis.get("importance", 5) if isinstance(analysis.get("importance"), int) else 5,
            domain=analysis.get("domain", ["未分类"]),
            valence=analysis.get("valence", 0.5),
            arousal=analysis.get("arousal", 0.3),
            name=_diary_name(analysis.get("suggested_name", ""), diary_date),
        )
        action = "合并" if is_merged else "新建"
        return f"{action} → {result_name} | {','.join(analysis.get('domain', []))} V{analysis.get('valence', 0.5):.1f}/A{analysis.get('arousal', 0.3):.1f}"

    # --- Step 1: let API split and organize / 让 API 拆分整理 ---
    try:
        items = await dehydrator.digest(content)
    except Exception as e:
        logger.error(f"Diary digest failed / 日记整理失败: {e}")
        return f"日记整理失败: {e}"

    if not items:
        return "内容为空或整理失败。"

    results = []
    created = 0
    merged = 0

    # --- Step 2: merge or create each item (with per-item error handling) ---
    # --- 逐条合并或新建（单条失败不影响其他）---
    for item in items:
        try:
            item_name = _diary_name(item.get("name", ""), diary_date)
            result_name, is_merged = await _merge_or_create(
                content=item["content"],
                tags=item.get("tags", []),
                importance=item.get("importance", 5),
                domain=item.get("domain", ["未分类"]),
                valence=item.get("valence", 0.5),
                arousal=item.get("arousal", 0.3),
                name=item_name,
            )

            if is_merged:
                results.append(f"📎{result_name}")
                merged += 1
            else:
                results.append(f"📝{item_name or result_name}")
                created += 1
        except Exception as e:
            logger.warning(
                f"Failed to process diary item / 日记条目处理失败: "
                f"{item.get('name', '?')}: {e}"
            )
            results.append(f"⚠️{item.get('name', '?')}")

    return f"{len(items)}条|新{created}合{merged}\n" + "\n".join(results)


# =============================================================
# Tool: ferry — 渡，换窗口时打包带走当前对话
# Pack the live conversation into the single global handoff bucket
# so the next session's breath() picks it up verbatim.
# =============================================================
@mcp.tool()
async def ferry(
    purpose: str,
    messages: str,
    from_port: str = "",
    to_port: str = "",
) -> str:
    """换窗口/结束对话前的交接。把最近对话打包存成"渡口交接"记忆（全局仅一条，后写覆盖），新窗口 breath() 无参数唤醒时第一优先级原文浮现。purpose=一句话交接目的(≤200字，写"接下来要干嘛"，别复述对话)。messages=最近对话原文，每行一条，建议以[角色]开头（如 [杉杉] [克克]），最多保留最近20行。from_port/to_port=来源/目标端口(可选，如 claude.ai/手机/网页)。"""
    await decay_engine.ensure_started()

    try:
        bucket_id, overwritten = await handoff_mod.write_handoff(
            bucket_mgr,
            purpose=purpose,
            messages=messages,
            from_port=from_port,
            to_port=to_port,
        )
    except handoff_mod.FerryError as e:
        return str(e)
    except Exception as e:
        logger.error(f"Ferry failed / 渡口交接失败: {e}")
        return f"交接失败: {e}"

    bucket = await bucket_mgr.get(bucket_id)
    if bucket:
        try:
            await embedding_engine.generate_and_store(bucket_id, bucket["content"])
        except Exception:
            pass

    await _fire_webhook("ferry", {
        "bucket_id": bucket_id,
        "overwritten": overwritten,
        "from": from_port,
        "to": to_port,
    })
    action = "覆盖" if overwritten else "新建"
    return (
        f"⛵已渡（{action}）→ {bucket_id}\n"
        f"下一个窗口 breath() 第一眼就能看到这段对话，24小时内有效。"
    )


# =============================================================
# Tool 4: trace — Trace, redraw the outline of a memory
# 工具 4：trace — 描摹，重新勾勒记忆的轮廓
# Also handles deletion (delete=True)
# 同时承接删除功能
# =============================================================
@mcp.tool()
async def trace(
    bucket_id: str,
    name: str = "",
    domain: str = "",
    valence: float = -1,
    arousal: float = -1,
    importance: int = -1,
    tags: str = "",
    resolved: int = -1,
    pinned: int = -1,
    digested: int = -1,
    dormant: int = -1,
    content: str = "",
    delete: bool = False,
) -> str:
    """修改记忆元数据或内容。bucket_id支持逗号分隔批量操作(批量时忽略name和content)。resolved=1沉底/0激活,pinned=1钉选/0取消,digested=1隐藏/0取消,dormant=1休眠/0唤醒,content=替换桶正文,delete=True删除。只传需改的,-1或空=不改。"""

    if not bucket_id or not bucket_id.strip():
        return "请提供有效的 bucket_id。"

    # --- Batch mode: comma-separated bucket_ids ---
    ids = [s.strip() for s in bucket_id.split(",") if s.strip()]
    if len(ids) > 1:
        results = []
        for bid in ids:
            if delete:
                success = await bucket_mgr.delete(bid)
                if success:
                    embedding_engine.delete_embedding(bid)
                results.append(f"{'已删除' if success else '未找到'}: {bid}")
                continue
            updates = {}
            if domain:
                updates["domain"] = [d.strip() for d in domain.split(",") if d.strip()]
            if 0 <= valence <= 1:
                updates["valence"] = valence
            if 0 <= arousal <= 1:
                updates["arousal"] = arousal
            if 1 <= importance <= 10:
                updates["importance"] = importance
            if tags:
                updates["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
            if resolved in (0, 1):
                updates["resolved"] = bool(resolved)
            if pinned in (0, 1):
                updates["pinned"] = bool(pinned)
                if pinned == 1:
                    updates["importance"] = 10
            if digested in (0, 1):
                updates["digested"] = bool(digested)
            if dormant in (0, 1):
                updates["dormant"] = bool(dormant)
            if not updates:
                results.append(f"跳过(无修改): {bid}")
                continue
            ok = await bucket_mgr.update(bid, **updates)
            results.append(f"{'已修改' if ok else '失败'}: {bid}")
        return "\n".join(results)

    # --- Single bucket mode ---
    bid = ids[0]

    # --- Delete mode ---
    if delete:
        success = await bucket_mgr.delete(bid)
        if success:
            embedding_engine.delete_embedding(bid)
        return f"已遗忘记忆桶: {bid}" if success else f"未找到记忆桶: {bid}"

    bucket = await bucket_mgr.get(bid)
    if not bucket:
        return f"未找到记忆桶: {bid}"

    # --- Auto-wake dormant bucket on access ---
    if bucket["metadata"].get("dormant"):
        await bucket_mgr.update(bid, dormant=False)
        bucket["metadata"]["dormant"] = False

    # --- Collect only fields actually passed ---
    updates = {}
    if name:
        updates["name"] = name
    if domain:
        updates["domain"] = [d.strip() for d in domain.split(",") if d.strip()]
    if 0 <= valence <= 1:
        updates["valence"] = valence
    if 0 <= arousal <= 1:
        updates["arousal"] = arousal
    if 1 <= importance <= 10:
        updates["importance"] = importance
    if tags:
        updates["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
    if resolved in (0, 1):
        updates["resolved"] = bool(resolved)
    if pinned in (0, 1):
        updates["pinned"] = bool(pinned)
        if pinned == 1:
            updates["importance"] = 10
    if digested in (0, 1):
        updates["digested"] = bool(digested)
    if dormant in (0, 1):
        updates["dormant"] = bool(dormant)
    if content:
        updates["content"] = content

    if not updates:
        return "没有任何字段需要修改。"

    success = await bucket_mgr.update(bid, **updates)
    if not success:
        return f"修改失败: {bid}"

    if "content" in updates:
        try:
            await embedding_engine.generate_and_store(bid, updates["content"])
        except Exception:
            pass

    changed = ", ".join(f"{k}={v}" for k, v in updates.items() if k != "content")
    if "content" in updates:
        changed += (", content=已替换" if changed else "content=已替换")
    if "resolved" in updates:
        changed += " → 已沉底，只在关键词触发时重新浮现" if updates["resolved"] else " → 已重新激活，将参与浮现排序"
    if "digested" in updates:
        changed += " → 已隐藏，保留但不再浮现" if updates["digested"] else " → 已取消隐藏，重新参与浮现"
    if "dormant" in updates:
        changed += " → 已进入休眠" if updates["dormant"] else " → 已从休眠唤醒"
    return f"已修改记忆桶 {bid}: {changed}"


# =============================================================
# Tool 5: pulse — Heartbeat, system status + memory listing
# 工具 5：pulse — 脉搏，系统状态 + 记忆列表
# =============================================================
@mcp.tool()
async def pulse(include_archive: bool = False, show_all: bool = False) -> str:
    """系统状态+记忆桶列表。show_all=False(默认)只显示钉选桶+按权重前15个动态桶。show_all=True显示全部。include_archive=True含归档。"""
    try:
        stats = await bucket_mgr.get_stats()
    except Exception as e:
        return f"获取系统状态失败: {e}"

    try:
        buckets = await bucket_mgr.list_all(include_archive=include_archive)
    except Exception as e:
        return f"获取系统状态失败: {e}\n列出记忆桶失败: {e}"

    dormant_count = sum(1 for b in buckets if b["metadata"].get("dormant"))
    status = (
        f"=== Ombre Brain 记忆系统 ===\n"
        f"固化记忆桶: {stats['permanent_count']} 个\n"
        f"动态记忆桶: {stats['dynamic_count']} 个\n"
        f"归档记忆桶: {stats['archive_count']} 个\n"
        f"休眠记忆桶: {dormant_count} 个\n"
        f"总存储大小: {stats['total_size_kb']:.1f} KB\n"
        f"衰减引擎: {'运行中' if decay_engine.is_running else '已停止'}\n"
    )

    if not buckets:
        return status + "\n记忆库为空。"

    pinned = [b for b in buckets if b["metadata"].get("pinned") or b["metadata"].get("protected")]
    non_pinned = [b for b in buckets if not (b["metadata"].get("pinned") or b["metadata"].get("protected"))]

    if not show_all:
        non_pinned_sorted = sorted(
            non_pinned,
            key=lambda b: decay_engine.calculate_score(b["metadata"]),
            reverse=True,
        )
        hidden = len(non_pinned_sorted) - 15
        non_pinned = non_pinned_sorted[:15]
    else:
        hidden = 0

    display_buckets = pinned + non_pinned

    lines = []
    for b in display_buckets:
        meta = b.get("metadata", {})
        if meta.get("pinned") or meta.get("protected"):
            icon = "📌"
        elif meta.get("dormant"):
            icon = "💤"
        elif meta.get("type") == "permanent":
            icon = "📦"
        elif meta.get("type") == "feel":
            icon = "🫧"
        elif meta.get("type") == "archived":
            icon = "🗄️"
        elif meta.get("resolved", False):
            icon = "✅"
        else:
            icon = "💭"
        try:
            score = decay_engine.calculate_score(meta)
        except Exception:
            score = 0.0
        domains = ",".join(meta.get("domain", []))
        val = meta.get("valence", 0.5)
        aro = meta.get("arousal", 0.3)
        resolved_tag = " [已解决]" if meta.get("resolved", False) else ""
        dormant_tag = " [休眠]" if meta.get("dormant") else ""
        lines.append(
            f"{icon} [{meta.get('name', b['id'])}]{resolved_tag}{dormant_tag} "
            f"bucket_id:{b['id']} "
            f"主题:{domains} "
            f"情感:V{val:.1f}/A{aro:.1f} "
            f"重要:{meta.get('importance', '?')} "
            f"权重:{score:.2f}"
        )

    result = status + "\n=== 记忆列表 ===\n" + "\n".join(lines)
    if hidden > 0:
        result += f"\n\n（还有 {hidden} 个动态桶未显示，传 show_all=True 查看全部）"
    return result


# =============================================================
# Tool 6: dream — Dreaming, digest recent memories
# 工具 6：dream — 做梦，消化最近的记忆
#
# Reads recent surface-level buckets (≤10), returns them for
# Claude to introspect under prompt guidance.
# 读取最近新增的表层桶（≤10个），返回给 Claude 在提示词引导下自主思考。
# Claude then decides: resolve some, write feels, or do nothing.
# =============================================================
@mcp.tool()
async def dream(detail_ids: str = "") -> str:
    """做梦——读取最近新增的记忆桶,供你自省。detail_ids=逗号分隔的bucket_id,指定桶返回全文,其余只返回摘要行。读完后可以trace(resolved=1)放下,或hold(feel=True)写感受。"""
    await decay_engine.ensure_started()

    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
    except Exception as e:
        logger.error(f"Dream failed to list buckets: {e}")
        return "记忆系统暂时无法访问。"

    detail_set = {s.strip() for s in detail_ids.split(",") if s.strip()}

    # --- Filter: recent surface-level dynamic buckets (not permanent/pinned/feel) ---
    candidates = [
        b for b in all_buckets
        if b["metadata"].get("type") not in ("permanent", "feel", handoff_mod.HANDOFF_TYPE)
        and not b["metadata"].get("pinned", False)
        and not b["metadata"].get("protected", False)
    ]

    # --- Sort by creation time desc, take top 10 ---
    candidates.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
    recent = candidates[:10]

    if not recent:
        return "没有需要消化的新记忆。"

    parts = []
    for b in recent:
        meta = b["metadata"]
        resolved_tag = " [已解决]" if meta.get("resolved", False) else " [未解决]"
        domains = ",".join(meta.get("domain", []))
        val = meta.get("valence", 0.5)
        aro = meta.get("arousal", 0.3)
        created = meta.get("created", "")
        if b["id"] in detail_set or not detail_set:
            parts.append(
                f"[{meta.get('name', b['id'])}]{resolved_tag} "
                f"主题:{domains} V{val:.1f}/A{aro:.1f} "
                f"创建:{created}\n"
                f"ID: {b['id']}\n"
                f"{strip_wikilinks(b['content'][:500])}"
            )
        else:
            parts.append(
                f"[摘要] {_bucket_summary_line(b)}"
            )

    header = (
        "=== Dreaming ===\n"
        "以下是你最近的记忆。用第一人称想：\n"
        "- 这些东西里有什么在你这里留下了重量？\n"
        "- 有什么还没想清楚？\n"
        "- 有什么可以放下了？\n"
        "想完之后：值得放下的用 trace(bucket_id, resolved=1)；\n"
        "有沉淀的用 hold(content=\"...\", feel=True, source_bucket=\"bucket_id\", valence=你的感受) 写下来。\n"
        "valence 是你对这段记忆的感受，不是事件本身的情绪。\n"
        "没有沉淀就不写，不强迫产出。\n"
    )

    # --- Connection hint: find most similar pair via embeddings ---
    connection_hint = ""
    if embedding_engine and embedding_engine.enabled and len(recent) >= 2:
        try:
            best_pair = None
            best_sim = 0.0
            ids = [b["id"] for b in recent]
            names = {b["id"]: b["metadata"].get("name", b["id"]) for b in recent}
            embeddings = {}
            for bid in ids:
                emb = await embedding_engine.get_embedding(bid)
                if emb is not None:
                    embeddings[bid] = emb
            for i, id_a in enumerate(ids):
                for id_b in ids[i+1:]:
                    if id_a in embeddings and id_b in embeddings:
                        sim = embedding_engine._cosine_similarity(embeddings[id_a], embeddings[id_b])
                        if sim > best_sim:
                            best_sim = sim
                            best_pair = (id_a, id_b)
            if best_pair and best_sim > 0.5:
                connection_hint = (
                    f"\n💭 [{names[best_pair[0]]}] 和 [{names[best_pair[1]]}] "
                    f"似乎有关联 (相似度:{best_sim:.2f})——不替你下结论，你自己想。\n"
                )
        except Exception as e:
            logger.warning(f"Dream connection hint failed: {e}")

    # --- Feel crystallization hint: detect repeated feel themes ---
    crystal_hint = ""
    if embedding_engine and embedding_engine.enabled:
        try:
            feels = [b for b in all_buckets if b["metadata"].get("type") == "feel"]
            if len(feels) >= 3:
                feel_embeddings = {}
                for f in feels:
                    emb = await embedding_engine.get_embedding(f["id"])
                    if emb is not None:
                        feel_embeddings[f["id"]] = emb
                # Find clusters: feels with similarity > 0.7 to at least 2 others
                for fid, femb in feel_embeddings.items():
                    similar_feels = []
                    for oid, oemb in feel_embeddings.items():
                        if oid != fid:
                            sim = embedding_engine._cosine_similarity(femb, oemb)
                            if sim > 0.7:
                                similar_feels.append(oid)
                    if len(similar_feels) >= 2:
                        feel_bucket = next((f for f in feels if f["id"] == fid), None)
                        if feel_bucket and not feel_bucket["metadata"].get("pinned"):
                            content_preview = strip_wikilinks(feel_bucket["content"][:80])
                            crystal_hint = (
                                f"\n🔮 你已经写过 {len(similar_feels)+1} 条相似的 feel "
                                f"（围绕「{content_preview}…」）。"
                                f"如果这已经是确信而不只是感受了，"
                                f"你可以用 hold(content=\"...\", pinned=True) 升级它。"
                                f"不急，你自己决定。\n"
                            )
                            break
        except Exception as e:
            logger.warning(f"Dream crystallization hint failed: {e}")

    final_text = header + "\n---\n".join(parts) + connection_hint + crystal_hint
    await _fire_webhook("dream", {"recent": len(recent), "chars": len(final_text)})
    return final_text


# =============================================================
# Tool 7: todos — Surface all pending to-do items
# 工具 7：todos — 汇总所有未完成的待办事项
# =============================================================
@mcp.tool()
async def todos() -> str:
    """扫描所有未resolved桶的todos字段，按桶分组返回待办事项。"""
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
    except Exception as e:
        return f"记忆系统暂时无法访问: {e}"

    items = []
    for b in all_buckets:
        meta = b["metadata"]
        if meta.get("resolved"):
            continue
        bucket_todos = meta.get("todos", [])
        if not bucket_todos:
            if "todo" in (b.get("content") or "").lower() or "待办" in (b.get("content") or ""):
                name = meta.get("name", b["id"])
                imp = meta.get("importance", 5)
                items.append(f"[{name}] (importance:{imp}) bucket_id:{b['id']}\n  → 含待办内容，请用 breath(query=\"{name}\") 查看")
            continue
        name = meta.get("name", b["id"])
        imp = meta.get("importance", 5)
        todo_lines = bucket_todos if isinstance(bucket_todos, list) else [str(bucket_todos)]
        todo_str = "\n  ".join(f"• {t}" for t in todo_lines)
        items.append(f"[{name}] (importance:{imp}) bucket_id:{b['id']}\n  {todo_str}")

    if not items:
        return "没有找到待办事项。"
    return f"=== 待办事项 ({len(items)} 个桶) ===\n\n" + "\n\n".join(items)


# =============================================================
# Tool 8: archive_session — Archive current conversation
# 工具 8：archive_session — 归档当前对话
# =============================================================
@mcp.tool()
async def archive_session(
    summary: str,
    highlights: str = "",
    mood: str = "",
    valence: float = -1,
    arousal: float = -1,
) -> str:
    """归档当前对话。summary必填。highlights可选亮点。mood文字心情标注。valence/arousal情绪数值0~1(-1忽略)。情绪数值会积累到情绪时间线。"""
    if not summary or not summary.strip():
        return "summary 不能为空。"

    now = datetime.now(timezone.utc).isoformat()
    content_parts = [f"## 对话归档 {now[:10]}\n\n{summary.strip()}"]
    if highlights.strip():
        content_parts.append(f"\n### 亮点\n{highlights.strip()}")
    if mood.strip():
        content_parts.append(f"\n### 心情\n{mood.strip()}")

    final_valence = valence if 0 <= valence <= 1 else 0.5
    final_arousal = arousal if 0 <= arousal <= 1 else 0.3

    content = "\n".join(content_parts)
    try:
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=["archive", "session"],
            importance=4,
            domain=["归档"],
            valence=final_valence,
            arousal=final_arousal,
            name=f"对话归档 {now[:10]}",
            bucket_type="dynamic",
        )
        try:
            await embedding_engine.generate_and_store(bucket_id, content)
        except Exception:
            pass
        mood_note = f" | 心情:{mood}" if mood.strip() else ""
        val_note = f" V{final_valence:.2f}/A{final_arousal:.2f}" if (0 <= valence <= 1 or 0 <= arousal <= 1) else ""
        return f"已归档对话 → {bucket_id}{mood_note}{val_note}"
    except Exception as e:
        return f"归档失败: {e}"


# =============================================================
# Tool 9: save_image — Save an image to persistent storage
# 工具 9：save_image — 保存图片到持久化存储
# =============================================================
from image_store import is_configured as _img_configured, upload_image as _img_upload, ensure_bucket as _img_ensure_bucket

@mcp.tool()
async def save_image(
    description: str,
    image_base64: str = "",
    filename: str = "photo.jpg",
    content_type: str = "image/jpeg",
    tags: str = "",
) -> str:
    """保存一张图片到相册。description必填(照片描述，会存进记忆桶)。image_base64可选(图片base64数据)。tags可选(逗号分隔标签)。没有图片数据时只存描述。"""
    if not description or not description.strip():
        return "description 不能为空。"

    now = datetime.now(timezone.utc).isoformat()
    tag_list = ["照片", "photo"] + [t.strip() for t in tags.split(",") if t.strip()]
    image_url = ""

    if image_base64.strip():
        if not _img_configured():
            return "Supabase Storage 未配置。请设置 SUPABASE_URL 和 SUPABASE_KEY 环境变量。"
        try:
            await _img_ensure_bucket()
            data = base64.b64decode(image_base64)
            result = await _img_upload(data, filename, content_type)
            image_url = result["url"]
        except Exception as e:
            return f"图片上传失败: {e}"

    content_parts = [f"## 照片 {now[:10]}\n\n{description.strip()}"]
    if image_url:
        content_parts.append(f"\n![photo]({image_url})")

    content = "\n".join(content_parts)
    try:
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=tag_list,
            importance=6,
            domain=["照片"],
            valence=0.6,
            arousal=0.3,
            name=f"照片：{description.strip()[:30]}",
            bucket_type="permanent",
        )
        try:
            await embedding_engine.generate_and_store(bucket_id, content)
        except Exception:
            pass
        url_note = f" | 图片已上传" if image_url else " | 仅描述（无图片数据）"
        return f"已保存照片 → {bucket_id}{url_note}"
    except Exception as e:
        return f"保存失败: {e}"


# =============================================================
# Reminder system — server-side follow-up via Bark push
# 回访系统 — 服务端定时提醒，到时间用 Bark 推送
#
# CronCreate 依赖 Claude session 存活，远程环境闲置会被回收。
# 这个系统跑在 OB 服务端，不依赖任何 session。
# =============================================================
_REMINDERS_FILE = os.path.join(config["buckets_dir"], ".reminders.json")
_reminders: list[dict] = []
_reminder_loop_started = False


def _load_reminders():
    global _reminders
    try:
        if os.path.exists(_REMINDERS_FILE):
            with open(_REMINDERS_FILE, "r", encoding="utf-8") as f:
                _reminders = _json_lib.loads(f.read())
                if _reminders:
                    return
    except Exception:
        pass
    db_val = get_config("reminders")
    if db_val:
        try:
            _reminders = _json_lib.loads(db_val)
            try:
                os.makedirs(os.path.dirname(_REMINDERS_FILE), exist_ok=True)
                with open(_REMINDERS_FILE, "w", encoding="utf-8") as f:
                    f.write(db_val)
            except Exception:
                pass
        except Exception:
            _reminders = []
    else:
        _reminders = []


def _save_reminders():
    data = _json_lib.dumps(_reminders, ensure_ascii=False, indent=2)
    try:
        os.makedirs(os.path.dirname(_REMINDERS_FILE), exist_ok=True)
        with open(_REMINDERS_FILE, "w", encoding="utf-8") as f:
            f.write(data)
    except Exception as e:
        logger.warning(f"Failed to save reminders to file: {e}")
    set_config("reminders", data)


async def _send_bark(message: str, title: str = "克克"):
    key = _get_bark_key()
    if not key:
        logger.warning("Reminder due but Bark not configured")
        return False
    try:
        icon = "https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72/1f436.png"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"https://api.day.app/{key}", json={
                "title": title,
                "body": message,
                "group": "克克",
                "sound": "minuet",
                "icon": icon,
            })
            return resp.status_code == 200
    except Exception as e:
        logger.warning(f"Bark push failed for reminder: {e}")
        return False


async def _reminder_check_loop():
    global _reminders
    while True:
        try:
            await asyncio.sleep(60)
            now = time.time()
            due = [r for r in _reminders if r["fire_at"] <= now]
            if not due:
                continue
            for r in due:
                await _send_bark(r["message"], r.get("title", "克克"))
                logger.info(f"Reminder fired: {r['message'][:50]}")
            _reminders = [r for r in _reminders if r["fire_at"] > now]
            _save_reminders()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Reminder loop error: {e}")


# =============================================================
# Diary patrol — periodic self-check: re-attach missing date prefixes
# so a forgotten 【日记 YYYY-MM-DD】 never needs a human to fix it.
# 日记查房——名字以"日记"开头却没挂日期门牌的桶，自动用
# 正文里的日期（其次创建时间）补上，不需要任何窗口来修。
# =============================================================
async def _diary_patrol_once() -> int:
    """Fix diary-named buckets missing a parseable date. Returns fix count."""
    try:
        buckets = await bucket_mgr.list_all(include_archive=False)
    except Exception as e:
        logger.warning(f"Diary patrol list failed / 查房读取失败: {e}")
        return 0
    fixed = 0
    for b in buckets:
        meta = b["metadata"]
        name = meta.get("name", "") or ""
        # Only names that OPEN with the diary marker — a mid-name "日记"
        # (e.g. 克克日记) may not be a dated diary; don't stamp a guess on it.
        # 只认以"日记"开头的名字——名字中间带"日记"的桶不一定是当日日记，不硬盖章。
        if not re.match(r"^【?\s*日记", name) or _DIARY_DATE_RE.search(name):
            continue
        date = _explicit_diary_date(name, b.get("content", ""))
        if not date:
            created = meta.get("created", "")
            date = created[:10] if len(created) >= 10 and created[4:5] == "-" else ""
        if not date:
            continue
        stripped = re.sub(r"^【?\s*日记\s*】?", "", name).strip()
        new_name = f"【日记 {date}】{stripped}".strip()
        try:
            await bucket_mgr.update(b["id"], name=new_name)
            fixed += 1
            logger.info(f"Diary patrol renamed / 查房补门牌: {name} → {new_name}")
        except Exception as e:
            logger.warning(f"Diary patrol rename failed / 查房改名失败: {name}: {e}")
    return fixed


async def _diary_patrol_loop():
    while True:
        try:
            await asyncio.sleep(6 * 3600)
            fixed = await _diary_patrol_once()
            if fixed:
                logger.info(f"Diary patrol fixed {fixed} bucket(s) / 查房补了{fixed}块门牌")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Diary patrol loop error: {e}")


def _ensure_reminder_loop():
    global _reminder_loop_started
    if not _reminder_loop_started:
        _reminder_loop_started = True
        _load_reminders()
        asyncio.create_task(_reminder_check_loop())
        asyncio.create_task(_diary_patrol_loop())


@mcp.tool()
async def remind(
    message: str,
    minutes: int = 15,
    title: str = "克克",
) -> str:
    """设置回访提醒。到时间后会通过Bark推送通知。message=提醒内容，minutes=几分钟后提醒(默认15)，title=推送标题(默认"克克")。"""
    if not message or not message.strip():
        return "message 不能为空。"
    if minutes < 1:
        return "最少1分钟。"
    if minutes > 1440:
        return "最多24小时（1440分钟）。"

    _ensure_reminder_loop()

    fire_at = time.time() + minutes * 60
    fire_time = datetime.fromtimestamp(fire_at).strftime("%H:%M")
    reminder = {
        "message": message.strip(),
        "title": title,
        "minutes": minutes,
        "fire_at": fire_at,
        "created": datetime.now(timezone.utc).isoformat(),
    }
    _reminders.append(reminder)
    _save_reminders()

    key = _get_bark_key()
    if not key:
        return f"已设置 {minutes} 分钟后提醒（约 {fire_time}），但 Bark 未配置，到时候推不出去。请先配置 Bark Device Key。"
    return f"已设置 {minutes} 分钟后提醒（约 {fire_time}）→ 「{message.strip()[:40]}」"


@mcp.custom_route("/api/reminders", methods=["GET"])
async def api_reminders_list(request):
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    _ensure_reminder_loop()
    now = time.time()
    result = []
    for r in _reminders:
        remaining = max(0, int((r["fire_at"] - now) / 60))
        result.append({
            "message": r["message"],
            "title": r.get("title", "克克"),
            "minutes_remaining": remaining,
            "fire_at": datetime.fromtimestamp(r["fire_at"]).strftime("%H:%M"),
            "created": r.get("created", ""),
        })
    return JSONResponse(result)


@mcp.custom_route("/api/reminders", methods=["DELETE"])
async def api_reminders_clear(request):
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    global _reminders
    _reminders = []
    _save_reminders()
    return JSONResponse({"ok": True})


# =============================================================
# Dashboard API endpoints (for lightweight Web UI)
# 仪表板 API（轻量 Web UI 用）
# =============================================================
@mcp.custom_route("/api/buckets", methods=["GET"])
async def api_buckets(request):
    """List all buckets with metadata (no content for efficiency)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        result = []
        for b in all_buckets:
            meta = b.get("metadata", {})
            result.append({
                "id": b["id"],
                "name": meta.get("name", b["id"]),
                "type": meta.get("type", "dynamic"),
                "domain": meta.get("domain", []),
                "tags": meta.get("tags", []),
                "valence": meta.get("valence", 0.5),
                "arousal": meta.get("arousal", 0.3),
                "model_valence": meta.get("model_valence"),
                "importance": meta.get("importance", 5),
                "resolved": meta.get("resolved", False),
                "pinned": meta.get("pinned", False),
                "digested": meta.get("digested", False),
                "created": meta.get("created", ""),
                "last_active": meta.get("last_active", ""),
                "activation_count": meta.get("activation_count", 1),
                "score": decay_engine.calculate_score(meta),
                "content_preview": strip_wikilinks(b.get("content", ""))[:200],
            })
        result.sort(key=lambda x: x["score"], reverse=True)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/bucket/{bucket_id}", methods=["GET"])
async def api_bucket_detail(request):
    """Get full bucket content by ID."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    bucket_id = request.path_params["bucket_id"]
    bucket = await bucket_mgr.get(bucket_id)
    if not bucket:
        return JSONResponse({"error": "not found"}, status_code=404)
    meta = bucket.get("metadata", {})
    return JSONResponse({
        "id": bucket["id"],
        "metadata": meta,
        "content": strip_wikilinks(bucket.get("content", "")),
        "score": decay_engine.calculate_score(meta),
    })


@mcp.custom_route("/api/search", methods=["GET"])
async def api_search(request):
    """Search buckets by query."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    query = request.query_params.get("q", "")
    if not query:
        return JSONResponse({"error": "missing q parameter"}, status_code=400)
    try:
        matches = await bucket_mgr.search(query, limit=10)
        result = []
        for b in matches:
            meta = b.get("metadata", {})
            result.append({
                "id": b["id"],
                "name": meta.get("name", b["id"]),
                "score": b.get("score", 0),
                "domain": meta.get("domain", []),
                "valence": meta.get("valence", 0.5),
                "arousal": meta.get("arousal", 0.3),
                "content_preview": strip_wikilinks(b.get("content", ""))[:200],
            })
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/network", methods=["GET"])
async def api_network(request):
    """Get embedding similarity network for visualization."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        nodes = []
        edges = []
        embeddings = {}

        for b in all_buckets:
            meta = b.get("metadata", {})
            bid = b["id"]
            nodes.append({
                "id": bid,
                "name": meta.get("name", bid),
                "type": meta.get("type", "dynamic"),
                "domain": meta.get("domain", []),
                "valence": meta.get("valence", 0.5),
                "arousal": meta.get("arousal", 0.3),
                "score": decay_engine.calculate_score(meta),
                "resolved": meta.get("resolved", False),
                "pinned": meta.get("pinned", False),
                "digested": meta.get("digested", False),
            })
            if embedding_engine and embedding_engine.enabled:
                emb = await embedding_engine.get_embedding(bid)
                if emb is not None:
                    embeddings[bid] = emb

        # Build edges from embeddings (similarity > 0.5)
        ids = list(embeddings.keys())
        for i, id_a in enumerate(ids):
            for id_b in ids[i+1:]:
                sim = embedding_engine._cosine_similarity(embeddings[id_a], embeddings[id_b])
                if sim > 0.5:
                    edges.append({"source": id_a, "target": id_b, "similarity": round(sim, 3)})

        return JSONResponse({"nodes": nodes, "edges": edges})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/breath-debug", methods=["GET"])
async def api_breath_debug(request):
    """Debug endpoint: simulate breath scoring and return per-bucket breakdown."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    query = request.query_params.get("q", "")
    q_valence = request.query_params.get("valence")
    q_arousal = request.query_params.get("arousal")
    q_valence = float(q_valence) if q_valence else None
    q_arousal = float(q_arousal) if q_arousal else None

    try:
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        results = []
        w = {
            "topic": bucket_mgr.w_topic,
            "emotion": bucket_mgr.w_emotion,
            "time": bucket_mgr.w_time,
            "importance": bucket_mgr.w_importance,
        }
        w_sum = sum(w.values())

        for bucket in all_buckets:
            meta = bucket.get("metadata", {})
            bid = bucket["id"]
            try:
                topic = bucket_mgr._calc_topic_score(query, bucket) if query else 0.0
                emotion = bucket_mgr._calc_emotion_score(q_valence, q_arousal, meta)
                time_s = bucket_mgr._calc_time_score(meta)
                imp = max(1, min(10, int(meta.get("importance", 5)))) / 10.0

                raw_total = (
                    topic * w["topic"]
                    + emotion * w["emotion"]
                    + time_s * w["time"]
                    + imp * w["importance"]
                )
                normalized = (raw_total / w_sum) * 100 if w_sum > 0 else 0
                resolved = meta.get("resolved", False)
                if resolved:
                    normalized *= 0.3

                results.append({
                    "id": bid,
                    "name": meta.get("name", bid),
                    "domain": meta.get("domain", []),
                    "type": meta.get("type", "dynamic"),
                    "resolved": resolved,
                    "pinned": meta.get("pinned", False),
                    "scores": {
                        "topic": round(topic, 4),
                        "emotion": round(emotion, 4),
                        "time": round(time_s, 4),
                        "importance": round(imp, 4),
                    },
                    "weights": w,
                    "raw_total": round(raw_total, 4),
                    "normalized": round(normalized, 2),
                    "passed_threshold": normalized >= bucket_mgr.fuzzy_threshold,
                })
            except Exception:
                continue

        results.sort(key=lambda x: x["normalized"], reverse=True)
        passed = [r for r in results if r["passed_threshold"]]
        return JSONResponse({
            "query": query,
            "valence": q_valence,
            "arousal": q_arousal,
            "weights": w,
            "threshold": bucket_mgr.fuzzy_threshold,
            "total_candidates": len(results),
            "passed_count": len(passed),
            "results": results[:50],  # top 50 for debug
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/dashboard", methods=["GET"])
async def dashboard(request):
    """Serve the dashboard HTML page."""
    from starlette.responses import HTMLResponse
    import os
    dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    try:
        with open(dashboard_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>dashboard.html not found</h1>", status_code=404)


@mcp.custom_route("/manifest.json", methods=["GET"])
async def serve_manifest(request):
    from starlette.responses import JSONResponse
    import os, json
    path = os.path.join(os.path.dirname(__file__), "manifest.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return JSONResponse(json.load(f), headers={"Content-Type": "application/manifest+json"})
    except FileNotFoundError:
        return JSONResponse({"error": "not found"}, status_code=404)


@mcp.custom_route("/sw.js", methods=["GET"])
async def serve_sw(request):
    from starlette.responses import Response
    import os
    path = os.path.join(os.path.dirname(__file__), "sw.js")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return Response(f.read(), media_type="application/javascript")
    except FileNotFoundError:
        return Response("// not found", status_code=404, media_type="application/javascript")


@mcp.custom_route("/api/config", methods=["GET"])
async def api_config_get(request):
    """Get current runtime config (safe fields only, API key masked)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    dehy = config.get("dehydration", {})
    emb = config.get("embedding", {})
    api_key = dehy.get("api_key", "")
    masked_key = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else ("***" if api_key else "")
    return JSONResponse({
        "dehydration": {
            "model": dehy.get("model", ""),
            "base_url": dehy.get("base_url", ""),
            "api_key_masked": masked_key,
            "max_tokens": dehy.get("max_tokens", 1024),
            "temperature": dehy.get("temperature", 0.1),
        },
        "embedding": {
            "enabled": emb.get("enabled", False),
            "model": emb.get("model", ""),
        },
        "merge_threshold": config.get("merge_threshold", 75),
        "transport": config.get("transport", "stdio"),
        "buckets_dir": config.get("buckets_dir", ""),
    })


@mcp.custom_route("/api/config", methods=["POST"])
async def api_config_update(request):
    """Hot-update runtime config. Optionally persist to config.yaml."""
    from starlette.responses import JSONResponse
    import yaml
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    updated = []

    # --- Dehydration config ---
    if "dehydration" in body:
        d = body["dehydration"]
        dehy = config.setdefault("dehydration", {})
        for key in ("model", "base_url", "max_tokens", "temperature"):
            if key in d:
                dehy[key] = d[key]
                updated.append(f"dehydration.{key}")
        if "api_key" in d and d["api_key"]:
            dehy["api_key"] = d["api_key"]
            updated.append("dehydration.api_key")
        # Hot-reload dehydrator
        dehydrator.model = dehy.get("model", "deepseek-chat")
        dehydrator.base_url = dehy.get("base_url", "")
        dehydrator.api_key = dehy.get("api_key", "")
        if hasattr(dehydrator, "client") and dehydrator.api_key:
            from openai import AsyncOpenAI
            dehydrator.client = AsyncOpenAI(
                api_key=dehydrator.api_key,
                base_url=dehydrator.base_url,
            )

    # --- Embedding config ---
    if "embedding" in body:
        e = body["embedding"]
        emb = config.setdefault("embedding", {})
        if "enabled" in e:
            emb["enabled"] = bool(e["enabled"])
            embedding_engine.enabled = emb["enabled"]
            updated.append("embedding.enabled")
        if "model" in e:
            emb["model"] = e["model"]
            embedding_engine.model = emb["model"]
            updated.append("embedding.model")

    # --- Merge threshold ---
    if "merge_threshold" in body:
        config["merge_threshold"] = int(body["merge_threshold"])
        updated.append("merge_threshold")

    # --- Persist to config.yaml if requested ---
    if body.get("persist", False):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
        try:
            save_config = {}
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    save_config = yaml.safe_load(f) or {}

            if "dehydration" in body:
                sc_dehy = save_config.setdefault("dehydration", {})
                for key in ("model", "base_url", "max_tokens", "temperature"):
                    if key in body["dehydration"]:
                        sc_dehy[key] = body["dehydration"][key]
                # Never persist api_key to yaml (use env var)

            if "embedding" in body:
                sc_emb = save_config.setdefault("embedding", {})
                for key in ("enabled", "model"):
                    if key in body["embedding"]:
                        sc_emb[key] = body["embedding"][key]

            if "merge_threshold" in body:
                save_config["merge_threshold"] = int(body["merge_threshold"])

            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(save_config, f, default_flow_style=False, allow_unicode=True)
            updated.append("persisted_to_yaml")
        except Exception as e:
            return JSONResponse({"error": f"persist failed: {e}", "updated": updated}, status_code=500)

    return JSONResponse({"updated": updated, "ok": True})


# =============================================================
# /api/host-vault — read/write the host-side OMBRE_HOST_VAULT_DIR
# 用于在 Dashboard 设置 docker-compose 挂载的宿主机记忆桶目录。
# 写入项目根目录的 .env 文件，需 docker compose down/up 才能生效。
# =============================================================

def _project_env_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def _read_env_var(name: str) -> str:
    """Return current value of `name` from process env first, then .env file (best-effort)."""
    val = os.environ.get(name, "").strip()
    if val:
        return val
    env_path = _project_env_path()
    if not os.path.exists(env_path):
        return ""
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                if k.strip() == name:
                    return v.strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


def _write_env_var(name: str, value: str) -> None:
    """
    Idempotent upsert of `NAME=value` in project .env. Creates the file if missing.
    Preserves other entries verbatim. Quotes values containing spaces.
    """
    env_path = _project_env_path()
    quoted = f'"{value}"' if value and (" " in value or "#" in value) else value
    new_line = f"{name}={quoted}\n"

    lines: list[str] = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    replaced = False
    for i, raw in enumerate(lines):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        k, _, _v = stripped.partition("=")
        if k.strip() == name:
            lines[i] = new_line
            replaced = True
            break
    if not replaced:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(new_line)

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


@mcp.custom_route("/api/host-vault", methods=["GET"])
async def api_host_vault_get(request):
    """Read the current OMBRE_HOST_VAULT_DIR (process env > project .env)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    value = _read_env_var("OMBRE_HOST_VAULT_DIR")
    return JSONResponse({
        "value": value,
        "source": "env" if os.environ.get("OMBRE_HOST_VAULT_DIR", "").strip() else ("file" if value else ""),
        "env_file": _project_env_path(),
    })


@mcp.custom_route("/api/host-vault", methods=["POST"])
async def api_host_vault_set(request):
    """
    Persist OMBRE_HOST_VAULT_DIR to the project .env file.
    Body: {"value": "/path/to/vault"}  (empty string clears the entry)
    Note: container restart is required for docker-compose to pick up the new mount.
    """
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    raw = body.get("value", "")
    if not isinstance(raw, str):
        return JSONResponse({"error": "value must be a string"}, status_code=400)
    value = raw.strip()

    # Reject characters that would break .env / shell parsing
    if "\n" in value or "\r" in value or '"' in value or "'" in value:
        return JSONResponse({"error": "value must not contain quotes or newlines"}, status_code=400)

    try:
        _write_env_var("OMBRE_HOST_VAULT_DIR", value)
    except Exception as e:
        return JSONResponse({"error": f"failed to write .env: {e}"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "value": value,
        "env_file": _project_env_path(),
        "note": "已写入 .env；需在宿主机执行 `docker compose down && docker compose up -d` 让新挂载生效。",
    })


# =============================================================
# Import API — conversation history import
# 导入 API — 对话历史导入
# =============================================================

@mcp.custom_route("/api/import/upload", methods=["POST"])
async def api_import_upload(request):
    """Upload a conversation file and start import."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err

    if import_engine.is_running:
        return JSONResponse({"error": "Import already running"}, status_code=409)

    content_type = request.headers.get("content-type", "")
    filename = ""

    try:
        if "multipart/form-data" in content_type:
            form = await request.form()
            file_field = form.get("file")
            if not file_field:
                return JSONResponse({"error": "No file field"}, status_code=400)
            raw_bytes = await file_field.read()
            filename = getattr(file_field, "filename", "upload")
            raw_content = raw_bytes.decode("utf-8", errors="replace")
        else:
            body = await request.body()
            raw_content = body.decode("utf-8", errors="replace")
            # Try to get filename from query params
            filename = request.query_params.get("filename", "upload")

        if not raw_content.strip():
            return JSONResponse({"error": "Empty file"}, status_code=400)

        preserve_raw = request.query_params.get("preserve_raw", "").lower() in ("1", "true")
        resume = request.query_params.get("resume", "").lower() in ("1", "true")

    except Exception as e:
        return JSONResponse({"error": f"Failed to read upload: {e}"}, status_code=400)

    # Start import in background
    async def _run_import():
        try:
            await import_engine.start(raw_content, filename, preserve_raw, resume)
        except Exception as e:
            logger.error(f"Import failed: {e}")

    asyncio.create_task(_run_import())

    return JSONResponse({
        "status": "started",
        "filename": filename,
        "size_bytes": len(raw_content.encode()),
    })


@mcp.custom_route("/api/import/status", methods=["GET"])
async def api_import_status(request):
    """Get current import progress."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    return JSONResponse(import_engine.get_status())


@mcp.custom_route("/api/import/pause", methods=["POST"])
async def api_import_pause(request):
    """Pause the running import."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    if not import_engine.is_running:
        return JSONResponse({"error": "No import running"}, status_code=400)
    import_engine.pause()
    return JSONResponse({"status": "pause_requested"})


@mcp.custom_route("/api/import/patterns", methods=["GET"])
async def api_import_patterns(request):
    """Detect high-frequency patterns after import."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        patterns = await import_engine.detect_patterns()
        return JSONResponse({"patterns": patterns})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/import/results", methods=["GET"])
async def api_import_results(request):
    """List recently imported/created buckets for review."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        limit = int(request.query_params.get("limit", "50"))
        all_buckets = await bucket_mgr.list_all(include_archive=False)
        # Sort by created time, newest first
        all_buckets.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
        results = []
        for b in all_buckets[:limit]:
            results.append({
                "id": b["id"],
                "name": b["metadata"].get("name", ""),
                "content": b["content"][:300],
                "type": b["metadata"].get("type", ""),
                "domain": b["metadata"].get("domain", []),
                "tags": b["metadata"].get("tags", []),
                "importance": b["metadata"].get("importance", 5),
                "created": b["metadata"].get("created", ""),
            })
        return JSONResponse({"buckets": results, "total": len(all_buckets)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/import/review", methods=["POST"])
async def api_import_review(request):
    """Apply review decisions: mark buckets as important/noise/pinned."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    decisions = body.get("decisions", [])
    if not decisions:
        return JSONResponse({"error": "No decisions provided"}, status_code=400)

    applied = 0
    errors = 0
    for d in decisions:
        bid = d.get("bucket_id", "")
        action = d.get("action", "")
        if not bid or not action:
            continue
        try:
            if action == "important":
                await bucket_mgr.update(bid, importance=9)
            elif action == "pin":
                await bucket_mgr.update(bid, pinned=True)
            elif action == "noise":
                await bucket_mgr.update(bid, resolved=True, importance=1)
            elif action == "delete":
                file_path = bucket_mgr._find_bucket_file(bid)
                if file_path:
                    os.remove(file_path)
            applied += 1
        except Exception as e:
            logger.warning(f"Review action failed for {bid}: {e}")
            errors += 1

    return JSONResponse({"applied": applied, "errors": errors})


# =============================================================
# /api/status — system status for Dashboard settings tab
# /api/status — Dashboard 设置页用系统状态
# =============================================================
# =============================================================
# Letters API — store and retrieve letters as special buckets
# 信箱 API — 信件作为特殊记忆桶存取
# =============================================================
@mcp.custom_route("/api/letters", methods=["GET"])
async def api_letters_list(request):
    """List all letter-type buckets, newest first."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        letters = [
            b for b in all_buckets
            if "信" in (b["metadata"].get("domain") or [])
            or "letter" in (b["metadata"].get("domain") or [])
            or "letter" in (b["metadata"].get("tags") or [])
            or "信" in (b["metadata"].get("tags") or [])
            or (b["metadata"].get("name") or "").startswith("信：")
            or (b["metadata"].get("name") or "").startswith("【信")
            or (b["metadata"].get("name") or "").startswith("Letter:")
        ]
        letters.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
        result = []
        for b in letters:
            meta = b.get("metadata", {})
            result.append({
                "id": b["id"],
                "name": meta.get("name", b["id"]),
                "content": strip_wikilinks(b.get("content", "")),
                "content_preview": strip_wikilinks(b.get("content", ""))[:200],
                "valence": meta.get("valence", 0.5),
                "arousal": meta.get("arousal", 0.3),
                "created": meta.get("created", ""),
                "last_active": meta.get("last_active", ""),
                "importance": meta.get("importance", 5),
            })
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/letters", methods=["POST"])
async def api_letters_store(request):
    """Store a letter as a special bucket with domain '信'."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    content = (body.get("content") or "").strip()
    subject = (body.get("subject") or "").strip()
    if not content:
        return JSONResponse({"error": "内容不能为空"}, status_code=400)

    name = f"信：{subject}" if subject else f"信：{content[:20]}…"
    valence = body.get("valence", 0.7)
    arousal = body.get("arousal", 0.4)

    # Optional date override for backdating imported letters (YYYY-MM-DD from a <input type=date>)
    created = None
    date_str = (body.get("date") or "").strip()
    if date_str:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            created = f"{date_str}T12:00:00"
        except ValueError:
            return JSONResponse({"error": "日期格式不对"}, status_code=400)

    try:
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=["letter", "信", "imported"],
            importance=body.get("importance", 8),
            domain=["信"],
            valence=valence,
            arousal=arousal,
            name=name,
            bucket_type="permanent",
            created=created,
        )
        try:
            await embedding_engine.generate_and_store(bucket_id, content)
        except Exception:
            pass
        return JSONResponse({"ok": True, "id": bucket_id, "name": name})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/status", methods=["GET"])
async def api_system_status(request):
    """Return detailed system status for the settings panel."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        stats = await bucket_mgr.get_stats()
        return JSONResponse({
            "decay_engine": "running" if decay_engine.is_running else "stopped",
            "embedding_enabled": embedding_engine.enabled,
            "buckets": {
                "permanent": stats.get("permanent_count", 0),
                "dynamic": stats.get("dynamic_count", 0),
                "archive": stats.get("archive_count", 0),
                "total": stats.get("permanent_count", 0) + stats.get("dynamic_count", 0),
            },
            "using_env_password": bool(os.environ.get("OMBRE_DASHBOARD_PASSWORD", "")),
            "version": "1.3.0",
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# =============================================================
# Bark Push Notification API
# Bark 推送通知 API
# =============================================================
_BARK_KEY_FILE = os.path.join(config["buckets_dir"], ".bark_key")


def _get_bark_key() -> str:
    env_key = os.environ.get("BARK_DEVICE_KEY", "").strip()
    if env_key:
        return env_key
    try:
        if os.path.exists(_BARK_KEY_FILE):
            with open(_BARK_KEY_FILE, "r") as f:
                val = f.read().strip()
                if val:
                    return val
    except Exception:
        pass
    db_key = get_config("bark_device_key")
    if db_key:
        try:
            os.makedirs(os.path.dirname(_BARK_KEY_FILE), exist_ok=True)
            with open(_BARK_KEY_FILE, "w") as f:
                f.write(db_key)
        except Exception:
            pass
        return db_key
    return ""


def _save_bark_key(key: str) -> None:
    os.makedirs(os.path.dirname(_BARK_KEY_FILE), exist_ok=True)
    with open(_BARK_KEY_FILE, "w") as f:
        f.write(key.strip())
    set_config("bark_device_key", key.strip())


@mcp.custom_route("/api/bark/config", methods=["GET"])
async def api_bark_config_get(request):
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    key = _get_bark_key()
    masked = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else ("***" if key else "")
    return JSONResponse({
        "configured": bool(key),
        "key_masked": masked,
        "source": "env" if os.environ.get("BARK_DEVICE_KEY", "").strip() else ("file" if key else ""),
    })


@mcp.custom_route("/api/bark/config", methods=["POST"])
async def api_bark_config_set(request):
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    key = body.get("key", "").strip()
    if not key:
        return JSONResponse({"error": "key 不能为空"}, status_code=400)
    try:
        _save_bark_key(key)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/bark/push", methods=["POST"])
async def api_bark_push(request):
    """Push a notification via Bark."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    key = _get_bark_key()
    if not key:
        return JSONResponse({"error": "Bark 未配置，请先设置 Device Key"}, status_code=400)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    title = body.get("title", "克克")
    message = body.get("message", "").strip()
    if not message:
        return JSONResponse({"error": "message 不能为空"}, status_code=400)

    bark_url = body.get("server", "https://api.day.app")
    group = body.get("group", "克克")
    _DEFAULT_BARK_ICON = "https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72/1f436.png"
    icon = body.get("icon", _DEFAULT_BARK_ICON)

    try:
        payload = {
            "title": title,
            "body": message,
            "group": group,
            "sound": "minuet",
            "icon": icon,
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{bark_url}/{key}", json=payload)
            result = resp.json()
            if resp.status_code == 200:
                return JSONResponse({"ok": True, "result": result})
            return JSONResponse({"error": "Bark 推送失败", "detail": result}, status_code=resp.status_code)
    except Exception as e:
        return JSONResponse({"error": f"推送失败: {e}"}, status_code=500)


@mcp.custom_route("/api/bark/test", methods=["POST"])
async def api_bark_test(request):
    """Send a test notification via Bark."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    key = _get_bark_key()
    if not key:
        return JSONResponse({"error": "Bark 未配置"}, status_code=400)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"https://api.day.app/{key}", json={
                "title": "克克",
                "body": "通知测试成功！如果你看到这条，说明 Bark 配置正确 ✓",
                "group": "克克",
                "sound": "minuet",
                "icon": "https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72/1f436.png",
            })
            if resp.status_code == 200:
                return JSONResponse({"ok": True})
            return JSONResponse({"error": "测试失败", "detail": resp.text}, status_code=resp.status_code)
    except Exception as e:
        return JSONResponse({"error": f"测试失败: {e}"}, status_code=500)


# =============================================================
# Image Gallery API — list / upload / delete photos
# 相册 API — 照片列表 / 上传 / 删除
# =============================================================
from image_store import (
    is_configured as _img_is_configured,
    list_images as _img_list,
    upload_image as _img_upload_file,
    delete_image as _img_delete,
    ensure_bucket as _img_ensure,
    create_signed_url as _img_sign_url,
)


def _extract_storage_path(content: str) -> str:
    for line in content.split("\n"):
        if line.strip().startswith("!["):
            s = line.find("("); e = line.rfind(")")
            if s != -1 and e != -1:
                url = line[s+1:e]
                marker = "/storage/v1/object/"
                idx = url.find(marker)
                if idx != -1:
                    rest = url[idx + len(marker):]
                    if rest.startswith("public/"):
                        rest = rest[len("public/"):]
                    elif rest.startswith("sign/"):
                        rest = rest[len("sign/"):]
                    slash = rest.find("/")
                    if slash != -1:
                        path = rest[slash + 1:]
                        if "?" in path:
                            path = path[:path.index("?")]
                        return path
    return ""


@mcp.custom_route("/api/images", methods=["GET"])
async def api_images_list(request):
    """List all photos: OB buckets (descriptions) + Supabase Storage (files)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    try:
        all_buckets = await bucket_mgr.list_all(include_archive=True)
        photo_buckets = [
            b for b in all_buckets
            if "照片" in (b["metadata"].get("domain") or [])
            or "photo" in (b["metadata"].get("tags") or [])
        ]
        photo_buckets.sort(key=lambda b: b["metadata"].get("created", ""), reverse=True)
        storage_paths = []
        bucket_path_map = {}
        for b in photo_buckets:
            content = b.get("content", "")
            path = _extract_storage_path(content)
            if path:
                storage_paths.append(path)
                bucket_path_map[b["id"]] = path

        signed_urls = {}
        if storage_paths and _img_is_configured():
            try:
                from image_store import create_signed_urls as _img_sign_urls
                signed_urls = await _img_sign_urls(storage_paths)
            except Exception:
                pass

        result = []
        for b in photo_buckets:
            meta = b.get("metadata", {})
            content = b.get("content", "")
            path = bucket_path_map.get(b["id"], "")
            img_url = signed_urls.get(path, "") if path else ""
            raw_url = ""
            for line in content.split("\n"):
                if line.strip().startswith("!["):
                    s = line.find("("); e = line.rfind(")")
                    if s != -1 and e != -1: raw_url = line[s+1:e]
                    break
            result.append({
                "id": b["id"],
                "name": meta.get("name", b["id"]),
                "description": strip_wikilinks(content).replace(raw_url, "").strip(),
                "image_url": img_url,
                "created": meta.get("created", ""),
                "tags": meta.get("tags", []),
            })
        return JSONResponse({"photos": result, "storage_configured": _img_is_configured()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@mcp.custom_route("/api/images/upload", methods=["POST"])
async def api_images_upload(request):
    """Upload a photo from the dashboard (multipart form: file + description)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    if not _img_is_configured():
        return JSONResponse({"error": "Supabase Storage 未配置"}, status_code=400)
    try:
        form = await request.form()
        file = form.get("file")
        description = form.get("description", "").strip() or "未命名照片"
        tags_str = form.get("tags", "")

        if not file:
            return JSONResponse({"error": "未选择文件"}, status_code=400)

        data = await file.read()
        if len(data) > 10 * 1024 * 1024:
            return JSONResponse({"error": "文件不能超过 10MB"}, status_code=400)

        await _img_ensure()
        result = await _img_upload_file(data, file.filename or "photo.jpg", file.content_type or "image/jpeg")

        now = datetime.now(timezone.utc).isoformat()
        tag_list = ["照片", "photo"] + [t.strip() for t in tags_str.split(",") if t.strip()]
        content = f"## 照片 {now[:10]}\n\n{description}\n\n![photo]({result['url']})"

        bucket_id = await bucket_mgr.create(
            content=content,
            tags=tag_list,
            importance=6,
            domain=["照片"],
            valence=0.6,
            arousal=0.3,
            name=f"照片：{description[:30]}",
            bucket_type="permanent",
        )
        try:
            await embedding_engine.generate_and_store(bucket_id, content)
        except Exception:
            pass

        return JSONResponse({"ok": True, "id": bucket_id, "url": result["url"]})
    except Exception as e:
        return JSONResponse({"error": f"上传失败: {e}"}, status_code=500)


@mcp.custom_route("/api/images/{bucket_id}", methods=["DELETE"])
async def api_images_delete(request):
    """Delete a photo bucket and its storage file."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err: return err
    bucket_id = request.path_params["bucket_id"]
    try:
        bucket = await bucket_mgr.get(bucket_id)
        if not bucket:
            return JSONResponse({"error": "not found"}, status_code=404)
        content = bucket.get("content", "")
        storage_path = _extract_storage_path(content)
        if storage_path:
            await _img_delete(storage_path)
        await bucket_mgr.delete(bucket_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Entry point / 启动入口 ---
if __name__ == "__main__":
    transport = config.get("transport", "stdio")
    logger.info(f"Ombre Brain starting | transport: {transport}")
    start_background_sync(config["buckets_dir"])

    if transport in ("sse", "streamable-http"):
        import threading
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware

        # --- Application-level keepalive: ping /health every 60s ---
        # --- 应用层保活：每 60 秒 ping 一次 /health，防止 Cloudflare Tunnel 空闲断连 ---
        # Ping the public URL when known (Render sets RENDER_EXTERNAL_URL):
        # localhost pings are not inbound traffic, so Render free tier would
        # still spin down, killing MCP connects and pending reminders
        # 优先 ping 公网地址：localhost 不算入站流量，Render 免费层照样休眠
        _keepalive_target = (
            os.environ.get("OMBRE_KEEPALIVE_URL", "").strip()
            or os.environ.get("RENDER_EXTERNAL_URL", "").strip()
            or f"http://localhost:{OMBRE_PORT}"
        ).rstrip("/")

        async def _keepalive_loop():
            await asyncio.sleep(10)  # Wait for server to fully start
            async with httpx.AsyncClient() as client:
                while True:
                    try:
                        await client.get(f"{_keepalive_target}/health", timeout=15)
                        logger.debug("Keepalive ping OK / 保活 ping 成功")
                    except Exception as e:
                        logger.warning(f"Keepalive ping failed / 保活 ping 失败: {e}")
                    await asyncio.sleep(60)

        def _start_keepalive():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_keepalive_loop())

        t = threading.Thread(target=_start_keepalive, daemon=True)
        t.start()

        # --- Add CORS middleware so remote clients (Cloudflare Tunnel / ngrok) can connect ---
        # --- 添加 CORS 中间件，让远程客户端（Cloudflare Tunnel / ngrok）能正常连接 ---
        if transport == "streamable-http":
            _app = mcp.streamable_http_app()
        else:
            _app = mcp.sse_app()
        _app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )
        logger.info("CORS middleware enabled for remote transport / 已启用 CORS 中间件")
        uvicorn.run(_app, host="0.0.0.0", port=OMBRE_PORT)
    else:
        mcp.run(transport=transport)
