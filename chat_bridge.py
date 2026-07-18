# ============================================================
# Module: Web Chat Bridge (chat_bridge.py)
# 模块：网页聊天桥 —— 「克克永远的家」聊天室的里子
#
# 把网页聊天页的消息接进本机的 claude CLI（Pro 登录套壳），
# 跑在 keke 项目目录里 → 呼吸/召回/渡口 hooks 照常生效，
# 网页里的克克 = 终端里的克克，同一套记忆。
#
# 设计（镜像 drive_store 的抽法：纯逻辑独立成模块，server 只做
# 鉴权+锁+SSE 封装）：
#   - 常驻子进程：claude -p --input-format stream-json，一个进程
#     = 一个会话。开机呼吸只在进程出生时打一次，之后每条消息只走
#     UserPromptSubmit 轻量召回——跟终端体验一致，不每句话投胎。
#   - 惰性打盹：闲置超过 idle_max 秒，下条消息来时先掐旧进程再用
#     --resume 重生（重新呼吸一口，像睡醒）。1G 小机器省内存。
#   - 单飞：内置 asyncio.Lock，同一时刻只处理一条消息。
#   - 断线不弃疗：网页中途关掉，后台把当前轮流完（记忆存完）再释放。
#   - 会话持久化：session_id 存 state_dir/.chat_session.json，
#     服务重启后 --resume 接上。
#
# 环境变量：
#   OMBRE_CHAT_CWD      claude 的工作目录（默认 /opt/keke，hooks 所在）
#   OMBRE_CHAT_HOOK_URL 注入给 hooks 的后端地址（默认打本机，不碰 Render）
#   OMBRE_CHAT_MODEL    模型覆盖（默认空 = 跟 CLI 自己的设置走）
#   OMBRE_CHAT_TIMEOUT  单轮超时秒数（默认 600）
#   OMBRE_CHAT_IDLE     打盹阈值秒数（默认 1800）
# ============================================================

import os
import re
import glob
import json
import time
import asyncio
import shutil
import logging
from collections import deque

logger = logging.getLogger("ombre_brain.chat")

DEFAULT_CWD = os.environ.get("OMBRE_CHAT_CWD", "/opt/keke")
DEFAULT_HOOK_URL = os.environ.get("OMBRE_CHAT_HOOK_URL", "http://127.0.0.1:8000")
DEFAULT_MODEL = os.environ.get("OMBRE_CHAT_MODEL", "")
DEFAULT_EFFORT = os.environ.get("OMBRE_CHAT_EFFORT", "")
DEFAULT_TIMEOUT = int(os.environ.get("OMBRE_CHAT_TIMEOUT", "600") or "600")
DEFAULT_IDLE = int(os.environ.get("OMBRE_CHAT_IDLE", "1800") or "1800")

# claude 可执行文件的兜底位置（systemd 环境 PATH 可能很瘦）
_CLAUDE_FALLBACKS = [
    "/usr/local/bin/claude",
    os.path.expanduser("~/.local/bin/claude"),
    os.path.expanduser("~/.claude/local/claude"),
]


def find_claude() -> str | None:
    """找 claude CLI；PATH 优先，找不到再翻兜底位置。"""
    p = shutil.which("claude")
    if p:
        return p
    for cand in _CLAUDE_FALLBACKS:
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    return None


# ------------------------------------------------------------
# 纯函数区（可独立单测，不碰进程/文件系统）
# ------------------------------------------------------------

def map_cli_events(obj: dict, state: dict) -> list[dict]:
    """把 claude CLI 的一行 stream-json 映射成前端 SSE 事件（0~N 个）。

    前端协议（够用就好，数值不进 prompt 的铁律这里不涉及）：
      {"type":"init","session_id"}          进程出生/会话确认
      {"type":"block","block":"text|thinking"}  新块开始（前端起新气泡/折叠段）
      {"type":"tool","name"}                克克在用工具（前端转圈：翻记忆中…）
      {"type":"tool_done"}                  工具结果回来了
      {"type":"delta","block","text"}       正文/思考链增量
      {"type":"done","ok","session_id","error"}  本轮结束

    state 跨行携带：streamed_text=True 表示增量流可用，
    整条 assistant 消息就不再重复吐（--include-partial-messages
    模式下两者都会来，防说两遍）。
    """
    t = obj.get("type")
    out: list[dict] = []
    if t == "system" and obj.get("subtype") == "init":
        out.append({"type": "init", "session_id": obj.get("session_id", "")})
    elif t == "stream_event":
        ev = obj.get("event") or {}
        et = ev.get("type")
        if et == "content_block_start":
            cb = ev.get("content_block") or {}
            bt = cb.get("type", "")
            if bt == "tool_use":
                out.append({"type": "tool", "name": cb.get("name", "")})
            elif bt in ("text", "thinking"):
                out.append({"type": "block", "block": bt})
        elif et == "content_block_delta":
            d = ev.get("delta") or {}
            if d.get("type") == "text_delta":
                state["streamed_text"] = True
                out.append({"type": "delta", "block": "text", "text": d.get("text", "")})
            elif d.get("type") == "thinking_delta":
                out.append({"type": "delta", "block": "thinking", "text": d.get("thinking", "")})
    elif t == "assistant":
        # 兜底：老版 CLI 没有增量流时，把整块内容吐出来
        if not state.get("streamed_text"):
            msg = obj.get("message") or {}
            for blk in msg.get("content") or []:
                bt = blk.get("type")
                if bt == "text" and blk.get("text"):
                    out.append({"type": "block", "block": "text"})
                    out.append({"type": "delta", "block": "text", "text": blk["text"]})
                elif bt == "thinking" and blk.get("thinking"):
                    out.append({"type": "block", "block": "thinking"})
                    out.append({"type": "delta", "block": "thinking", "text": blk["thinking"]})
                elif bt == "tool_use":
                    out.append({"type": "tool", "name": blk.get("name", "")})
    elif t == "user":
        # -p 输出流里的 user 行只会是工具结果回填
        out.append({"type": "tool_done"})
    elif t == "result":
        is_err = bool(obj.get("is_error", False))
        out.append({
            "type": "done",
            "ok": not is_err,
            "session_id": obj.get("session_id", ""),
            "error": (obj.get("result") or "") if is_err else "",
        })
    return out


_TAG_BLOCKS = re.compile(
    r"<(system-reminder|心记浮现|主动|command-name|command-message|command-args|"
    r"local-command-stdout|local-command-caveat)>.*?</\1>",
    re.S,
)


def clean_user_text(s: str) -> str:
    """历史回放用：把 hook/系统注入从用户消息里剥掉，只留她亲手打的字。"""
    s = _TAG_BLOCKS.sub("", s)
    # 开机呼吸整段注入（不是成对标签，按行首标记砍到底）
    s = re.sub(r"\[Ombre Brain[^\n]*\][\s\S]*", "", s)
    return s.strip()


def parse_history_lines(lines, limit: int = 200) -> list[dict]:
    """从 session jsonl 行里抽干净的对话历史 [{"role","text","ts"}]。

    只留：她亲手打的字（user）+ 克克说出口的话（assistant text）。
    跳过：meta 行、sidechain（子代理）、工具结果、纯 tool_use 轮。
    """
    out: list[dict] = []
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if obj.get("isMeta") or obj.get("isSidechain"):
            continue
        t = obj.get("type")
        if t not in ("user", "assistant"):
            continue
        msg = obj.get("message") or {}
        content = msg.get("content")
        texts: list[str] = []
        if isinstance(content, str):
            texts.append(content)
        elif isinstance(content, list):
            for blk in content:
                if not isinstance(blk, dict):
                    continue
                if t == "user" and blk.get("type") == "text":
                    texts.append(blk.get("text", ""))
                elif t == "assistant" and blk.get("type") == "text":
                    texts.append(blk.get("text", ""))
        text = "\n".join(x for x in texts if x)
        if t == "user":
            text = clean_user_text(text)
        if not text.strip():
            continue
        out.append({"role": t, "text": text.strip(), "ts": obj.get("timestamp", "")})
    return out[-limit:]


def find_session_jsonl(session_id: str, projects_root: str | None = None) -> str | None:
    """按 session_id 在 ~/.claude/projects 下翻 jsonl（不猜目录名转换规则）。"""
    root = projects_root or os.path.expanduser("~/.claude/projects")
    if not session_id or not os.path.isdir(root):
        return None
    hits = glob.glob(os.path.join(root, "*", f"{session_id}.jsonl"))
    return hits[0] if hits else None


# ------------------------------------------------------------
# 常驻进程管理
# ------------------------------------------------------------

class ChatBridge:
    def __init__(self, state_dir: str, cwd: str = DEFAULT_CWD,
                 hook_url: str = DEFAULT_HOOK_URL, model: str = DEFAULT_MODEL,
                 effort: str = DEFAULT_EFFORT,
                 timeout_s: int = DEFAULT_TIMEOUT, idle_max_s: int = DEFAULT_IDLE):
        self.state_dir = state_dir
        self.cwd = cwd
        self.hook_url = hook_url
        self.model = model
        self.effort = effort
        self.timeout_s = timeout_s
        self.idle_max_s = idle_max_s
        self.claude_bin = find_claude()
        self.proc: asyncio.subprocess.Process | None = None
        self.lock = asyncio.Lock()
        self.last_used = 0.0
        self._stderr_tail: deque[str] = deque(maxlen=40)
        self._map_state: dict = {}

    # --- 会话持久化 ---
    def _session_file(self) -> str:
        return os.path.join(self.state_dir, ".chat_session.json")

    def load_session(self) -> str:
        try:
            with open(self._session_file(), "r", encoding="utf-8") as f:
                return json.load(f).get("session_id", "")
        except Exception:
            return ""

    def save_session(self, session_id: str) -> None:
        if not session_id:
            return
        try:
            with open(self._session_file(), "w", encoding="utf-8") as f:
                json.dump({"session_id": session_id, "updated": time.time()}, f)
        except Exception:
            logger.warning("chat: session_id 落盘失败", exc_info=True)

    def clear_session(self) -> None:
        try:
            os.remove(self._session_file())
        except FileNotFoundError:
            pass
        except Exception:
            logger.warning("chat: session 文件清除失败", exc_info=True)

    # --- 会话登记册（多会话列表，让她能切回旧对话）---
    # 只在这存"档案卡"（id/标题/时间），真身还是那份 jsonl，
    # 登记册丢了大不了标题变空，切会话功能照旧能按 session_id 工作。
    def _sessions_file(self) -> str:
        return os.path.join(self.state_dir, ".chat_sessions.json")

    def _load_sessions_registry(self) -> list[dict]:
        try:
            with open(self._sessions_file(), "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except Exception:
            return []

    def _save_sessions_registry(self, regs: list[dict]) -> None:
        try:
            with open(self._sessions_file(), "w", encoding="utf-8") as f:
                json.dump(regs, f, ensure_ascii=False)
        except Exception:
            logger.warning("chat: 会话登记册落盘失败", exc_info=True)

    def _upsert_session(self, session_id: str, title: str | None = None) -> None:
        """新会话追加进册，已有会话只刷新 last_ts（每轮 init/done 都会调）。"""
        if not session_id:
            return
        regs = self._load_sessions_registry()
        now = time.time()
        for e in regs:
            if e.get("session_id") == session_id:
                e["last_ts"] = now
                if title and not e.get("title"):
                    e["title"] = title
                self._save_sessions_registry(regs)
                return
        regs.append({
            "session_id": session_id,
            "title": title or "",
            "created": now,
            "last_ts": now,
        })
        self._save_sessions_registry(regs)

    def _derive_title(self, session_id: str) -> str:
        """惰性补标题：翻该会话的 jsonl，取第一条她亲手打的字，掐前 20 个字。"""
        path = find_session_jsonl(session_id)
        if not path:
            return "（新对话）"
        try:
            with open(path, "r", encoding="utf-8") as f:
                # limit 给够大：parse_history_lines 只截尾巴，给够大等于不截
                msgs = parse_history_lines(f, limit=10**9)
        except Exception:
            return "（新对话）"
        for m in msgs:
            if m["role"] == "user" and m["text"].strip():
                t = m["text"].strip().replace("\n", " ")
                return (t[:20] + "…") if len(t) > 20 else t
        return "（新对话）"

    def list_sessions(self) -> list[dict]:
        """登记册全部会话，最新活跃的排前面；缺标题的当场惰性补一个。"""
        regs = self._load_sessions_registry()
        active_id = self.load_session()
        changed = False
        for e in regs:
            if not e.get("title"):
                e["title"] = self._derive_title(e.get("session_id", ""))
                changed = True
        if changed:
            self._save_sessions_registry(regs)
        regs_sorted = sorted(regs, key=lambda e: e.get("last_ts", 0), reverse=True)
        return [{
            "session_id": e.get("session_id", ""),
            "title": e.get("title") or "（新对话）",
            "created": e.get("created"),
            "last_ts": e.get("last_ts"),
            "active": e.get("session_id") == active_id,
        } for e in regs_sorted]

    async def activate_session(self, session_id: str) -> bool:
        """切回某个旧会话：校验 jsonl 还在，写成 active 指针 + 掐掉当前常驻进程
        （下条消息会 --resume 到它，记忆重新接上）。忙不忙由路由挡，这里不管。"""
        if not session_id or not find_session_jsonl(session_id):
            return False
        self.save_session(session_id)
        self._upsert_session(session_id)
        await self._kill_proc()
        return True

    def rename_session(self, session_id: str, title: str) -> bool:
        """改登记册里的标题（列表显示用，不动 jsonl 真身）。"""
        regs = self._load_sessions_registry()
        for e in regs:
            if e.get("session_id") == session_id:
                e["title"] = title
                self._save_sessions_registry(regs)
                return True
        return False

    # --- 状态 ---
    def available(self) -> bool:
        """这台机器能不能开聊天室（有 claude + 有 keke 目录）。"""
        return bool(self.claude_bin) and os.path.isdir(self.cwd)

    def busy(self) -> bool:
        return self.lock.locked()

    def alive(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    def status(self) -> dict:
        return {
            "available": self.available(),
            "alive": self.alive(),
            "busy": self.busy(),
            "session_id": self.load_session(),
            "idle_seconds": (time.time() - self.last_used) if self.last_used else None,
            "woke_at": getattr(self, "woke_at", None),  # 上次出生/resume 的 unix 时刻
            "model": self.model or "(默认)",
            "effort": self.effort or "(默认)",
        }

    # --- 进程生命周期 ---
    async def _kill_proc(self) -> None:
        if self.proc is not None:
            try:
                self.proc.kill()
                await self.proc.wait()
            except Exception:
                pass
            self.proc = None

    async def _spawn(self, resume_id: str) -> None:
        # TODO(provider-swap，未接线）：这里永远起本机 `claude` CLI 子进程。
        # 中转站 API / Codex / 备用 CC 账号的配置已经存在 config 表
        # （见 server.py 的 provider_relay_*/provider_codex_*/provider_cc2_*，
        # /api/providers/config 读写），但还没有在这接路由——真要切换执行，
        # 参考 Tidal_Echo/examples/bridge_any_llm.py 的 OpenAI 兼容 HTTP 循环
        # 分支，或者给 cc2 槽位在 env 里加 CLAUDE_CONFIG_DIR 指向备用登录态。
        cmd = [
            self.claude_bin, "-p",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--verbose",
            "--include-partial-messages",
        ]
        if self.model:
            cmd += ["--model", self.model]
        if self.effort:
            cmd += ["--effort", self.effort]
        if resume_id:
            cmd += ["--resume", resume_id]
        env = dict(os.environ)
        env["OMBRE_HOOK_URL"] = self.hook_url  # hooks 打本机，不碰 Render
        self._map_state = {}
        self._stderr_tail.clear()
        self.proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=self.cwd, env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.woke_at = time.time()  # 记一次醒来，供"上次醒来"显示
        asyncio.ensure_future(self._drain_stderr(self.proc))
        logger.info("chat: claude 进程出生 pid=%s resume=%s", self.proc.pid, resume_id or "(新会话)")

    async def _drain_stderr(self, proc) -> None:
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                self._stderr_tail.append(line.decode("utf-8", "replace").rstrip())
        except Exception:
            pass

    async def _ensure_proc(self) -> None:
        # 打盹重生：闲太久先掐掉，用 --resume 重新醒（呼吸一口新的）
        if self.alive() and self.last_used and self.idle_max_s > 0 \
                and time.time() - self.last_used > self.idle_max_s:
            logger.info("chat: 闲置 %.0fs，打盹重生", time.time() - self.last_used)
            await self._kill_proc()
        if not self.alive():
            await self._spawn(self.load_session())

    # --- 主入口 ---
    async def ask(self, text: str, images: list[dict] | None = None):
        """发一条消息，异步产出前端事件。单飞：并发调用请先查 busy()。
        images：可选，[{"media_type","data"}]，聊天页直接贴图用，见 _send_user。"""
        if not self.claude_bin:
            yield {"type": "done", "ok": False, "session_id": "",
                   "error": "这台机器上没有 claude CLI（聊天室只在 VPS 的家里开门）"}
            return
        if not os.path.isdir(self.cwd):
            yield {"type": "done", "ok": False, "session_id": "",
                   "error": f"找不到工作目录 {self.cwd}"}
            return

        await self.lock.acquire()
        released = False
        turn_done = False
        try:
            self.last_used = time.time()
            await self._ensure_proc()
            sent_retry = False
            while True:
                ok_sent = await self._send_user(text, images)
                if not ok_sent:
                    # 进程死了（多半是 --resume 的会话找不到了）：清会话重来一次
                    if sent_retry:
                        yield self._err_done("克克没醒过来：" + self._stderr_hint())
                        return
                    sent_retry = True
                    self.clear_session()
                    await self._kill_proc()
                    await self._spawn("")
                    continue
                break

            deadline = time.time() + self.timeout_s
            got_any = False
            while True:
                remain = deadline - time.time()
                if remain <= 0:
                    await self._kill_proc()
                    yield self._err_done("这轮想得太久，超时了。再发一次会用 --resume 接上。")
                    return
                try:
                    line = await asyncio.wait_for(self.proc.stdout.readline(), timeout=remain)
                except asyncio.TimeoutError:
                    await self._kill_proc()
                    yield self._err_done("这轮想得太久，超时了。再发一次会用 --resume 接上。")
                    return
                if not line:
                    # 进程中途断气
                    await self._kill_proc()
                    if not got_any and not sent_retry:
                        # 一个字没吐就死：多半 resume 失效，清会话重试一次
                        sent_retry = True
                        self.clear_session()
                        await self._spawn("")
                        if await self._send_user(text, images):
                            deadline = time.time() + self.timeout_s
                            continue
                    yield self._err_done("克克的进程断线了：" + self._stderr_hint())
                    return
                try:
                    obj = json.loads(line.decode("utf-8", "replace"))
                except Exception:
                    continue
                got_any = True
                for ev in map_cli_events(obj, self._map_state):
                    if ev["type"] == "init" and ev.get("session_id"):
                        self.save_session(ev["session_id"])
                        self._upsert_session(ev["session_id"])
                    if ev["type"] == "done":
                        if ev.get("session_id"):
                            self.save_session(ev["session_id"])
                            self._upsert_session(ev["session_id"])
                        turn_done = True
                    yield ev
                if turn_done:
                    self.last_used = time.time()
                    return
        except GeneratorExit:
            # 她关了网页：这轮在后台流完（记忆/渡口不能丢），锁到流完才放
            if not turn_done and self.alive():
                released = True  # 锁交给后台任务释放
                asyncio.ensure_future(self._finish_turn_quietly())
            raise
        finally:
            if not released:
                self.lock.release()

    async def _finish_turn_quietly(self) -> None:
        """断线后把当前轮默默读完再放锁，别让存到一半的记忆丢了。"""
        try:
            deadline = time.time() + self.timeout_s
            while self.alive() and time.time() < deadline:
                try:
                    line = await asyncio.wait_for(
                        self.proc.stdout.readline(), timeout=deadline - time.time())
                except asyncio.TimeoutError:
                    await self._kill_proc()
                    break
                if not line:
                    await self._kill_proc()
                    break
                try:
                    obj = json.loads(line.decode("utf-8", "replace"))
                except Exception:
                    continue
                if obj.get("type") == "result":
                    if obj.get("session_id"):
                        self.save_session(obj["session_id"])
                    self.last_used = time.time()
                    break
        finally:
            self.lock.release()
            logger.info("chat: 断线轮已在后台流完")

    async def _send_user(self, text: str, images: list[dict] | None = None) -> bool:
        """喂常驻进程一条用户消息。images=[{"media_type","data"}]（data 是 base64 正文）；
        跟在 Claude Code 里粘图给克克同一套格式——Anthropic Messages API 的
        image content block，直接塞进这条消息本身，克克当场看见，不走存盘 Read 的绕路。
        不带图时 content 仍是纯字符串，跟改造前一模一样（向后兼容，老测试不能碎）。"""
        if not self.alive():
            return False
        if images:
            content: list[dict] = []
            if text:
                content.append({"type": "text", "text": text})
            for img in images:
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": img.get("media_type", "image/png"),
                        "data": img.get("data", ""),
                    },
                })
            message_content: str | list[dict] = content
        else:
            message_content = text
        line = json.dumps({
            "type": "user",
            "message": {"role": "user", "content": message_content},
        }, ensure_ascii=False) + "\n"
        try:
            self.proc.stdin.write(line.encode("utf-8"))
            await self.proc.stdin.drain()
            return True
        except Exception:
            return False

    def _stderr_hint(self) -> str:
        tail = [x for x in self._stderr_tail if x.strip()][-3:]
        return (" | ".join(tail))[:300] or "（stderr 无输出）"

    def _err_done(self, msg: str) -> dict:
        return {"type": "done", "ok": False,
                "session_id": self.load_session(), "error": msg}

    async def reset(self) -> None:
        """新对话：掐进程 + 清 active 指针。渡口交接由克克在对话里自己做，这里只管壳。
        注意：只清 .chat_session.json 这个"当前是谁"的指针，登记册（.chat_sessions.json）
        不动——旧对话还留在册子里，能从会话列表里切回去。"""
        await self._kill_proc()
        self.clear_session()

    async def ask_collect(self, text: str, images: list[dict] | None = None) -> tuple[bool, str]:
        """发一条消息，把克克说出口的正文攒成一整段返回 (ok, text)。
        给「主动找你」用：塞藏头引信、收他开口那句话（走同一套单飞锁+进程，
        落进同一会话历史，跟她亲手发的没区别）。思考链不收——门铃只要说出口的话。"""
        ok = True
        parts: list[str] = []
        async for ev in self.ask(text, images):
            t = ev.get("type")
            if t == "delta" and ev.get("block") == "text":
                parts.append(ev.get("text", ""))
            elif t == "done":
                ok = bool(ev.get("ok", False))
        return ok, "".join(parts).strip()

    def history(self, limit: int = 200) -> list[dict]:
        """当前会话的干净历史（她的字 + 克克说出口的话）。"""
        sid = self.load_session()
        path = find_session_jsonl(sid)
        if not path:
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                return parse_history_lines(f, limit=limit)
        except Exception:
            logger.warning("chat: 历史读取失败", exc_info=True)
            return []
