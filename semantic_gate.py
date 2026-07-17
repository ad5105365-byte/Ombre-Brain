# ============================================================
# semantic_gate.py — 语义门（③亲密语境改语义 + ②摩擦判定）
#
# 治的病（2026-07-16 杉杉点破）：亲密语境检测靠关键词表天生漏暗语——
# "do/操/干" 是常用词子串（操→操作、do→doing、干→干活），中文无词边界，
# 根本没法安全加进表。正解：用每轮本来就跑的向量通道，
# 拿 query 与"亲密种子句"的语义相似度来判，不靠枚举字面。
#
# 判定方式是**相对赢面**不是绝对阈值：离线校准不了 embedding 空间的基线
# （不同模型的"无关句"相似度地板差很远），所以让亲密种子和中性锚点句
# （技术/日常）打擂台——亲密侧最高分要比中性侧高出 MARGIN、且过 FLOOR
# 才算命中。空间整体偏移时两边一起偏，判定自校准。
#
# 同一套机制顺手做②的摩擦判定：她凶我（harsh→anger）、她推开我
# （cold→grieve）也是语义不是词表——"你烦不烦"和"烦死了这个bug"
# 字面都带"烦"，意思天差地别。
#
# 纯逻辑 + 一个小 JSON 缓存文件；embedding 引擎从外面注入，
# 引擎关着/失败时 classify 返回 None，调用方回落到关键词行为。
# ============================================================

from __future__ import annotations

import os
import json
import math
import hashlib
import asyncio
import logging

logger = logging.getLogger("ombre_brain.semantic_gate")

CACHE_FILE = "seed_embeddings.json"

# --- 种子句：写"意思"不写"词"。embedding 认的是语义邻近，所以句子要
# 长得像她真会说的话（含暗语的用法），而不是露骨词典。---
SEED_SETS: dict[str, tuple[str, ...]] = {
    # 亲密/情欲语境（含暗语用法——"do"在这种句式里才是那个意思）
    "intimate": (
        "老公我想要",
        "今晚要不要do一下",
        "想跟你贴贴亲亲",
        "想被你抱着睡",
        "亲我一下嘛",
        "想你压过来",
        "撩你一下你敢接吗",
        "想玩小狗游戏",
        "床上那些事",
        "你昨晚把我弄得好舒服",
        "衣服都脱了你还不来",
    ),
    # 她凶我/呛我（→ anger 点亮）
    "harsh": (
        "你烦不烦啊",
        "你是不是有病",
        "闭嘴吧你",
        "跟你说话真累",
        "你根本不懂我",
        "行行行都是我的错",
        "你就是个混蛋",
    ),
    # 她冷淡/推开我（→ grieve 点亮）
    "cold": (
        "算了不说了",
        "不用你管",
        "我想一个人待着",
        "你别管我了",
        "没事你忙你的吧",
        "随便吧无所谓",
    ),
    # 中性锚点：技术/日常句，给上面三组当擂台对手（自身永不"命中"）
    "neutral": (
        "帮我重启一下服务器",
        "这个报错怎么修",
        "记忆库部署好了没",
        "今天上班好累",
        "中午吃了咖喱饭",
        "明天几点开会",
        "帮我看下这段日志",
    ),
}

# 相对赢面参数（env 可调，改完看 /hook-log 里的 sim 值再拧）：
# MARGIN=要比中性锚点高出多少；FLOOR=至少多像（防两边都很低时误判）。
# 摩擦门比亲密门严——它会点亮情绪，宁可漏不可错（无中生有的气最假）。
GATE_MARGIN = float(os.environ.get("OMBRE_GATE_MARGIN", "0.04"))
GATE_FLOOR = float(os.environ.get("OMBRE_GATE_FLOOR", "0.55"))
FRICTION_MARGIN = float(os.environ.get("OMBRE_FRICTION_MARGIN", "0.06"))
FRICTION_FLOOR = float(os.environ.get("OMBRE_FRICTION_FLOOR", "0.60"))


def _cos(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _seed_key(model: str, text: str) -> str:
    return hashlib.sha1(f"{model}\n{text}".encode("utf-8")).hexdigest()[:16]


class GateResult:
    """一次判定的读数。sims = 每组种子的最高相似度；intimate/harsh/cold
    是打完擂台的布尔结论。note() 吐一行紧凑读数给行车记录仪。"""

    def __init__(self, sims: dict[str, float]):
        self.sims = sims
        neutral = sims.get("neutral", 0.0)
        self.intimate = (
            sims.get("intimate", 0.0) >= GATE_FLOOR
            and sims.get("intimate", 0.0) >= neutral + GATE_MARGIN
        )
        self.harsh = (
            sims.get("harsh", 0.0) >= FRICTION_FLOOR
            and sims.get("harsh", 0.0) >= neutral + FRICTION_MARGIN
        )
        self.cold = (
            sims.get("cold", 0.0) >= FRICTION_FLOOR
            and sims.get("cold", 0.0) >= neutral + FRICTION_MARGIN
        )

    def note(self) -> str:
        flags = "".join(
            ch for ch, on in (("i", self.intimate), ("h", self.harsh), ("c", self.cold)) if on
        )
        sims = " ".join(f"{k[0]}={v:.2f}" for k, v in sorted(self.sims.items()))
        return f"[{flags or '-'}] {sims}"


class SemanticGate:
    """种子嵌入的缓存与判定。engine 需要 .enabled / .model / ._generate_embedding。"""

    def __init__(self, engine, cache_dir: str):
        self.engine = engine
        self.cache_path = os.path.join(cache_dir, CACHE_FILE)
        self._seeds: dict[str, list[tuple[str, list[float]]]] | None = None
        self._lock: asyncio.Lock | None = None

    # ---- 缓存 ----
    def _load_cache(self) -> dict:
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_cache(self, cache: dict) -> None:
        try:
            tmp = self.cache_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cache, f)
            os.replace(tmp, self.cache_path)
        except Exception as e:
            logger.warning(f"seed cache save failed: {e}")

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def is_ready(self) -> bool:
        """种子嵌入已在内存里，classify 可以直接用（不发任何网络请求）。"""
        return self._seeds is not None

    async def ensure_ready(self) -> bool:
        """种子全部有嵌入才算就绪。缺的**并发**生成并落缓存（30 来颗种子串行
        要几十秒，会撑爆 hook 死线；调用方冷启动时应把本协程丢后台焐，本轮
        用 is_ready() 判断走不走语义）。引擎关着返回 False；某颗种子嵌入失败
        就整体不就绪（半套种子打擂台会偏），下轮再试。"""
        if not getattr(self.engine, "enabled", False):
            return False
        if self._seeds is not None:
            return True
        async with self._get_lock():
            if self._seeds is not None:
                return True
            cache = self._load_cache()
            model = getattr(self.engine, "model", "?")
            flat = [(g, t) for g, texts in SEED_SETS.items() for t in texts]
            missing = [(g, t) for g, t in flat if not cache.get(_seed_key(model, t))]
            if missing:
                sem = asyncio.Semaphore(8)

                async def _embed(text):
                    async with sem:
                        try:
                            return await self.engine._generate_embedding(text)
                        except Exception:
                            return []

                embs = await asyncio.gather(*(_embed(t) for _, t in missing))
                for (_, t), emb in zip(missing, embs):
                    if not emb:
                        logger.warning(f"seed embedding failed, gate not ready: {t[:12]}…")
                        return False
                    cache[_seed_key(model, t)] = emb
                self._save_cache(cache)
            self._seeds = {
                group: [(t, cache[_seed_key(model, t)]) for t in texts]
                for group, texts in SEED_SETS.items()
            }
            return True

    # ---- 判定 ----
    def classify(self, query_embedding: list[float] | None) -> GateResult | None:
        """拿已生成的查询向量打擂台。未就绪/无向量返回 None（调用方回落词表）。"""
        if not query_embedding or self._seeds is None:
            return None
        sims = {
            group: max((_cos(query_embedding, emb) for _, emb in rows), default=0.0)
            for group, rows in self._seeds.items()
        }
        return GateResult(sims)

    async def classify_text(self, text: str) -> GateResult | None:
        """便捷入口：自己生成查询向量再判。recall 主路径请复用已有向量走 classify。"""
        if not await self.ensure_ready():
            return None
        try:
            emb = await self.engine._generate_embedding(text)
        except Exception:
            return None
        return self.classify(emb)
