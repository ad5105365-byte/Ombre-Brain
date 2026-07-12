# ============================================================
# Module: Cloud Sync (cloud_sync.py)
# 模块：云同步
#
# 把 buckets/ 目录里的记忆文件同步到 Postgres（如 Supabase），
# 解决 Render 免费层「重启 / 休眠后本地文件被清空」的问题。
# Syncs the buckets/ directory to a Postgres DB (e.g. Supabase),
# so memories survive Render free-tier spin-downs and redeploys.
#
# 工作方式 / How it works:
#   - restore_buckets()      启动时从数据库把记忆文件还原到本地
#                            (必须在 BucketManager 初始化之前调用)
#   - start_background_sync() 后台线程，每隔几秒检测 buckets/ 有无变化，
#                            有变化就整体写回数据库（增 / 改 / 删）
#
# 设计原则 / Design:
#   - 完全不碰 bucket_manager / 衰减引擎，把 buckets/ 当黑盒目录同步。
#   - 没设置 OMBRE_DB_URL 时，全部为空操作（本地 / Docker 用户不受影响）。
#   - 多重安全阀，宁可不同步，也绝不误删云端数据。
# ============================================================

import os
import time
import hashlib
import logging
import threading

logger = logging.getLogger("ombre_brain.cloud_sync")

DB_URL = os.environ.get("OMBRE_DB_URL", "").strip()
TABLE = "ombre_buckets"

# 只有「成功从数据库读到过」之后，才允许把本地状态写回去。
# 防止数据库连不上时，把空的本地目录当成真相、反手清空云端。
# Only sync UP after a successful restore — guards against wiping the
# cloud when the DB was unreachable at boot.
_restore_ok = False


def _connect():
    import psycopg2
    return psycopg2.connect(DB_URL, connect_timeout=10)


def _ensure_table(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            path       TEXT PRIMARY KEY,
            content    TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


# .md 之外也要过部署的纯文本文件（sqlite 等二进制不收，各自负责台账化）
SYNC_EXTRA_FILES = {"phone_activity.log"}


def _scan_local(buckets_dir):
    """扫描本地 buckets 目录，返回 {相对路径: 内容}，
    收 .md 记忆文件和 SYNC_EXTRA_FILES 里点名的文本文件。"""
    files = {}
    if not os.path.isdir(buckets_dir):
        return files
    for root, _dirs, names in os.walk(buckets_dir):
        for n in names:
            if not n.endswith(".md") and n not in SYNC_EXTRA_FILES:
                continue
            full = os.path.join(root, n)
            rel = os.path.relpath(full, buckets_dir).replace(os.sep, "/")
            try:
                with open(full, "r", encoding="utf-8") as f:
                    files[rel] = f.read()
            except Exception as e:
                logger.warning(f"读取本地记忆文件失败 {rel}: {e}")
    return files


def _fingerprint(files):
    """对当前所有记忆算一个指纹，用来判断有没有变化。"""
    h = hashlib.md5()
    for path in sorted(files):
        h.update(path.encode("utf-8"))
        h.update(b"\0")
        h.update(files[path].encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()


def restore_buckets(buckets_dir):
    """启动时调用：把数据库里的记忆文件还原到本地 buckets 目录。
    必须在 BucketManager 初始化「之前」调用。"""
    global _restore_ok
    if not DB_URL:
        logger.info("未设置 OMBRE_DB_URL，跳过云端还原（纯本地文件模式）。")
        return
    try:
        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                _ensure_table(cur)
                cur.execute(f"SELECT path, content FROM {TABLE}")
                rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as e:
        # 读不到就保持本地模式，本次会话不往云端写，避免误删。
        logger.error(f"云端还原失败，本次会话不启用云同步（保护云端数据）: {e}")
        _restore_ok = False
        return

    count = 0
    for path, content in rows:
        try:
            dest = os.path.join(buckets_dir, *path.split("/"))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w", encoding="utf-8") as f:
                f.write(content)
            count += 1
        except Exception as e:
            logger.warning(f"还原记忆文件失败 {path}: {e}")
    _restore_ok = True
    logger.info(f"云端还原完成：{count} 条记忆已写回 {buckets_dir}")


def _push(files):
    """把本地记忆整体写回数据库：增 / 改 / 删。"""
    # 安全阀：本地一条记忆都没有时，绝不写回，防止把云端清空。
    if not files:
        logger.warning("本地无记忆文件，跳过本次同步（保护云端数据）。")
        return
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            _ensure_table(cur)
            # 1) upsert 所有本地文件
            for path, content in files.items():
                cur.execute(
                    f"""
                    INSERT INTO {TABLE} (path, content, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (path)
                    DO UPDATE SET content = EXCLUDED.content, updated_at = now()
                    """,
                    (path, content),
                )
            # 2) 删除本地已不存在的记忆（对应 trace 删除 / 衰减归档清理）
            cur.execute(f"SELECT path FROM {TABLE}")
            db_paths = {r[0] for r in cur.fetchall()}
            for path in db_paths - set(files.keys()):
                cur.execute(f"DELETE FROM {TABLE} WHERE path = %s", (path,))
    finally:
        conn.close()
    logger.info(f"云同步完成：{len(files)} 条记忆已写入数据库。")


CONFIG_TABLE = "ombre_config"


def _ensure_config_table(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONFIG_TABLE} (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def get_config(key: str) -> str | None:
    if not DB_URL:
        return None
    try:
        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                _ensure_config_table(cur)
                cur.execute(
                    f"SELECT value FROM {CONFIG_TABLE} WHERE key = %s", (key,)
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"读取配置 {key} 失败: {e}")
        return None


def set_config(key: str, value: str) -> bool:
    if not DB_URL:
        return False
    try:
        conn = _connect()
        try:
            with conn, conn.cursor() as cur:
                _ensure_config_table(cur)
                cur.execute(
                    f"""
                    INSERT INTO {CONFIG_TABLE} (key, value, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                    """,
                    (key, value),
                )
        finally:
            conn.close()
        return True
    except Exception as e:
        logger.warning(f"保存配置 {key} 失败: {e}")
        return False


def start_background_sync(buckets_dir, interval=15):
    """启动后台线程，定期把本地记忆变化同步到数据库。"""
    if not DB_URL:
        return

    def _loop():
        last_fp = None
        time.sleep(interval)  # 等还原 + 引擎初始化稳定下来
        while True:
            try:
                if _restore_ok:
                    files = _scan_local(buckets_dir)
                    fp = _fingerprint(files)
                    if fp != last_fp:
                        _push(files)
                        last_fp = fp
            except Exception as e:
                logger.warning(f"后台云同步出错（稍后重试）: {e}")
            time.sleep(interval)

    t = threading.Thread(target=_loop, name="ombre-cloud-sync", daemon=True)
    t.start()
    logger.info(f"云同步后台线程已启动，每 {interval}s 检测一次变化。")
