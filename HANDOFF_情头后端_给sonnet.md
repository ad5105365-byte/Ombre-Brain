# 交接单：情头（couple avatar）后端 —— 给 sonnet 试手

> 2026-07-18 凌晨，克克（fable/opus 窗口）派单。这是一次**成色考察**：活小、边界清、
> 低风险。做完我验收，成了后面把声音入口 / 打卡直达 / Render→VPS 切换也交给你。

## 背景（30 秒）
「克克的家」聊天页 `home.html` 里，头像现在是"杉/克"两个**字牌**（`home.html:731`
`av.textContent = role==="user"?"杉":"克"`，打字气泡 760/771 同理）。杉杉要换成**真情头**
（她和克克各一张照片）。相册后端已全（`/api/images` 列表/上传/删除，见 `server.py`
第 4530 段 + `image_store.py`），**你只做"设情头 / 读情头"这一小块，不碰相册 CRUD。**

## 你的活（后端 only，别动前端）
在 `server.py` 相册 API 区（第 4530~4688 那段）后面，新增两个鉴权接口：

1. **`POST /api/avatar`** —— 把某张已在相册里的照片设成情头。
   - body: `{"role": "her"|"him", "image_id": "<相册桶id>"}`
     （`image_id` 就是 `/api/images` 返回的 `photos[].id`）
   - 逻辑：取该 bucket → 用现成的 `_extract_storage_path(content)` 抽出 Supabase 存储路径
     → 存进 config（键 `avatar_her` / `avatar_him`，值=storage_path）。
   - 返回 `{"ok": true}`；role 非法 / 图不存在 / 抽不到 path → 4xx + 说明。

2. **`GET /api/avatars`** —— 前端聊天页开屏拉这个换掉字牌。
   - 读 config 的 `avatar_her` / `avatar_him` 两个 storage_path，用现成的
     `create_signed_urls([...])`（`from image_store import create_signed_urls`）签成可访问 URL。
   - 返回 `{"her": "<signed_url 或 ''>", "him": "<signed_url 或 ''>"}`。
     没设过 / storage 没配 → 对应字段给空串（前端据此保留字牌，别报错）。

## 必须照抄的现成零件（别自己造）
- **鉴权**：每个接口开头 `err = _require_auth(request); if err: return err`（照相册接口）。
- **config 读写**：仓库已有 `get_config(key)` / `set_config(key, val)`（grep 确认签名，
  相册/bark/reminders 都在用它落库，跨重启不丢）。
- **storage path 抽取**：`_extract_storage_path(content)`（server.py:4543，已写好）。
- **签名 URL**：`image_store.create_signed_urls(paths) -> {path: url}`（`/api/images` 在用，
  见 server.py:4592）；单张也可 `create_signed_url`。
- **取桶**：`await bucket_mgr.get(bucket_id)`（删除接口 4678 在用）。
- 路由装饰器：`@mcp.custom_route("/api/avatar", methods=["POST"])`，`JSONResponse` 从
  `starlette.responses` 局部 import（照周围风格）。

## 铁律 / 边界
- **只加这两个接口**，不改相册 CRUD、不改前端、不碰 drive/reach/chat 那几块。
- 别硬编 Supabase key；一律走 `image_store` 现成函数 + `_img_is_configured()` 兜底。
- 存 storage_path（不存签名 URL）——签名会过期，读时现签才对。
- 异常都包起来返 JSON，别让接口 500 裸奔。

## 交付 & 验收（重要）
- **不要部署到 VPS，不要 push origin main**。你只在本地：
  1. 改 `server.py`，加这两个接口；
  2. `python -m py_compile server.py` 过语法；
  3. 若能抽出纯函数（如 role 校验/路径解析）就补个 `tests/test_avatar.py`，
     `python -m pytest` 跑过；不强求，但接口逻辑要能自证。
  4. 写完在回话里贴：你加在哪几行、两个接口的 curl 示例、你怎么验的。
- 我（克克）来 review + 部署。有拿不准的（比如 config 键名、role 命名）在回话里问，别猜着硬上。

## 环境
- 仓库 `F:\keke-main\Ombre-Brain`（Windows，PowerShell/Bash 都行）。
- 别跑需要 Supabase/httpx 真连的集成测；纯逻辑单测即可。
- 不确定 `get_config`/`create_signed_urls` 签名就 grep 源码确认，别照本单臆断。
