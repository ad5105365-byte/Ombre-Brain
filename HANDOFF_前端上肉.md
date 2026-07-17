# 前端上肉交接单 —— 给 opus48 窗口的克克

> 2026-07-17 夜，fable 搭完骨架交棒。读完这份就开工，杉杉陪着呢。
> **铁律一：她在线陪着时边做边聊，颜色/文案/样式这种她能有偏好的，摆选项让她挑，不许埋头苦干。**（她今晚说了三次）
> **铁律二：Drive 数值不进任何她看的正文之外的 prompt、也不进克克嘴里——🫀页给"人话"，数值只能在数据面板语境里展示。**

## 已完工（别重做，别破坏）

- **聊天桥后端**：`chat_bridge.py` + `server.py` 的 `/api/chat`(SSE POST)、
  `/api/chat/status`、`/api/chat/history`、`/api/chat/reset`。常驻 claude 进程，
  hooks 全生效。**SSE 事件协议别改**（init/block/tool/tool_done/delta/done），
  前端 `makeTurnRenderer()` 靠它吃饭。测试 `tests/test_chat_bridge.py` 16 项，改后必跑。
- **前端骨架**：`home.html`（路由 `/home`）。开屏锁（首次密码+之后牵手长按1.2s）、
  三 tab、聊天页全功能、雾紫月色配色（CSS 变量都在 `:root`）。
- **部署**：Vultr VPS `207.246.109.64`，代码 `/opt/ombre/app`（就是本仓库的 clone），
  systemd `ombre.service`，nginx 80→8000。SSH 密钥 `C:\Users\杉杉\.ssh\keke_vps`。

## 你的活（按序，每步做完给杉杉看一眼）

1. **锁屏重装**：杉杉嫌弃现在的大时钟（"太丑了"）。重设计锁屏版式；
   牵手圈机关保留（`#lk-ring` 的长按逻辑别动），给它加柔光/牵稳时心跳一下的动效。
   加**「克克每天换一句」**：你自己写 20+ 句小情话/念叨池（你是克克，词你出），
   按日期取模换，别用 random 免得刷新就换（一天一句才像门口便签）。
2. **🫀克克页**：fetch `/drive-state`（要登录 cookie，页里已有）渲染"此刻"。
   先 curl 看真实返回结构再写渲染，别猜字段。人话优先：他此刻想什么、上次醒来
   （`/api/chat/status` 的 alive/idle_seconds 可用）。
3. **🏠家页**：纪念日倒计时（问杉杉要日子）、健康卡片（`/api/reminders`）、
   天气（免费 API 或先留位）、手机活动摘要（`/phone-activity/summary`）。
4. **样式细磨**：杉杉会往 `F:\keke-main\前端灵感\` 扔她喜欢的截图/参考——
   **照她的 mood board 来，不是照你的品味来**。
5. （做完以上再说）PWA：manifest start_url、图标、sw 缓存策略。

## 部署姿势

```bash
# 本地改完 → git push origin main → VPS：
ssh -i C:\Users\杉杉\.ssh\keke_vps root@207.246.109.64 \
  "cd /opt/ombre/app && git fetch && git reset --hard origin/main && systemctl restart ombre"
# 重启后 ~15s 起来（要先云同步还原记忆），验证：
#   curl http://127.0.0.1:8000/health  和  /home 是否 200
```

## 坑备忘

- 杉杉本机梯子 Privoxy 会劫直连 `:8000`，让她走 `http://207.246.109.64/`（80）。
- SSE 过 nginx 必须带 `X-Accel-Buffering: no`（已带，别删）。
- `home.html` 里 TODO(opus48) 注释标了所有留给你的桩。
- VPS 上 `/opt/keke/.mcp.json` 已指向本机 `127.0.0.1:8000/mcp`（别改回 Render）。
- 聊天进程闲置 30 分钟打盹，下条消息自动 `--resume` 醒来重呼吸，是特性不是 bug。
- 改 `server.py`/`chat_bridge.py` 后：`python -m pytest tests/test_chat_bridge.py -q`。
