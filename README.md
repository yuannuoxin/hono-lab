# Hono Lab · WebSocket 与时间测试台

一个 Hono（Node 运行时）小项目，带三个自测页面：**WebSocket 回声测试**、**服务端时间偏移测量**和 **SSE 单向推送**。没有前端构建步骤，页面是原生 HTML/CSS/JS。

## 运行

```bash
npm install
npm run dev      # node --watch，端口 3100
```

打开 http://127.0.0.1:3100

| 路径 | 说明 |
| --- | --- |
| `/` | 首页，三页入口 + 服务信息 |
| `/sse` | SSE 单向推送测试页 |
| `/websocket` | WebSocket 测试页 |
| `/time` | 服务端时间测试页 |
| `/ws` | WebSocket 端点 |
| `/api/sse` | SSE 事件流（`text/event-stream`） |
| `/api/sse/push` | 手动触发一条/多条 `notice` 广播 |
| `/api/sse/kick` | 掐断所有流，用来演示断线续传 |
| `/api/time` | 服务器时间（JSON，含 `x-server-time` 响应头） |
| `/api/time/sse` | 时间页专属的 SSE 快节拍推送（80ms 一条） |
| `/api/info` | Node 版本、pid、端口、运行时长 |

## 技术栈

- **hono** 4.x
- **@hono/node-server** 2.x（HTTP 服务）
- **@hono/node-ws** 1.3.x（`createNodeWebSocket` + `injectWebSocket`）
- **hono/streaming** 的 `streamSSE`（SSE）
- 前端零依赖，样式手写在一个 `public/styles.css` 里

## WebSocket 端点协议

连上 `/ws` 后服务端先发一条 `hello`，之后每秒广播一次 `tick`：

```jsonc
// 服务端 → 客户端
{ "type": "hello", "t": 1795674419000, "id": "pmjrc3", "clients": 1 }
{ "type": "tick",  "t": 1795674420000, "clients": 1 }

// 客户端 → 服务端（测 RTT）
{ "type": "ping", "id": "1-z29e", "t0": 1795674420187 }
// 服务端 → 客户端
{ "type": "pong", "id": "1-z29e", "t0": 1795674420187, "t1": 1795674420188 }
```

- 非 `ping` 的文本原样回收成 `{ "type": "echo", "t1": ..., "bytes": n, "data": "..." }`
- 二进制帧原样回显，随后补一条 `{ "type": "echo-binary", "bytes": 32 }`
- 所有连接关闭时停掉 `setInterval`，不空转

页面上的地址栏可以填任意 `ws://` / `wss://`（只写域名会自动补 `ws://`，填 `https://` 会换成 `wss://`），默认连本机 `/ws`，上次连过的地址存在 `localStorage`。连第三方纯回声服务时没有 `hello` / `tick`，`ping` 会被原样吐回，页面按回包里的 `id` 认领算 RTT。实测可用的公共回声：`wss://ws.postman-echo.com/raw`、`wss://ws.ifelse.io`（后者连上会先发一条横幅文本）。

## 时间接口与偏差算法

`GET /api/time` 返回：

```json
{ "t": 1795674419000, "iso": "...", "tz": "Asia/Shanghai", "offsetMinutes": 480, "uptimeMs": 4037 }
```

页面采样多次，每次记三个时刻：本机发出前 `t0`、响应里的服务器 `tS`、本机收到后 `t3`。

```
offset = tS − (t0 + t3) / 2      // 服务器时钟 − 本机时钟
rtt    = t3 − t0
```

取 `rtt` 最小的一次作为最终偏差（往返越短，中点假设越接近真实），然后用 `Date.now() + offset` 跑一块服务器秒表。同一台机器上实测偏差在 ±2 ms 内，往返 1–5 ms。

### 三种采样通道

页面右上角可以切 HTTP / WebSocket / SSE，公式一样，区别只在三个时刻怎么拿到：

| 通道 | 往返怎么来 | 能算出真实偏差吗 | 备注 |
| --- | --- | --- | --- |
| HTTP | 一次请求 | 能，取往返最短 | 最通用，但要付连接、代理、队头开销 |
| WebSocket | 长连接上发一帧回一帧 | 能，往返通常更小更稳 | `/ws` 新增 `{ "type": "time", "t0": ... }`，服务端回 `{ "type": "time", "tS": ... }` |
| SSE | 只有下行 | 不能，只能给下限 | 客户端发不出 `t0`，只能算 `tS − t3`，结果含服务端→客户端那段单向延迟（约 RTT/2），取 `tS − t3` 最大的一笔 |

SSE 模式在页面上会显示 `≈`、`最小往返 = — 单向`，并标注「下限，含单向延迟」。要精确校时就用 HTTP 或 WebSocket；SSE 只适合看趋势，以及演示为什么单向推送做不了钟差测量。

## SSE 端点协议

`GET /api/sse` 返回 `text/event-stream`，握手先发一条不带 `id` 的 `hello`，随后每秒推 `tick`（带自增 `id`）：

```
event: hello
data: {"cid":"q2f5aj","clients":1,"seq":115,"resumedFrom":"","t":1795675292517}
retry: 3000

event: tick
data: {"t":1795675293177,"clients":1}
id: 116
```

- 服务端保留 **120 条**环形缓冲；浏览器重连会自动带上 `Last-Event-ID`，命中缓冲就把断线期间的事件按原 id 补发，命中不到则发一条 `event: gap`
- `retry: 3000` 让浏览器自己 3 秒后重连，前端不用写重试逻辑
- `hello` 故意不带 `id`，否则会把浏览器的续传基准顶到最新，补发就失效了
- `POST /api/sse/push`（body `{ "text": "...", "burst": 5 }`）广播自定义事件 `notice`；上行必须走普通请求，这是 SSE 与 WebSocket 的根本差别
- `POST /api/sse/kick` 直接掐断所有流，用来实测「断线 → 自动重连 → 补发」

## 部署到 Cloudflare Workers

Node 版跑在本机，Workers 版是另写的一入口（`src/worker.js`），同一套 `public/` 静态资源，通过 `ASSETS` 绑定提供。

新增/改动的文件：

| 文件 | 作用 |
| --- | --- |
| `src/worker.js` | Workers 入口：Hono + `WebSocketPair` 手动升级 + `streamSSE` + 静态资源兜底 |
| `wrangler.jsonc` | 部署配置：`main`、`assets.directory`、`ASSETS` 绑定 |
| `package.json` | 加了 `wrangler` 开发依赖和 `preview` / `deploy` / `deploy:dry` 脚本 |

```bash
npm run preview      # wrangler dev，本地模拟 Workers，默认 http://127.0.0.1:8787
npm run deploy:dry   # 只打包不部署，产物在 dist/
npx wrangler login   # 首次部署需要登录
npm run deploy       # 真正推到 Cloudflare
```

部署后 `/`、`/websocket`、`/time`、`/sse`、`/api/*` 都在同一个 Worker 上；自定义域名在控制台加路由即可。

### 与 Node 版的差异（重要）

- **没有文件系统**：静态页走 `env.ASSETS.fetch()`，`/websocket` 这类别名由 Worker 映射到 `ws.html`。
- **`html_handling` 必须设成 `none`**：默认的自动同名 `.html` 映射会把 `/ws` 当成静态文件 `ws.html` 直接返回 200，WebSocket 永远握手不上（本项目踩过这个坑）。
- **WebSocket 用 `WebSocketPair` 手动升级**：`hono/cloudflare-workers` 的 `upgradeWebSocket` 在 Hono 4.13 已标 deprecated，直接 `server.accept()` + 返回 `101 + webSocket` 更稳。
- **广播只在本实例内**：`clients` 是模块级 `Set`，Workers 会横向扩多个 isolate，跨实例推送要上 Durable Object；`setInterval` 也可能随实例回收停掉。
- **SSE 每连接独立计时**：Workers 没有常驻进程，`/api/sse` 用 `stream.sleep(1000)` 在连接内循环推 `tick`，因此没有共享环形缓冲，断线不补发；`/api/sse/push` 与 `/api/sse/kick` 返回 `ok: false` 并在页面上提示不可用。
- **`/api/info` 的字段**：`node` 固定 `cloudflare-workers`，`pid` / `port` 为 `null`；`uptimeMs` 按 isolate 内的 `STARTED` 估算，实例重启会归零。线上时区是 UTC，本地 miniflare 跟随本机时区。

## 跨域：SSE 会被拦、WebSocket 不会

- **WebSocket 不走同源策略**。握手是一次 HTTP Upgrade，但升级后的帧不受 CORS 管辖，浏览器也不会做预检；`Origin` 只是发给服务端自己判断，服务端不拒绝就能连。所以从任何页面连 `wss://hono-lab.mttd.ipv64.net/ws` 都不会报跨域。
- **SSE 是一条普通 HTTP GET**（`text/event-stream`），走 fetch 的 `cors` 模式，响应没有 `Access-Control-Allow-Origin` 就被浏览器直接拦下。断线重连时浏览器还会带上 `Last-Event-ID`，这个头不在 CORS 安全列表里，会再触发一次预检。
- 两套入口都挂了 `hono/cors`：放行 `*`、方法与 `Content-Type` / `Last-Event-ID`，并暴露 `x-server-time`。
- `Access-Control-Allow-Origin: *` 不能和 `Access-Control-Allow-Credentials: true` 共存。如果要跨站带 Cookie 订阅，得把 origin 改成回显具体域名并显式允许凭据。
- WebSocket 侧目前没有校验 `Origin`，等于任何站点都能连；真要对外，应在 `onOpen` 里比对 `c.req.header('Origin')` 或 `ws.raw` 的握手头。

## 已知边界

- Node 侧 `serve()` 与 `injectWebSocket()` 必须按顺序调用，否则 `/ws` 会 404 或握手失败。
- `@hono/node-ws@1.3` 的 peer 依赖是 `@hono/node-server@^2`，装成 1.x 会 ERESOLVE。
- 时间偏移的精度受浏览器与服务端网络往返限制，跨机房时误差会放大到 RTT/2 量级；这不是 NTP，只适合做粗略校验。
- SSE 必须关缓冲（响应头 `cache-control: no-cache, no-transform` + `x-accel-buffering: no`），否则中间层会把事件攒着一起吐；HTTP/1.1 下同源并发连接上限 6 个，订阅开太多会挤住同域普通请求。
- `streamSSE` 的处理器要一直挂到客户端断开（本项目用 `stream.onAbort` 等 promise resolve），否则连接会被当成写完直接关掉。
- 浏览器不许 https 页面连 `ws://`（混合内容），这类地址要换成 `wss://`；WebSocket API 也不暴露握手响应头，协商出的子协议与实际压缩扩展在页面上看不到。
