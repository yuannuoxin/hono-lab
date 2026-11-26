# Hono Lab · WebSocket 与时间测试台

一个 Hono（Node 运行时）小项目，带两个自测页面：**WebSocket 回声测试**和**服务端时间偏移测量**。没有前端构建步骤，页面是原生 HTML/CSS/JS。

## 运行

```bash
npm install
npm run dev      # node --watch，端口 3100
```

打开 http://127.0.0.1:3100

| 路径 | 说明 |
| --- | --- |
| `/` | 首页，两页入口 + 服务信息 |
| `/websocket` | WebSocket 测试页 |
| `/time` | 服务端时间测试页 |
| `/ws` | WebSocket 端点 |
| `/api/time` | 服务器时间（JSON，含 `x-server-time` 响应头） |
| `/api/info` | Node 版本、pid、端口、运行时长 |

## 技术栈

- **hono** 4.x
- **@hono/node-server** 2.x（HTTP 服务）
- **@hono/node-ws** 1.3.x（`createNodeWebSocket` + `injectWebSocket`）
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

## 已知边界

- Node 侧 `serve()` 与 `injectWebSocket()` 必须按顺序调用，否则 `/ws` 会 404 或握手失败。
- `@hono/node-ws@1.3` 的 peer 依赖是 `@hono/node-server@^2`，装成 1.x 会 ERESOLVE。
- 时间偏移的精度受浏览器与服务端网络往返限制，跨机房时误差会放大到 RTT/2 量级；这不是 NTP，只适合做粗略校验。
