import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { createNodeWebSocket } from '@hono/node-ws';
import { readFile } from 'node:fs/promises';
import { extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const PUBLIC = fileURLToPath(new URL('../public', import.meta.url));
const PORT = Number(process.env.PORT ?? 3100);
const STARTED = Date.now();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
};

const PAGES = {
  '/': 'index.html',
  '/websocket': 'ws.html',
  '/time': 'time.html',
  '/styles.css': 'styles.css',
  '/ws.js': 'ws.js',
  '/time.js': 'time.js',
};

const app = new Hono();
const { injectWebSocket, upgradeWebSocket } = createNodeWebSocket({ app });

/* ---------------- 静态页面 ---------------- */

for (const [route, file] of Object.entries(PAGES)) {
  app.get(route, async (c) => {
    try {
      const body = await readFile(join(PUBLIC, file));
      return c.body(body, 200, {
        'content-type': MIME[extname(file)] ?? 'application/octet-stream',
        'cache-control': 'no-store',
      });
    } catch {
      return c.text('Not Found', 404);
    }
  });
}

/* ---------------- 服务端时间接口 ---------------- */

app.get('/api/time', (c) => {
  // 尽量贴近"开始处理请求"的时刻取时间，减小处理耗时带来的偏差
  const now = Date.now();
  c.header('x-server-time', String(now));
  return c.json({
    t: now,
    iso: new Date(now).toISOString(),
    tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
    offsetMinutes: -new Date(now).getTimezoneOffset(),
    uptimeMs: now - STARTED,
  });
});

app.get('/api/info', (c) =>
  c.json({
    node: process.version,
    pid: process.pid,
    port: PORT,
    uptimeMs: Date.now() - STARTED,
    clock: Date.now(),
    tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
  }),
);

/* ---------------- WebSocket 测试端点 ---------------- */

const clients = new Set();
let tickTimer = null;

function broadcast(payload) {
  const text = JSON.stringify(payload);
  for (const client of clients) {
    try {
      client.send(text);
    } catch {
      clients.delete(client);
    }
  }
}

app.get(
  '/ws',
  upgradeWebSocket(() => ({
    onOpen(_event, ws) {
      clients.add(ws);

      ws.send(
        JSON.stringify({
          type: 'hello',
          t: Date.now(),
          id: Math.random().toString(36).slice(2, 8),
          clients: clients.size,
        }),
      );

      // 每秒向所有连接推送一次服务器时间，用来验证服务端主动下发
      if (!tickTimer) {
        tickTimer = setInterval(() => {
          broadcast({ type: 'tick', t: Date.now(), clients: clients.size });
        }, 1000);
      }
    },

    onMessage(event, ws) {
      const t1 = Date.now();

      if (typeof event.data === 'string') {
        let payload = null;
        try {
          payload = JSON.parse(event.data);
        } catch {
          /* 非 JSON 文本，走普通回声 */
        }

        if (payload?.type === 'ping') {
          ws.send(JSON.stringify({ type: 'pong', id: payload.id, t0: payload.t0, t1, echo: payload.echo ?? null }));
          return;
        }

        ws.send(
          JSON.stringify({
            type: 'echo',
            t1,
            bytes: Buffer.byteLength(event.data),
            data: event.data,
          }),
        );
        return;
      }

      // 二进制帧：原样回声，再补一条元信息
      const bytes = event.data?.byteLength ?? event.data?.length ?? 0;
      ws.send(event.data);
      ws.send(JSON.stringify({ type: 'echo-binary', t1, bytes }));
    },

    onClose(_event, ws) {
      clients.delete(ws);
      if (!clients.size && tickTimer) {
        clearInterval(tickTimer);
        tickTimer = null;
      }
    },

    onError(_event, ws) {
      clients.delete(ws);
    },
  })),
);

/* ---------------- 启动 ---------------- */

const server = serve({ fetch: app.fetch, port: PORT }, (info) => {
  console.log(`hono-lab 已启动 → http://127.0.0.1:${info.port}`);
  console.log(`  WebSocket 测试  http://127.0.0.1:${info.port}/websocket`);
  console.log(`  服务端时间测试  http://127.0.0.1:${info.port}/time`);
});

injectWebSocket(server);
