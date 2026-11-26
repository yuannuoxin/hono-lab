import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { createNodeWebSocket } from '@hono/node-ws';
import { streamSSE } from 'hono/streaming';
import { cors } from 'hono/cors';
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
  '/sse': 'sse.html',
  '/styles.css': 'styles.css',
  '/ws.js': 'ws.js',
  '/time.js': 'time.js',
  '/sse.js': 'sse.js',
};

const app = new Hono();
const { injectWebSocket, upgradeWebSocket } = createNodeWebSocket({ app });

// SSE 是普通 HTTP GET，跨源订阅要靠服务端放行；WebSocket 不吃这套，它自己校验 Origin
app.use(
  '*',
  cors({
    origin: '*',
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    // 浏览器断线重连时会带 Last-Event-ID，它不在 CORS 安全列表里，必须显式放行
    allowHeaders: ['Content-Type', 'Last-Event-ID'],
    exposeHeaders: ['x-server-time'],
    maxAge: 600,
  }),
);

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

// 给时间页的 SSE 通道用：每个连接自己的快节拍推送，不参与 /api/sse 的广播
app.get('/api/time/sse', (c) => {
  c.header('cache-control', 'no-cache, no-transform');
  c.header('x-accel-buffering', 'no');

  return streamSSE(c, async (stream) => {
    await stream.writeSSE({ event: 'hello', retry: 1000, data: JSON.stringify({ t: Date.now() }) });

    while (true) {
      await stream.sleep(80);
      await stream.writeSSE({
        event: 'tick',
        data: JSON.stringify({
          t: Date.now(),
          tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
          offsetMinutes: -new Date().getTimezoneOffset(),
        }),
      });
    }
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

        if (payload?.type === 'time') {
          // 时间页的 WebSocket 通道：把收到时刻和 t0 一起带回，客户端算 offset
          const tS = Date.now();
          ws.send(
            JSON.stringify({
              type: 'time',
              id: payload.id,
              t0: payload.t0,
              tS,
              tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
              offsetMinutes: -new Date(tS).getTimezoneOffset(),
            }),
          );
          return;
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

/* ---------------- SSE 测试端点 ---------------- */

const sseClients = new Map(); // cid -> { write }
let sseSeq = 0;
const SSE_RING = [];
const SSE_RING_MAX = 120;
let sseTimer = null;
let sseIdleTimer = null;

function ssePush(event, payload) {
  sseSeq += 1;
  const record = { id: String(sseSeq), event, data: JSON.stringify(payload) };
  SSE_RING.push(record);
  if (SSE_RING.length > SSE_RING_MAX) SSE_RING.shift();

  for (const [cid, client] of sseClients) {
    client.write(record).catch(() => {
      sseClients.delete(cid);
    });
  }
  return record;
}

function ensureSseTimer() {
  if (sseIdleTimer) {
    clearTimeout(sseIdleTimer);
    sseIdleTimer = null;
  }
  if (sseTimer) return;
  sseTimer = setInterval(() => {
    ssePush('tick', { t: Date.now(), clients: sseClients.size });
  }, 1000);
}

// 最后一个人走光后再撑 15 秒，让重连期间漏掉的事件还能补发
function scheduleSseIdleStop() {
  if (sseClients.size || sseIdleTimer) return;
  sseIdleTimer = setTimeout(() => {
    sseIdleTimer = null;
    if (sseClients.size || !sseTimer) return;
    clearInterval(sseTimer);
    sseTimer = null;
  }, 15000);
}

app.get('/api/sse', (c) => {
  const cid = Math.random().toString(36).slice(2, 8);
  const lastId = c.req.header('last-event-id') ?? '';
  const t0 = Date.now();

  // 关掉各层缓冲，否则事件会被攒着一起吐出来
  c.header('cache-control', 'no-cache, no-transform');
  c.header('x-accel-buffering', 'no');

  return streamSSE(c, async (stream) => {
    const client = {
      write: (record) => stream.writeSSE({ event: record.event, id: record.id, data: record.data }),
      abort: () => stream.abort(),
    };
    sseClients.set(cid, client);
    ensureSseTimer();

    // hello 不带 id，避免把浏览器的 Last-Event-ID 基准顶到最新
    await stream.writeSSE({
      event: 'hello',
      retry: 3000,
      data: JSON.stringify({
        cid,
        clients: sseClients.size,
        seq: sseSeq,
        resumedFrom: lastId,
        t: t0,
      }),
    });

    // 浏览器重连时会自动带上 Last-Event-ID，按它把断线期间的事件补回去
    if (lastId) {
      const index = SSE_RING.findIndex((record) => record.id === lastId);
      if (index >= 0) {
        for (const record of SSE_RING.slice(index + 1)) {
          await client.write(record);
        }
      } else {
        await stream.writeSSE({
          event: 'gap',
          data: JSON.stringify({ lastId, seq: sseSeq, note: '这个 id 已经滚出缓冲区，补不回来了' }),
        });
      }
    }

    await new Promise((resolve) => {
      stream.onAbort(() => resolve());
    });

    sseClients.delete(cid);
    scheduleSseIdleStop();
  });
});

app.post('/api/sse/push', async (c) => {
  let text = '手动触发';
  let burst = 1;
  try {
    const body = await c.req.json();
    if (body?.text) text = String(body.text).slice(0, 200);
    if (Number(body?.burst) > 1) burst = Math.min(Number(body.burst), 10);
  } catch {
    /* 没有 JSON body 就用默认文案 */
  }

  const ids = [];
  for (let i = 0; i < burst; i += 1) {
    const record = ssePush('notice', { text: burst > 1 ? `${text} (${i + 1}/${burst})` : text, t: Date.now() });
    ids.push(record.id);
  }

  return c.json({ ok: true, ids, clients: sseClients.size, seq: sseSeq });
});

app.post('/api/sse/kick', (c) => {
  const kicked = [];
  for (const [cid, client] of sseClients) {
    kicked.push(cid);
    try {
      client.abort();
    } catch {
      /* 已经断开的就跳过 */
    }
  }
  sseClients.clear();
  scheduleSseIdleStop();
  return c.json({ ok: true, kicked });
});

app.get('/api/sse/info', (c) =>
  c.json({
    clients: sseClients.size,
    seq: sseSeq,
    ring: SSE_RING.length,
    timer: Boolean(sseTimer),
    subscriberIds: [...sseClients.keys()],
  }),
);

/* ---------------- 启动 ---------------- */

const server = serve({ fetch: app.fetch, port: PORT }, (info) => {
  console.log(`hono-lab 已启动 → http://127.0.0.1:${info.port}`);
  console.log(`  WebSocket 测试  http://127.0.0.1:${info.port}/websocket`);
  console.log(`  服务端时间测试  http://127.0.0.1:${info.port}/time`);
  console.log(`  SSE 测试       http://127.0.0.1:${info.port}/sse`);
});

injectWebSocket(server);
