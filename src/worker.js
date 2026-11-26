import { Hono } from 'hono';
import { streamSSE } from 'hono/streaming';
import { cors } from 'hono/cors';

const STARTED = Date.now();

const app = new Hono();

// SSE 走普通 HTTP，跨源必须由服务端回 ACAO；WebSocket 不看这个，只认自己校验的 Origin
app.use(
  '*',
  cors({
    origin: '*',
    allowMethods: ['GET', 'POST', 'OPTIONS'],
    allowHeaders: ['Content-Type', 'Last-Event-ID'],
    exposeHeaders: ['x-server-time'],
    maxAge: 600,
  }),
);

// 路径 → 静态文件，Workers 没有文件系统，统一走 ASSETS 绑定
const PAGES = {
  '/': '/index.html',
  '/websocket': '/ws.html',
  '/time': '/time.html',
  '/sse': '/sse.html',
};

/* ---------------- 服务端时间接口 ---------------- */

app.get('/api/time', (c) => {
  const now = Date.now();
  c.header('x-server-time', String(now));
  return c.json({
    t: now,
    iso: new Date(now).toISOString(),
    tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
    offsetMinutes: -new Date(now).getTimezoneOffset(),
    uptimeMs: now - STARTED,
    runtime: 'cloudflare-workers',
  });
});

// 时间页的 SSE 通道：连接内自跑快节拍
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
    node: 'cloudflare-workers',
    pid: null,
    port: null,
    uptimeMs: Date.now() - STARTED,
    clock: Date.now(),
    tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
    runtime: 'cloudflare-workers',
  }),
);

/* ---------------- WebSocket（Workers 原生 WebSocketPair） ---------------- */

// 仅在当前实例内有效：Workers 横向扩多个 isolate，跨实例广播需要 Durable Object
const clients = new Set();

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

function attachSession(server) {
  clients.add(server);
  let tickTimer = null;

  const sendJSON = (payload) => {
    try {
      server.send(JSON.stringify(payload));
    } catch {
      clients.delete(server);
    }
  };

  sendJSON({
    type: 'hello',
    t: Date.now(),
    id: Math.random().toString(36).slice(2, 8),
    clients: clients.size,
    runtime: 'cloudflare-workers',
  });

  // 连接存活期间每秒钟推一次；实例被回收会连带停掉，见 README 的已知边界
  tickTimer = setInterval(() => {
    broadcast({ type: 'tick', t: Date.now(), clients: clients.size });
  }, 1000);

  const cleanup = () => {
    clients.delete(server);
    if (tickTimer) {
      clearInterval(tickTimer);
      tickTimer = null;
    }
  };

  server.addEventListener('message', (event) => {
    const t1 = Date.now();
    const data = event.data;

    if (typeof data === 'string') {
      let payload = null;
      try {
        payload = JSON.parse(data);
      } catch {
        /* 非 JSON 文本，走普通回声 */
      }

      if (payload?.type === 'time') {
        const tS = Date.now();
        sendJSON({
          type: 'time',
          id: payload.id,
          t0: payload.t0,
          tS,
          tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
          offsetMinutes: -new Date(tS).getTimezoneOffset(),
        });
        return;
      }

      if (payload?.type === 'ping') {
        sendJSON({ type: 'pong', id: payload.id, t0: payload.t0, t1, echo: payload.echo ?? null });
        return;
      }

      const bytes = new TextEncoder().encode(data).byteLength;
      sendJSON({ type: 'echo', t1, bytes, data });
      return;
    }

    const bytes = data?.byteLength ?? data?.length ?? 0;
    try {
      server.send(data);
    } catch {
      clients.delete(server);
    }
    sendJSON({ type: 'echo-binary', t1, bytes });
  });

  server.addEventListener('close', cleanup);
  server.addEventListener('error', cleanup);
}

app.get('/ws', (c) => {
  if (c.req.header('Upgrade') !== 'websocket') {
    return c.text('Expected: Upgrade: websocket', 426);
  }

  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];

  server.accept();
  attachSession(server);

  return new Response(null, { status: 101, webSocket: client });
});

/* ---------------- SSE ---------------- */

// 每个连接自带计时循环；Workers 没有常驻进程，因此没有跨连接的环形缓冲
app.get('/api/sse', (c) => {
  const cid = Math.random().toString(36).slice(2, 8);
  const lastId = c.req.header('last-event-id') ?? '';

  c.header('cache-control', 'no-cache, no-transform');
  c.header('x-accel-buffering', 'no');

  return streamSSE(c, async (stream) => {
    let seq = Number(lastId) || 0;

    await stream.writeSSE({
      event: 'hello',
      retry: 3000,
      data: JSON.stringify({
        cid,
        clients: 1,
        seq,
        resumedFrom: lastId,
        t: Date.now(),
        runtime: 'cloudflare-workers',
        buffered: false,
      }),
    });

    if (lastId) {
      await stream.writeSSE({
        event: 'gap',
        data: JSON.stringify({
          lastId,
          seq,
          note: 'Workers 版每个连接独立计时，没有共享环形缓冲，断线期间的事件不补发',
        }),
      });
    }

    while (true) {
      await stream.sleep(1000);
      seq += 1;
      await stream.writeSSE({
        event: 'tick',
        id: String(seq),
        data: JSON.stringify({ t: Date.now(), clients: 1 }),
      });
    }
  });
});

app.post('/api/sse/push', (c) =>
  c.json(
    {
      ok: false,
      ids: [],
      clients: 0,
      reason: 'Workers 版没有跨连接通道，notice 需要 Durable Object 才能广播；本机 node 版可用',
    },
    200,
  ),
);

app.post('/api/sse/kick', (c) => c.json({ ok: false, kicked: [], reason: 'Workers 版无法从外部掐断别人的流' }, 200));

app.get('/api/sse/info', (c) => c.json({ clients: 0, seq: 0, ring: 0, timer: false, runtime: 'cloudflare-workers' }));

/* ---------------- 静态资源 ---------------- */

app.get('*', async (c) => {
  const pathname = new URL(c.req.url).pathname;
  const assetPath = PAGES[pathname] ?? pathname;

  const assetUrl = new URL(c.req.url);
  assetUrl.pathname = assetPath;

  const res = await c.env.ASSETS.fetch(new URL(assetUrl.toString()));
  if (res.status === 404) {
    const fallback = await c.env.ASSETS.fetch(new URL('/index.html', c.req.url));
    return new Response(fallback.body, { status: 404, headers: { 'content-type': 'text/html; charset=utf-8' } });
  }
  return res;
});

export default app;
