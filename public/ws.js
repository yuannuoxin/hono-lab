(() => {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const logEl = $('#log');
  const stateEl = $('#state');
  const toggleBtn = $('#toggle');
  const autoBox = $('#autoReconnect');
  const urlInput = $('#urlInput');

  const DEFAULT_URL = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`;
  const STORE_KEY = 'hono-lab:ws-url';

  const stat = {
    sent: $('#statSent'),
    recv: $('#statRecv'),
    rtt: $('#statRtt'),
    range: $('#statRange'),
    up: $('#statUp'),
    clients: $('#statClients'),
  };

  let ws = null;
  let wantOpen = false;
  let openedAt = 0;
  let currentUrl = '';
  let reconnectTimer = null;
  let uptimeTimer = null;
  let sentCount = 0;
  let recvCount = 0;
  let failCount = 0;
  let rttSamples = [];
  const pendingPings = new Map();

  const esc = (v) =>
    String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  function stamp() {
    const d = new Date();
    const p = (n, w = 2) => String(n).padStart(w, '0');
    return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
  }

  function log(kind, text) {
    const line = document.createElement('div');
    line.className = `line ${kind}`;
    line.innerHTML = `<span class="t">${stamp()}</span><span>${esc(text)}</span>`;
    logEl.appendChild(line);
    while (logEl.childElementCount > 300) logEl.firstElementChild.remove();
    logEl.scrollTop = logEl.scrollHeight;
  }

  function setState(kind, label) {
    stateEl.className = `pill ${kind}`.trim();
    stateEl.innerHTML = `<i class="dot"></i>${label}`;
  }

  /* 允许三种写法：ws://xxx、wss://xxx、直接写域名；http/https 自动换成 ws/wss */
  function normalize(raw) {
    let value = String(raw ?? '').trim();
    if (!value) return '';
    if (/^https?:\/\//i.test(value)) value = value.replace(/^http/i, 'ws');
    else if (!/^wss?:\/\//i.test(value)) value = `ws://${value}`;

    try {
      const url = new URL(value);
      if (!/^wss?:$/.test(url.protocol)) return '';
      return url.href;
    } catch {
      return '';
    }
  }

  function refreshStats() {
    stat.sent.textContent = sentCount;
    stat.recv.textContent = recvCount;

    if (!rttSamples.length) {
      stat.rtt.innerHTML = '—<small>ms</small>';
      stat.range.textContent = '—';
      return;
    }

    const avg = Math.round(rttSamples.reduce((a, b) => a + b, 0) / rttSamples.length);
    const min = Math.min(...rttSamples);
    const max = Math.max(...rttSamples);
    stat.rtt.innerHTML = `${avg}<small>ms</small>`;
    stat.range.textContent = `${min} – ${max} ms`;
  }

  function startUptime() {
    stopUptime();
    uptimeTimer = setInterval(() => {
      if (!openedAt) return;
      const sec = Math.floor((performance.now() - openedAt) / 1000);
      stat.up.textContent =
        sec < 60 ? `${sec}s` : `${Math.floor(sec / 60)}m ${String(sec % 60).padStart(2, '0')}s`;
    }, 1000);
  }

  function stopUptime() {
    if (uptimeTimer) clearInterval(uptimeTimer);
    uptimeTimer = null;
    stat.up.textContent = '—';
  }

  function connect(rawUrl) {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

    const target = normalize(rawUrl ?? urlInput.value);
    if (!target) {
      log('error', '! 地址不合法，需要 ws:// 或 wss://（也可以只写域名）');
      return;
    }

    currentUrl = target;
    urlInput.value = target;
    urlInput.disabled = true;
    wantOpen = true;
    toggleBtn.textContent = '断开';
    setState('warn', '连接中');
    log('sys', `→ connect ${target}`);

    try {
      ws = new WebSocket(target);
    } catch (error) {
      ws = null;
      urlInput.disabled = false;
      toggleBtn.textContent = '连接';
      wantOpen = false;
      setState('bad', '地址无效');
      log('error', `! 创建连接失败：${error.message}`);
      return;
    }

    ws.binaryType = 'arraybuffer';

    ws.addEventListener('open', () => {
      openedAt = performance.now();
      failCount = 0;
      setState('ok', '已连接');
      log('sys', '✓ handshake done，连接已建立');
      try {
        localStorage.setItem(STORE_KEY, target);
      } catch {
        /* 隐私模式下写不了，忽略 */
      }
      startUptime();
      // 连上先自动打一发，第三方纯回声服务也能立刻看到 RTT
      setTimeout(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
          log('sys', '… 自动探测');
          sendPing('auto');
        }
      }, 250);
    });

    ws.addEventListener('message', (event) => {
      recvCount += 1;

      if (typeof event.data !== 'string') {
        log('in', `← binary ${event.data.byteLength ?? event.data.length ?? 0}B`);
        refreshStats();
        return;
      }

      let msg = null;
      try {
        msg = JSON.parse(event.data);
      } catch {
        log('in', `← ${event.data}`);
        refreshStats();
        return;
      }

      // 第三方回声服务会把我们的 ping 原样吐回，按 id 认领算 RTT
      if (msg?.type === 'ping' && pendingPings.has(msg.id)) {
        const rtt = Date.now() - pendingPings.get(msg.id);
        pendingPings.delete(msg.id);
        rttSamples.push(rtt);
        if (rttSamples.length > 200) rttSamples.shift();
        log('in', `← ping 原样回声 id=${msg.id} rtt=${rtt}ms`);
        refreshStats();
        return;
      }

      switch (msg.type) {
        case 'hello':
          if (typeof msg.clients === 'number') stat.clients.textContent = msg.clients;
          log('sys', `← hello id=${msg.id} clients=${msg.clients} server=${new Date(msg.t).toISOString()}`);
          break;

        case 'tick':
          if (typeof msg.clients === 'number') stat.clients.textContent = msg.clients;
          log('in', `← tick server=${new Date(msg.t).toLocaleTimeString('zh-CN', { hour12: false })} clients=${msg.clients}`);
          break;

        case 'pong': {
          const rtt = Date.now() - msg.t0;
          pendingPings.delete(msg.id);
          rttSamples.push(rtt);
          if (rttSamples.length > 200) rttSamples.shift();
          log('in', `← pong id=${msg.id} rtt=${rtt}ms`);
          break;
        }

        case 'echo':
          log('in', `← echo ${msg.bytes}B ${String(msg.data).slice(0, 120)}`);
          break;

        case 'echo-binary':
          log('in', `← echo-binary ${msg.bytes}B`);
          break;

        default:
          log('in', `← ${event.data}`);
      }

      refreshStats();
    });

    ws.addEventListener('close', (event) => {
      setState('bad', `已断开 ${event.code}${event.reason ? ` · ${event.reason}` : ''}`);
      log('warn', `✕ 连接关闭 code=${event.code}${event.reason ? ` reason=${event.reason}` : ''}`);
      stopUptime();
      stat.clients.textContent = '—';
      openedAt = 0;
      ws = null;
      urlInput.disabled = false;
      pendingPings.clear();

      if (wantOpen && autoBox.checked) {
        failCount += 1;
        if (failCount >= 5) {
          wantOpen = false;
          toggleBtn.textContent = '连接';
          log('error', '! 连续 5 次没连上，已停止自动重连');
          return;
        }
        reconnectTimer = setTimeout(() => {
          log('sys', `… 自动重连（第 ${failCount} 次）`);
          connect(currentUrl);
        }, 1500);
      } else {
        wantOpen = false;
        toggleBtn.textContent = '连接';
      }
    });

    ws.addEventListener('error', () => {
      log('error', '! WebSocket 错误（地址不对、服务没起，或被 CORS/混合内容拦下）');
    });
  }

  function disconnect() {
    wantOpen = false;
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    toggleBtn.textContent = '连接';
    if (ws) {
      log('sys', '… 主动断开');
      ws.close(1000, 'client close');
    }
  }

  function send(payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      log('error', '! 未连接，无法发送');
      return false;
    }
    ws.send(typeof payload === 'string' ? payload : payload);
    sentCount += 1;
    refreshStats();
    return true;
  }

  let pingSeq = 0;
  function sendPing(echo) {
    const id = `${(++pingSeq).toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
    const t0 = Date.now();
    const body = JSON.stringify({ type: 'ping', id, t0, echo: echo ?? null });
    if (!send(body)) return;
    pendingPings.set(id, t0);
    if (pendingPings.size > 200) pendingPings.delete(pendingPings.keys().next().value);
    log('out', `→ ping id=${id}${echo ? ` echo=${echo}` : ''}`);
  }

  /* 交互 */

  toggleBtn.addEventListener('click', () => {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) disconnect();
    else connect();
  });

  urlInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) disconnect();
      connect();
    }
  });

  urlInput.addEventListener('change', () => {
    const normalized = normalize(urlInput.value);
    if (normalized) urlInput.value = normalized;
  });

  $('#sendText').addEventListener('click', () => {
    const value = $('#textInput').value;
    if (send(value)) log('out', `→ text ${value.length}B ${value}`);
  });

  $('#sendBinary').addEventListener('click', () => {
    const buf = new Uint8Array(32);
    crypto.getRandomValues(buf);
    if (send(buf)) log('out', '→ binary 32B');
  });

  $('#burst').addEventListener('click', () => {
    log('sys', '… 连发 20 次 ping');
    for (let i = 0; i < 20; i += 1) {
      setTimeout(() => sendPing(), i * 40);
    }
  });

  $('#clear').addEventListener('click', () => {
    logEl.innerHTML = '';
  });

  autoBox.addEventListener('change', () => {
    if (!autoBox.checked && reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
      wantOpen = false;
      toggleBtn.textContent = '连接';
    }
  });

  /* 启动 */

  let saved = '';
  try {
    saved = localStorage.getItem(STORE_KEY) ?? '';
  } catch {
    saved = '';
  }

  urlInput.value = saved || DEFAULT_URL;
  log('sys', `页面就绪，默认 ${DEFAULT_URL}`);

  if (!saved || saved === DEFAULT_URL) {
    connect(urlInput.value);
  } else {
    setState('', '未连接');
    log('sys', `读回上次的地址 ${saved}，点「连接」继续（第三方不通就填自己的）`);
  }
})();
