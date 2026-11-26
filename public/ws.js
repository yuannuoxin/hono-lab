(() => {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const logEl = $('#log');
  const stateEl = $('#state');
  const endpointEl = $('#endpoint');
  const toggleBtn = $('#toggle');
  const autoBox = $('#autoReconnect');

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
  let reconnectTimer = null;
  let uptimeTimer = null;
  let sentCount = 0;
  let recvCount = 0;
  let rttSamples = [];

  const endpoint = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`;
  endpointEl.textContent = endpoint;

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
    stateEl.className = `pill ${kind}`;
    stateEl.innerHTML = `<i class="dot"></i>${label}`;
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

  function connect() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

    wantOpen = true;
    toggleBtn.textContent = '断开';
    setState('warn', '连接中');
    log('sys', `→ connect ${endpoint}`);

    ws = new WebSocket(endpoint);
    ws.binaryType = 'arraybuffer';

    ws.addEventListener('open', () => {
      openedAt = performance.now();
      setState('ok', '已连接');
      log('sys', '✓ handshake done，连接已建立');
      startUptime();
      // 连上先自动打一发，方便立刻看到 RTT 与回声是否正常
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

      if (wantOpen && autoBox.checked) {
        reconnectTimer = setTimeout(() => {
          log('sys', '… 自动重连');
          connect();
        }, 1500);
      } else {
        wantOpen = false;
        toggleBtn.textContent = '连接';
      }
    });

    ws.addEventListener('error', () => {
      log('error', '! WebSocket 错误（服务未启动或握手被拒）');
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
    if (send(body)) log('out', `→ ping id=${id}${echo ? ` echo=${echo}` : ''}`);
  }

  /* 交互 */

  toggleBtn.addEventListener('click', () => {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) disconnect();
    else connect();
  });

  $('#sendText').addEventListener('click', () => {
    const value = $('#textInput').value;
    if (send(value)) log('out', `→ text ${Buffer2Str(value)}`);
  });

  function Buffer2Str(v) {
    return `${v.length}B ${v}`;
  }

  $('#sendBinary').addEventListener('click', () => {
    const buf = new Uint8Array(32);
    crypto.getRandomValues(buf);
    if (send(buf)) log('out', `→ binary 32B`);
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

  log('sys', `页面就绪，目标 ${endpoint}`);
  connect();
})();
