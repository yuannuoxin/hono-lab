(() => {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const logEl = $('#log');
  const stateEl = $('#state');
  const subsEl = $('#subs');
  const endpointEl = $('#endpoint');

  const stat = {
    recv: $('#statRecv'),
    tick: $('#statTick'),
    notice: $('#statNotice'),
    reconnect: $('#statReconnect'),
    lastId: $('#statLastId'),
    up: $('#statUp'),
  };

  const endpoint = '/api/sse';
  endpointEl.textContent = `GET ${endpoint} · event-stream`;

  let es = null;
  let openedAt = 0;
  let uptimeTimer = null;
  let helloSeq = 0;
  const counters = { recv: 0, tick: 0, notice: 0, reconnect: 0 };

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

  function refresh() {
    stat.recv.textContent = counters.recv;
    stat.tick.textContent = counters.tick;
    stat.notice.textContent = counters.notice;
    stat.reconnect.textContent = counters.reconnect;
  }

  function startUptime() {
    if (uptimeTimer) return;
    uptimeTimer = setInterval(() => {
      if (!openedAt) return;
      const sec = Math.floor((performance.now() - openedAt) / 1000);
      stat.up.textContent =
        sec < 60 ? `${sec}s` : `${Math.floor(sec / 60)}m ${String(sec % 60).padStart(2, '0')}s`;
    }, 1000);
  }

  function onEvent(name, event) {
    counters.recv += 1;
    if (name === 'tick') counters.tick += 1;
    if (name === 'notice') counters.notice += 1;
    if (event.lastEventId) stat.lastId.textContent = event.lastEventId;

    let data = null;
    try {
      data = JSON.parse(event.data);
    } catch {
      data = event.data;
    }

    const idPart = event.lastEventId ? ` id=${event.lastEventId}` : '';
    // 握手时记下 seq，比它小的 id 就是服务端按 Last-Event-ID 补发的
    const replay = name !== 'hello' && helloSeq && Number(event.lastEventId) <= helloSeq ? '↺ 续传 ' : '';

    switch (name) {
      case 'hello':
        helloSeq = Number(data?.seq) || 0;
        if (typeof data?.clients === 'number') subsEl.textContent = `订阅 ${data.clients}`;
        log(
          'sys',
          `← hello cid=${data?.cid} clients=${data?.clients} seq=${data?.seq}` +
            `${data?.resumedFrom ? ` resumedFrom=${data.resumedFrom}` : ''} retry=3s`,
        );
        break;

      case 'tick':
        log('in', `${replay}← tick${idPart} server=${new Date(data.t).toLocaleTimeString('zh-CN', { hour12: false })} clients=${data.clients}`);
        break;

      case 'notice':
        log('in', `${replay}← notice${idPart} ${data.text}`);
        break;

      case 'gap':
        log('error', `! gap${idPart} ${data?.note ?? ''}（lastId=${data?.lastId}）`);
        break;

      default:
        log('in', `${replay}← ${name}${idPart} ${String(event.data).slice(0, 120)}`);
    }

    refresh();
  }

  function connect() {
    if (es) return;

    setState('warn', '订阅中…');
    log('sys', `→ GET ${endpoint}`);
    toggleBtn.textContent = '断开';

    es = new EventSource(endpoint);

    es.addEventListener('open', () => {
      if (!openedAt) openedAt = performance.now();
      setState('ok', '已订阅');
      log('sys', '✓ open，连接已建立');
      startUptime();
    });

    es.addEventListener('error', () => {
      if (!es) return;
      if (es.readyState === EventSource.CLOSED) {
        setState('bad', '已断开');
        log('error', '✕ 连接关闭，浏览器不再自动重试');
        return;
      }
      counters.reconnect += 1;
      refresh();
      setState('warn', '重连中…');
      log('warn', '… 连接中断，浏览器按 retry=3s 自动重连');
    });

    es.addEventListener('message', (event) => onEvent('message', event));
    for (const name of ['hello', 'tick', 'notice', 'gap']) {
      es.addEventListener(name, (event) => onEvent(name, event));
    }
  }

  function disconnect() {
    if (!es) return;
    log('sys', '… 手动关闭 EventSource');
    es.close();
    es = null;
    setState('bad', '已手动断开');
    toggleBtn.textContent = '重新订阅';
    subsEl.textContent = '订阅 —';
  }

  const toggleBtn = $('#toggle');

  toggleBtn.addEventListener('click', () => {
    if (es) disconnect();
    else connect();
  });

  async function push(text, burst) {
    log('sys', `→ POST /api/sse/push burst=${burst}`);
    try {
      const res = await fetch('/api/sse/push', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ text, burst }),
      });
      const json = await res.json();
      log('out', `→ 已投递 ${json.ids.join(', ')}，当前订阅 ${json.clients}`);
    } catch {
      log('error', '! 推送失败（服务未启动？）');
    }
  }

  $('#push').addEventListener('click', () => push($('#pushText').value || '手动触发', 1));
  $('#burst').addEventListener('click', () => push($('#pushText').value || '手动触发', 5));

  $('#kick').addEventListener('click', async () => {
    log('sys', '… POST /api/sse/kick，让服务端断开所有流');
    try {
      const res = await fetch('/api/sse/kick', { method: 'POST' });
      const json = await res.json();
      log('sys', `… 已踢掉 ${json.kicked.length} 条流，等浏览器自己重连并带上 Last-Event-ID`);
    } catch {
      log('error', '! kick 失败');
    }
  });

  $('#clear').addEventListener('click', () => {
    logEl.innerHTML = '';
  });

  log('sys', `页面就绪，目标 ${endpoint}`);
  connect();
})();
