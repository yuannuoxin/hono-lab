(() => {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const clockEl = $('#clock');
  const clockSub = $('#clockSub');
  const stateEl = $('#syncState');
  const tzEl = $('#serverTz');
  const offsetEl = $('#statOffset');
  const rttEl = $('#statRtt');
  const localEl = $('#statLocal');
  const samplesBody = $('#samples');
  const autoBox = $('#autoSync');
  const calibrateBtn = $('#calibrate');
  const segEl = $('#channel');

  const LABEL = { http: 'HTTP', ws: 'WebSocket', sse: 'SSE' };
  const COUNT = { http: 7, ws: 7, sse: 6 };

  const pad = (n, w = 2) => String(n).padStart(w, '0');
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  let channel = 'ws';
  let offsetMs = 0;
  let bestRtt = null;
  let synced = false;
  let calibrating = false;
  let autoTimer = null;

  /* ---------------- 三种采样通道 ---------------- */

  async function sampleHttp() {
    const t0 = Date.now();
    const res = await fetch('/api/time', { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    const t3 = Date.now();

    return {
      t0,
      t3,
      server: body.t,
      tz: body.tz,
      offsetMinutes: body.offsetMinutes,
      rtt: t3 - t0,
      offset: body.t - (t0 + t3) / 2,
    };
  }

  async function withTimeSocket(run) {
    const url = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`;
    const socket = new WebSocket(url);

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('WebSocket 连接超时')), 3000);
      socket.addEventListener('open', () => {
        clearTimeout(timer);
        resolve();
      }, { once: true });
      socket.addEventListener('error', () => {
        clearTimeout(timer);
        reject(new Error('WebSocket 连不上（服务没起或被拦）'));
      }, { once: true });
    });

    try {
      return await run(socket);
    } finally {
      try {
        socket.close();
      } catch {
        /* 已经关了 */
      }
    }
  }

  function sampleWsOnce(socket) {
    const id = Math.random().toString(36).slice(2, 8);
    const t0 = Date.now();

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        socket.removeEventListener('message', onMessage);
        reject(new Error('等服务器 tS 超时'));
      }, 2000);

      function onMessage(event) {
        if (typeof event.data !== 'string') return;
        let msg = null;
        try {
          msg = JSON.parse(event.data);
        } catch {
          return;
        }
        if (msg?.type !== 'time' || msg.id !== id) return;

        clearTimeout(timer);
        socket.removeEventListener('message', onMessage);
        const t3 = Date.now();
        resolve({
          t0,
          t3,
          server: msg.tS,
          tz: msg.tz,
          offsetMinutes: msg.offsetMinutes,
          rtt: t3 - t0,
          offset: msg.tS - (t0 + t3) / 2,
        });
      }

      socket.addEventListener('message', onMessage);
      socket.send(JSON.stringify({ type: 'time', id, t0 }));
    });
  }

  function sampleSseRound(n) {
    return new Promise((resolve, reject) => {
      const stream = new EventSource('/api/time/sse');
      const rows = [];

      const finish = (error) => {
        clearTimeout(timer);
        stream.close();
        if (error) reject(error);
        else resolve(rows);
      };

      const timer = setTimeout(() => finish(new Error(`只收到 ${rows.length}/${n} 个事件`)), 4000);

      stream.addEventListener('tick', (event) => {
        let data = null;
        try {
          data = JSON.parse(event.data);
        } catch {
          return;
        }
        const t3 = Date.now();
        rows.push({
          t0: null,
          t3,
          server: data.t,
          tz: data.tz,
          offsetMinutes: data.offsetMinutes,
          rtt: null,
          // 没有 t0，只能算 tS − t3，结果里含着服务端→客户端的单向延迟
          offset: data.t - t3,
        });
        if (rows.length >= n) finish();
      });

      stream.addEventListener('error', () => {
        /* 浏览器自己会按 retry 重连，这里继续等事件 */
      });
    });
  }

  /* ---------------- 渲染 ---------------- */

  function renderClock() {
    const now = Date.now() + offsetMs;
    const d = new Date(now);
    const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    const ms = pad(d.getMilliseconds(), 3);
    clockEl.innerHTML = `${time}<span class="frac">.${ms}</span>`;

    const caveat = channel === 'sse' ? '（下限，含单向延迟）' : '';
    clockSub.textContent =
      `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} · ${Intl.DateTimeFormat().resolvedOptions().timeZone}` +
      ` · 偏差 ${offsetMs >= 0 ? '+' : ''}${Math.round(offsetMs)} ms · ${LABEL[channel]}${caveat}`;
  }

  function renderStats(sample) {
    const signed = `${offsetMs >= 0 ? '+' : ''}${offsetMs.toFixed(1)} ms`;
    offsetEl.textContent = synced ? (channel === 'sse' ? `≈ ${signed}` : signed) : '—';
    offsetEl.style.color = !synced ? '' : Math.abs(offsetMs) < 50 ? '#86efac' : Math.abs(offsetMs) < 500 ? '#fcd34d' : '#fda4af';

    rttEl.innerHTML = bestRtt == null ? '—<small>单向</small>' : `${bestRtt}<small>ms</small>`;

    if (sample) {
      const d = new Date(sample.t3);
      localEl.textContent = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
    }
  }

  function renderTable(round, best) {
    samplesBody.innerHTML = round
      .map((s, i) => {
        const cls = s === best ? 'best' : '';
        const sign = s.offset >= 0 ? '+' : '';
        return `<tr class="${cls}">
          <td>${i + 1}${s === best ? ' · 最优' : ''}</td>
          <td>${s.t0 ?? '—'}</td>
          <td>${s.server}</td>
          <td>${s.t3}</td>
          <td>${s.rtt == null ? '—' : `${s.rtt} ms`}</td>
          <td>${sign}${s.offset.toFixed(1)} ms</td>
        </tr>`;
      })
      .join('');
  }

  /* ---------------- 校准 ---------------- */

  async function calibrate(n = COUNT[channel]) {
    if (calibrating) return;
    calibrating = true;
    calibrateBtn.disabled = true;
    if (!synced) {
      stateEl.className = 'pill warn';
      stateEl.innerHTML = `<i class="dot"></i>采样中 · ${LABEL[channel]}`;
    }

    let round = [];
    try {
      if (channel === 'http') {
        for (let i = 0; i < n; i += 1) {
          round.push(await sampleHttp());
          if (i < n - 1) await sleep(120);
        }
      } else if (channel === 'ws') {
        round = await withTimeSocket(async (socket) => {
          const rows = [];
          for (let i = 0; i < n; i += 1) {
            rows.push(await sampleWsOnce(socket));
            if (i < n - 1) await sleep(120);
          }
          return rows;
        });
      } else {
        round = await sampleSseRound(n);
      }
    } catch (error) {
      stateEl.className = 'pill bad';
      stateEl.innerHTML = '<i class="dot"></i>采样失败';
      clockSub.textContent = `${LABEL[channel]}：${error.message ?? error}`;
      calibrating = false;
      calibrateBtn.disabled = false;
      return;
    }

    // HTTP / WS：往返最短的那次最接近零延迟；SSE 没有往返，取 tS − t3 最大的那次（单向延迟最小）
    const best =
      channel === 'sse'
        ? round.reduce((a, b) => (a.offset >= b.offset ? a : b))
        : round.reduce((a, b) => (a.rtt <= b.rtt ? a : b));

    offsetMs = best.offset;
    bestRtt = best.rtt;
    synced = true;

    if (best.tz) {
      tzEl.textContent = `tz ${best.tz} (UTC${best.offsetMinutes >= 0 ? '+' : ''}${best.offsetMinutes / 60})`;
    }
    stateEl.className = 'pill ok';
    stateEl.innerHTML = `<i class="dot"></i>已校准 · ${round.length} 次 · ${LABEL[channel]}`;

    renderTable(round, best);
    renderStats(best);
    renderClock();

    calibrating = false;
    calibrateBtn.disabled = false;
  }

  /* ---------------- 交互 ---------------- */

  calibrateBtn.addEventListener('click', () => calibrate());

  segEl.addEventListener('click', async (event) => {
    const button = event.target.closest('button');
    if (!button) return;
    const next = button.dataset.channel;
    if (!LABEL[next] || next === channel) return;

    channel = next;
    for (const item of segEl.querySelectorAll('button')) {
      item.classList.toggle('on', item.dataset.channel === next);
    }

    synced = false;
    offsetMs = 0;
    bestRtt = null;
    offsetEl.textContent = '—';
    offsetEl.style.color = '';
    rttEl.innerHTML = '—<small>ms</small>';
    clockEl.textContent = '—';
    clockSub.textContent = `通道切到 ${LABEL[next]}，重新采样…`;
    samplesBody.innerHTML = '<tr><td colspan="6" class="dim" style="text-align: left">正在采样…</td></tr>';
    calibrateBtn.textContent = `重新校准（${COUNT[next]} 次采样）`;

    while (calibrating) await sleep(100);
    calibrate();
  });

  autoBox.addEventListener('change', () => {
    if (autoTimer) {
      clearInterval(autoTimer);
      autoTimer = null;
    }
    if (autoBox.checked) autoTimer = setInterval(() => calibrate(), 5000);
  });

  // 秒表：按偏移推算，60ms 刷新足够看清毫秒跳动
  setInterval(() => {
    if (!synced) return;
    renderClock();
  }, 60);

  calibrate().then(() => {
    if (autoBox.checked) autoTimer = setInterval(() => calibrate(), 5000);
  });
})();
