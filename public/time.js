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

  const pad = (n, w = 2) => String(n).padStart(w, '0');
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  let offsetMs = 0;
  let bestRtt = null;
  let synced = false;
  let calibrating = false;
  let autoTimer = null;
  let lastSyncAt = 0;

  async function sampleOnce() {
    const t0 = Date.now();
    const res = await fetch('/api/time', { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    const t3 = Date.now();

    return {
      t0,
      t3,
      server: body.t,
      iso: body.iso,
      tz: body.tz,
      offsetMinutes: body.offsetMinutes,
      rtt: t3 - t0,
      offset: body.t - (t0 + t3) / 2,
    };
  }

  function renderClock() {
    const now = Date.now() + offsetMs;
    const d = new Date(now);
    const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    const ms = pad(d.getMilliseconds(), 3);
    clockEl.innerHTML = `${time}<span class="frac">.${ms}</span>`;
    clockSub.textContent = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} · ${Intl.DateTimeFormat().resolvedOptions().timeZone} · 偏差 ${offsetMs >= 0 ? '+' : ''}${Math.round(offsetMs)} ms`;
  }

  function renderStats(sample) {
    const signed = `${offsetMs >= 0 ? '+' : ''}${offsetMs.toFixed(1)} ms`;
    offsetEl.textContent = synced ? signed : '—';
    offsetEl.style.color = !synced ? '' : Math.abs(offsetMs) < 50 ? '#86efac' : Math.abs(offsetMs) < 500 ? '#fcd34d' : '#fda4af';

    rttEl.innerHTML = bestRtt == null ? '—<small>ms</small>' : `${bestRtt}<small>ms</small>`;

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
          <td>${s.t0}</td>
          <td>${s.server}</td>
          <td>${s.t3}</td>
          <td>${s.rtt} ms</td>
          <td>${sign}${s.offset.toFixed(1)} ms</td>
        </tr>`;
      })
      .join('');
  }

  async function calibrate(n = 7) {
    if (calibrating) return;
    calibrating = true;
    calibrateBtn.disabled = true;
    if (!synced) {
      stateEl.className = 'pill warn';
      stateEl.innerHTML = '<i class="dot"></i>校准中…';
    }

    const round = [];
    for (let i = 0; i < n; i += 1) {
      try {
        round.push(await sampleOnce());
      } catch (error) {
        stateEl.className = 'pill bad';
        stateEl.innerHTML = `<i class="dot"></i>请求失败`;
        clockSub.textContent = String(error.message ?? error);
        calibrating = false;
        calibrateBtn.disabled = false;
        return;
      }
      if (i < n - 1) await sleep(120);
    }

    const best = round.reduce((a, b) => (a.rtt <= b.rtt ? a : b));
    offsetMs = best.offset;
    bestRtt = best.rtt;
    synced = true;
    lastSyncAt = Date.now();

    tzEl.textContent = `tz ${best.tz} (UTC${best.offsetMinutes >= 0 ? '+' : ''}${best.offsetMinutes / 60})`;
    stateEl.className = 'pill ok';
    stateEl.innerHTML = `<i class="dot"></i>已校准 · ${round.length} 次采样`;

    renderTable(round, best);
    renderStats(best);
    renderClock();

    calibrating = false;
    calibrateBtn.disabled = false;
  }

  calibrateBtn.addEventListener('click', () => calibrate(7));

  autoBox.addEventListener('change', () => {
    if (autoTimer) {
      clearInterval(autoTimer);
      autoTimer = null;
    }
    if (autoBox.checked) autoTimer = setInterval(() => calibrate(5), 5000);
  });

  // 秒表：按偏移推算，60ms 刷新足够看清毫秒跳动
  setInterval(() => {
    if (!synced) return;
    renderClock();
  }, 60);

  calibrate(7).then(() => {
    if (autoBox.checked) autoTimer = setInterval(() => calibrate(5), 5000);
  });
})();
