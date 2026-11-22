// The report pages. Every text that came from a client or a replica is put
// into the page with textContent, never as markup.
'use strict';

const TABS = [['overview', 'Overview'], ['queries', 'Queries'], ['slow', 'Slowest'], ['errors', 'Failures'],
              ['clients', 'Clients'], ['replicas', 'Replicas'], ['now', 'Now']];
const REFRESH_MS = 5000;
const $ = (id) => document.getElementById(id);
const SVG = 'http://www.w3.org/2000/svg';

function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c != null) el.append(c.nodeType ? c : document.createTextNode(String(c)));
  return el;
}
function s(tag, attrs, text) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) el.setAttribute(k, v);
  if (text != null) el.textContent = text;
  return el;
}

const int = (n) => Math.round(n).toLocaleString('en-US');
function dur(ms) {
  if (ms >= 60000) return (ms / 60000).toFixed(1) + ' min';
  if (ms >= 1000) return (ms / 1000).toFixed(2) + ' s';
  if (ms >= 10) return ms.toFixed(0) + ' ms';
  if (ms >= 1) return ms.toFixed(1) + ' ms';
  return ms > 0 ? (ms * 1000).toFixed(0) + ' µs' : '0';
}
function bytes(n) {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + ' ' + units[i];
}
const pct = (part, whole) => whole ? (100 * part / whole).toFixed(part && part / whole < 0.1 ? 2 : 1) + '%' : '0%';
const clock = (t, withDate) => new Date(t).toLocaleString([], withDate
  ? { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }
  : { hour: '2-digit', minute: '2-digit' });
const stamp = (ms) => new Date(ms).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });

function route() {
  const [tab, arg] = location.hash.slice(1).split('/');
  return { tab: tab || 'overview', arg };
}
function params(extra) {
  const range = +$('range').value, to = Math.floor(Date.now() / 1000);
  const p = new URLSearchParams({ from: to - range, to, pool: $('pool').value, ...extra });
  return '?' + p;
}
async function get(path) {
  const r = await fetch(path);
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

function tile(label, value, note) {
  return h('div', { class: 'card tile' }, h('div', { class: 'label' }, label), h('div', { class: 'value' }, value),
           note ? h('div', { class: 'note' }, note) : null);
}
function card(title, sub, ...body) {
  return h('div', { class: 'card' }, h('h2', {}, title), sub ? h('p', { class: 'sub' }, sub) : null, ...body);
}
function table(columns, rows, onRow) {
  if (!rows.length) return h('div', { class: 'empty' }, 'Nothing in this range.');
  return h('table', {},
    h('thead', {}, h('tr', {}, columns.map((c) => h('th', { class: c.cls || '' }, c.title)))),
    h('tbody', {}, rows.map((r) => h('tr', onRow ? { class: 'link', onclick: () => onRow(r) } : {},
      columns.map((c) => h('td', { class: c.cls || '' }, c.cell(r)))))));
}
const num = (title, cell) => ({ title, cell, cls: 'num' });

// A line chart: time across, one scale up. `series` is [{name, color, values}],
// one value per entry of `times` (ms). The crosshair finds the time; the
// tooltip lists every series there.
function lineChart(times, series, format) {
  const legend = series.length > 1 ? h('div', { class: 'legend' }, series.map((one) => {
    const key = h('i', { class: 'key' });
    key.style.background = `var(${one.color})`;
    return h('span', {}, key, one.name);
  })) : null;
  const plot = h('div', {});
  // Drawn at the width it is shown at, and again when that changes, so text
  // and strokes are never stretched.
  let width = 0;
  new ResizeObserver(() => {
    if (plot.clientWidth && plot.clientWidth !== width) {
      width = plot.clientWidth;
      plot.replaceChildren(drawLines(width, times, series, format));
    }
  }).observe(plot);
  return h('div', {}, legend, plot);
}
function drawLines(W, times, series, format) {
  const H = 220, L = 8, R = series.length > 1 ? 44 : 12, T = 12, B = 22;
  const svg = s('svg', { class: 'chart', viewBox: `0 0 ${W} ${H}`, tabindex: 0 });
  const max = Math.max(...series.flatMap((x) => x.values), 0) || 1;
  const t0 = times[0], t1 = times[times.length - 1];
  const x = (t) => t1 === t0 ? (L + W - R) / 2 : L + (W - L - R) * (t - t0) / (t1 - t0);
  const y = (v) => H - B - (H - T - B) * v / max;
  for (let i = 0; i <= 3; i++) {
    const v = max * i / 3;
    svg.append(s('line', { class: i ? 'grid' : 'base', x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    if (i) svg.append(s('text', { x: L, y: y(v) - 4 }, format(v)));
  }
  const days = t1 - t0 > 86400000;
  for (const [t, anchor] of [[t0, 'start'], [(t0 + t1) / 2, 'middle'], [t1, 'end']]) {
    if (t1 > t0 || anchor === 'middle') svg.append(s('text', { x: x(t), y: H - 6, 'text-anchor': anchor }, clock(t, days)));
  }
  const cross = s('line', { class: 'cross', y1: T, y2: H - B, visibility: 'hidden' });
  svg.append(cross);
  const dots = [];
  const ends = [];
  for (const one of series) {
    const d = one.values.map((v, i) => `${i ? 'L' : 'M'}${x(times[i]).toFixed(1)} ${y(v).toFixed(1)}`).join('');
    svg.append(s('path', { class: 'line', d, stroke: `var(${one.color})` }));
    if (times.length === 1) svg.append(s('circle', { cx: x(t0), cy: y(one.values[0]), r: 4, fill: `var(${one.color})` }));
    ends.push({ name: one.name, at: y(one.values[one.values.length - 1]) });
    const dot = s('circle', { r: 4, fill: `var(${one.color})`, visibility: 'hidden' });
    dots.push(dot);
    svg.append(dot);
  }
  // Each line is named at its end; labels that would overlap are moved apart.
  if (series.length > 1) {
    ends.sort((a, b) => a.at - b.at);
    for (let i = 1; i < ends.length; i++) ends[i].at = Math.max(ends[i].at, ends[i - 1].at + 12);
    for (let i = ends.length - 1, low = H - B; i >= 0; i--, low -= 12) ends[i].at = Math.min(ends[i].at, low);
    for (const e of ends) svg.append(s('text', { class: 'end', x: W - R + 6, y: e.at + 4 }, e.name));
  }
  const hide = () => {
    cross.setAttribute('visibility', 'hidden');
    dots.forEach((d) => d.setAttribute('visibility', 'hidden'));
    $('tooltip').hidden = true;
  };
  const show = (i, clientX, clientY) => {
    cross.setAttribute('x1', x(times[i]));
    cross.setAttribute('x2', x(times[i]));
    cross.setAttribute('visibility', 'visible');
    series.forEach((one, k) => {
      dots[k].setAttribute('cx', x(times[i]));
      dots[k].setAttribute('cy', y(one.values[i]));
      dots[k].setAttribute('visibility', 'visible');
    });
    const tip = $('tooltip');
    tip.replaceChildren(h('div', { class: 'when' }, clock(times[i], true)), ...series.map((one) => {
      const key = h('i', { class: 'key' });
      key.style.background = `var(${one.color})`;
      return h('div', {}, key, h('b', {}, format(one.values[i])), ' ', h('span', {}, one.name));
    }));
    tip.hidden = false;
    const box = tip.getBoundingClientRect();
    tip.style.left = Math.min(clientX + 14, innerWidth - box.width - 8) + 'px';
    tip.style.top = Math.max(8, clientY - box.height - 10) + 'px';
  };
  let at = times.length - 1;
  svg.addEventListener('pointermove', (e) => {
    const box = svg.getBoundingClientRect();
    const t = t0 + (t1 - t0) * ((e.clientX - box.left) / box.width * W - L) / (W - L - R);
    at = times.reduce((best, v, i) => Math.abs(v - t) < Math.abs(times[best] - t) ? i : best, 0);
    show(at, e.clientX, e.clientY);
  });
  svg.addEventListener('pointerleave', hide);
  svg.addEventListener('blur', hide);
  svg.addEventListener('keydown', (e) => {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    at = Math.max(0, Math.min(times.length - 1, at + (e.key === 'ArrowLeft' ? -1 : 1)));
    const box = svg.getBoundingClientRect();
    show(at, box.left + box.width * x(times[at]) / W, box.top + 40);
    e.preventDefault();
  });
  return svg;
}

function charts(data) {
  const p = data.points;
  if (!p.length) return [h('div', { class: 'card empty' }, 'No requests in this range.')];
  const times = p.map((x) => x.t * 1000), per = data.step_s / 60;
  const step = per === 1 ? 'per minute' : `per ${per} minutes`;
  const one = (name, key) => [{ name, color: '--series-1', values: p.map((x) => x[key]) }];
  return [h('div', { class: 'charts' },
    card('Requests', step, lineChart(times, one('requests', 'count'), int)),
    card('Time in the replica', 'half, 95% and 99% of requests were faster than this',
      lineChart(times, [
        { name: 'p50', color: '--series-1', values: p.map((x) => x.p50_ms) },
        { name: 'p95', color: '--series-2', values: p.map((x) => x.p95_ms) },
        { name: 'p99', color: '--series-3', values: p.map((x) => x.p99_ms) }], dur)),
    card('Requests that did not succeed', step + ': errors, cancels, timeouts, refusals and failures',
      lineChart(times, one('not ok', 'not_ok'), int)),
    card('Wait for a free replica', 'average time in the bouncer\'s queue',
      lineChart(times, one('wait', 'wait_avg_ms'), dur)))];
}
function tiles(t) {
  return h('div', { class: 'tiles' },
    tile('Requests', int(t.count)),
    tile('Average time', dur(t.avg_ms), `p95 ${dur(t.p95_ms)} · p99 ${dur(t.p99_ms)}`),
    tile('Did not succeed', pct(t.not_ok, t.count), `${int(t.errors)} errors · ${int(t.timeouts)} timeouts · ${int(t.refused)} refused`),
    tile('Average wait', dur(t.wait_avg_ms), 'for a free replica'),
    tile('Sent to clients', bytes(t.bytes_out), `${int(t.retries)} retries`));
}

const samplesColumns = [
  { title: 'When', cell: (r) => stamp(r.ts) },
  num('Time', (r) => dur(r.ms)), num('Waited', (r) => dur(r.wait_ms)),
  { title: 'Ended', cell: (r) => h('span', { class: 'pill' }, r.status) },
  { title: 'Replica', cell: (r) => r.backend || '—' }, { title: 'Client', cell: (r) => r.client },
  { title: 'Request', cell: (r) => r.text, cls: 'q' }];
const openQuery = (r) => { location.hash = 'query/' + r.fp; };

const views = {
  async overview() {
    const data = await get('/api/overview' + params());
    return [tiles(data.total), ...charts(data)];
  },
  async queries() {
    const sort = sessionStorage.sort || 'total';
    const data = await get('/api/top' + params({ sort, limit: 100 }));
    const pick = h('select', { onchange: (e) => { sessionStorage.sort = e.target.value; render(); } },
      [['total', 'total time'], ['count', 'requests'], ['avg', 'average time'], ['max', 'slowest request'],
       ['errors', 'did not succeed'], ['bytes', 'bytes sent']].map(([v, t]) => {
        const o = h('option', { value: v }, t);
        o.selected = v === sort;
        return o;
      }));
    return [card(`${int(data.queries)} queries`, 'Requests that differ only in their values are one query. Click one for its history.',
      h('p', { class: 'sub' }, 'Heaviest by ', pick),
      table([
        { title: 'Query', cls: 'q', cell: (r) => {
          const bar = h('div', { class: 'bar' });
          bar.style.width = Math.max(1, 100 * r.share) + '%';
          return h('div', {}, r.text, bar);
        } },
        num('Requests', (r) => int(r.count)), num('Total time', (r) => `${dur(r.total_ms)} (${pct(r.share, 1)})`),
        num('Average', (r) => dur(r.avg_ms)), num('p95', (r) => dur(r.p95_ms)), num('Slowest', (r) => dur(r.max_ms)),
        num('Not ok', (r) => int(r.not_ok)), num('Sent', (r) => bytes(r.bytes_out))], data.rows, openQuery))];
  },
  async query(fp) {
    const data = await get('/api/query' + params({ fp, limit: 20 }));
    return [card('Query', null, h('pre', { class: 'q' }, data.text || '(not seen in this range)')),
      tiles(data.total), ...charts(data),
      card('Slowest requests', null, table(samplesColumns, data.slow)),
      card('Requests that did not succeed', 'the most recent kept', table(samplesColumns, data.failed))];
  },
  async slow() {
    const data = await get('/api/slow' + params({ limit: 100 }));
    return [card('Slowest requests', 'The slowest of each hour are kept with their full text.', table(samplesColumns, data.rows, openQuery))];
  },
  async errors() {
    const [data, overview] = await Promise.all([get('/api/errors' + params({ limit: 200 })), get('/api/overview' + params())]);
    const t = overview.total;
    return [h('div', { class: 'tiles' },
        tile('Errors', int(t.errors), 'raised by a replica'), tile('Cancels', int(t.cancels), 'by the client'),
        tile('Timeouts', int(t.timeouts), 'past the query budget'), tile('Refused', int(t.refused), 'queue full or wait too long'),
        tile('Failures', int(t.failed), 'no replica could answer')),
      card('Most recent', 'Up to 20 a minute are kept with their full text. Kinds kept: ' +
        (data.kinds.map((k) => `${k.status} (${k.sampled})`).join(', ') || 'none'),
        table(samplesColumns, data.rows, openQuery))];
  },
  async clients() {
    const data = await get('/api/clients' + params({ limit: 200 }));
    return [card('Clients', 'By address, the busiest first.', table([
      { title: 'Address', cell: (r) => r.name }, { title: 'Pool', cell: (r) => r.pool },
      num('Requests', (r) => int(r.count)), num('Not ok', (r) => int(r.not_ok)),
      num('Total time', (r) => dur(r.total_ms)), num('Average', (r) => dur(r.avg_ms))], data.rows))];
  },
  async replicas() {
    const [data, live] = await Promise.all([get('/api/backends' + params({ limit: 200 })), get('/api/live')]);
    const total = data.rows.reduce((n, r) => n + r.count, 0);
    return [card('Share of the work', 'Requests each replica answered in this range.', table([
        { title: 'Replica', cell: (r) => r.name }, { title: 'Pool', cell: (r) => r.pool },
        num('Requests', (r) => `${int(r.count)} (${pct(r.count, total)})`), num('Not ok', (r) => int(r.not_ok)),
        num('Total time', (r) => dur(r.total_ms)), num('Average', (r) => dur(r.avg_ms))], data.rows)),
      liveReplicas(live)];
  },
  async now() {
    const live = await get('/api/live');
    const queued = live.pools.flatMap((p) => p.queue.map((q) => ({ pool: p.name, ...q })));
    return [h('div', { class: 'tiles' },
        tile('Clients connected', int(live.clients)), tile('Waiting in the queue', int(queued.length)),
        tile('Up for', dur(live.uptime_s * 1000), 'version ' + live.version),
        live.stats_dropped ? tile('Statistics dropped', int(live.stats_dropped), 'requests not counted') : null),
      liveReplicas(live),
      card('Waiting for a replica', null, table([
        { title: 'Pool', cell: (r) => r.pool }, { title: 'Client', cell: (r) => r.client },
        num('Waited', (r) => dur(r.waited_ms)), { title: 'Request', cell: (r) => r.text, cls: 'q' }], queued))];
  },
};

function liveReplicas(live) {
  const rows = live.pools.flatMap((p) => p.backends.map((b) => ({ pool: p.name + (p.paused ? ' (paused)' : ''), ...b })));
  const state = (r) => {
    const dot = h('i', { class: 'dot' });
    dot.style.background = `var(${r.state === 'down' ? '--critical' : r.state === 'busy' ? '--warning' : '--good'})`;
    return h('span', {}, dot, r.state + (r.draining ? ', draining' : ''));
  };
  return card('Replicas now', null, table([
    { title: 'Replica', cell: (r) => r.addr }, { title: 'Pool', cell: (r) => r.pool }, { title: 'State', cell: state },
    num('Served', (r) => int(r.served)), num('Failures', (r) => int(r.failures)),
    num('Running for', (r) => r.running ? dur(r.running.ms) : ''),
    { title: 'Request', cls: 'q', cell: (r) => r.running ? r.running.text : r.last_error }], rows));
}

let drawn = 0;
async function render() {
  const { tab, arg } = route();
  const mine = ++drawn;
  for (const a of $('tabs').children) a.className = a.hash === '#' + tab ? 'on' : '';
  $('filters').hidden = tab === 'now';
  $('view').classList.add('stale');
  try {
    const live = await get('/api/live');
    const pool = $('pool'), chosen = pool.value;
    pool.replaceChildren(h('option', { value: '' }, 'all pools'), ...live.pools.map((p) => h('option', { value: p.name }, p.name)));
    pool.value = live.pools.some((p) => p.name === chosen) ? chosen : '';
    const body = await (views[tab] || views.overview)(arg);
    if (mine !== drawn) return;
    $('tooltip').hidden = true;
    $('view').replaceChildren(...body.filter(Boolean));
    $('status').textContent = 'updated ' + new Date().toLocaleTimeString();
  } catch (e) {
    if (mine !== drawn) return;
    $('view').replaceChildren(h('div', { class: 'card empty' }, e.message));
    $('status').textContent = '';
  }
  $('view').classList.remove('stale');
}

$('tabs').append(...TABS.map(([id, title]) => h('a', { href: '#' + id }, title)));
addEventListener('hashchange', render);
$('pool').addEventListener('change', render);
$('range').addEventListener('change', render);
// A page nobody is pointing at is refreshed; one being read is left alone.
setInterval(() => { if ($('tooltip').hidden && !document.hidden && !getSelection().toString()) render(); }, REFRESH_MS);
render();
