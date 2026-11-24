// The report pages. Every text that came from a client or a replica is put
// into the page with textContent, never as markup.
'use strict';

const TABS = [['overview', 'Overview'], ['live', 'Live'], ['queries', 'Queries'], ['slow', 'Slow'],
              ['failed', 'Failed'], ['clients', 'Clients'], ['replicas', 'Replicas']];
const RANGES = [[900, '15m', 'the last 15 minutes'], [3600, '1h', 'the last hour'], [21600, '6h', 'the last 6 hours'],
                [86400, '24h', 'the last 24 hours'], [604800, '7d', 'the last 7 days']];
// What the bouncer itself answers with, and what each means. Any other
// outcome that is not ok is an error code a replica raised.
const OUTCOMES = [['error', 'replica raised'], ['timeout', 'over the query budget'],
                  ['busy', 'queue full or waited too long'], ['io', 'no replica answered'], ['cancel', 'client cancelled']];
const REFRESH_S = 5;
const SVG = 'http://www.w3.org/2000/svg';
const $ = (id) => document.getElementById(id);

function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c != null && c !== false) el.append(c.nodeType ? c : document.createTextNode(String(c)));
  return el;
}
function s(tag, attrs, text) {
  const el = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) el.setAttribute(k, v);
  if (text != null) el.textContent = text;
  return el;
}

// ---- numbers and times ----

const int = (n) => Math.round(n).toLocaleString('en-US');
const sig3 = (x) => String(Number(x.toPrecision(3)));
// A duration at three significant figures: a percentile is a bucket's edge,
// and more digits would claim what it does not know.
function dur(ms) {
  if (!ms) return '0 ms';
  // The unit is chosen by the number as it will be written: 999.5 ms is
  // 1 s, not 1000 ms, and 59.99 s is a minute.
  const fits = (x, below) => Number(x.toPrecision(3)) < below;
  if (fits(ms * 1000, 1000)) return sig3(ms * 1000) + ' µs';
  if (fits(ms, 1000)) return sig3(Math.max(ms, 1)) + ' ms';
  if (fits(ms / 1000, 60)) return sig3(Math.max(ms / 1000, 1)) + ' s';
  return span(Math.round(ms / 1000));
}
function span(sec) {
  sec = Math.floor(sec);
  const d = Math.floor(sec / 86400), hh = Math.floor(sec % 86400 / 3600), m = Math.floor(sec % 3600 / 60), ss = sec % 60;
  if (d) return `${d} d ${hh} h`;
  if (hh) return `${hh} h ${two(m)} m`;
  if (m) return `${m} m ${two(ss)} s`;
  return `${ss} s`;
}
// Milliseconds for a column whose header names the unit, so a column reads
// as magnitudes and not as a mix of units.
// The steps are where the rounded number changes its length, so 99.96 is
// 100 and not 100.0.
const msCell = (ms) => ms >= 99.95 ? int(ms) : ms.toFixed(ms >= 9.995 ? 1 : 2);
function bytes(n) {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  // By the number as written: 1023.99 KiB is 1.0 MiB, not 1024.0 KiB.
  while (i < units.length - 1 && (i ? Number(n.toFixed(1)) : n) >= 1024) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + ' ' + units[i];
}
function pct(part, whole) {
  if (!part || !whole) return '0%';
  const p = 100 * part / whole;
  return p < 0.1 ? '<0.1%' : p.toFixed(1) + '%';
}
function two(n) { return String(n).padStart(2, '0'); }
const hm = (d) => `${two(d.getHours())}:${two(d.getMinutes())}`;
const md = (d) => `${two(d.getMonth() + 1)}-${two(d.getDate())}`;
const sameDay = (a, b) => a.toDateString() === b.toDateString();
// A moment in a table: to the millisecond, with the date only where the
// rows are not all of today.
function stamp(ms, withDate) {
  const d = new Date(ms);
  const date = withDate || !sameDay(d, new Date()) ? md(d) + ' ' : '';
  return `${date}${hm(d)}:${two(d.getSeconds())}.${String(d.getMilliseconds()).padStart(3, '0')}`;
}
function zone() {
  const off = -new Date().getTimezoneOffset();
  if (!off) return 'UTC';
  const a = Math.abs(off);
  return `UTC${off > 0 ? '+' : '−'}${Math.floor(a / 60)}${a % 60 ? ':' + two(a % 60) : ''}`;
}

// ---- state: everything that picks what is shown is in the address ----

function state() {
  const [path, query] = location.hash.slice(1).split('?');
  const [tab, arg] = path.split('/');
  const p = new URLSearchParams(query || '');
  const range = RANGES.find((r) => r[0] === +p.get('range')) || RANGES[1];
  return { tab: tab || 'overview', arg, pool: p.get('pool') || '', range };
}
function link(tab, change) {
  const st = { ...state(), ...change };
  const p = new URLSearchParams();
  if (st.pool) p.set('pool', st.pool);
  if (st.range[0] !== 3600) p.set('range', st.range[0]);
  const q = p.toString();
  return '#' + tab + (q ? '?' + q : '');
}
const here = (st) => st.tab + (st.arg ? '/' + st.arg : '');
async function get(path) {
  const r = await fetch(path);
  let body = null;
  try { body = await r.json(); } catch (e) { /* not ours to read */ }
  if (!r.ok || body == null) {
    const err = new Error(body && body.error || `${r.status} ${r.statusText}`);
    err.status = r.status;
    throw err;
  }
  return body;
}

// ---- pieces ----

function section(title, sub, ...body) {
  return h('section', {}, title ? h('h2', {}, title, sub ? h('small', {}, sub) : null) : null, ...body);
}
function strip(cells) {
  return h('section', { class: 'plain' }, h('div', { class: 'strip' }, cells.filter(Boolean).map((c) => h('div', {},
    h('div', { class: 'label' }, c.label),
    h('div', { class: 'value' + (c.lead ? ' lead' : '') + (c.bad ? ' bad' : '') }, c.value, c.delta),
    c.sub ? h('div', { class: 'sub' }, c.sub) : null))));
}
function table(columns, rows, empty, wide) {
  columns = columns.filter(Boolean);
  if (!rows.length) return h('div', { class: 'empty' }, empty);
  const cls = (c, r) => (typeof c.cls === 'function' ? c.cls(r) : c.cls) || '';
  return h('table', { class: wide ? 'wide' : '' },
    h('thead', {}, h('tr', {}, columns.map((c) => h('th', {
      class: [typeof c.cls === 'string' && c.cls.split(' ')[0], c.sort && 'sort', c.sorted && 'sorted'].filter(Boolean).join(' '),
      onclick: c.sort,
    }, c.title, c.sorted ? ' ▾' : '')))),
    h('tbody', {}, rows.map((r) => h('tr', {}, columns.map((c) => h('td', { class: cls(c, r) }, c.cell(r)))))));
}
const num = (title, cell, more) => ({ title, cell, cls: 'num', ...more });
// A request's text: two lines at most in a list, the whole of it on hover
// and on its own page. A link where there is somewhere to go.
function text(value, href) {
  return href ? h('a', { href, title: value }, value) : h('span', { title: value }, value);
}
// How a request ended, in the data's own words. `error: name` is a code a
// replica raised; the rest are the bouncer's.
function outcome(status) {
  if (status === 'ok' || status === 'cached') return h('span', { class: 'dim' }, status);
  if (status === 'cancel') return 'cancel';
  const own = OUTCOMES.some(([name]) => name === status);
  return h('span', { class: 'bad' }, own ? status : 'error: ' + status);
}
function failedCell(failed, count) {
  if (!failed) return h('span', { class: 'dim' }, '0');
  return h('span', { class: 'bad' }, `${int(failed)}  ${pct(failed, count)}`);
}
// How a number moved against the period before: which way and by how much,
// the earlier number on hover. Empty where it moved by less than a tenth:
// periods are whole minutes and percentiles are bucket edges, so less than
// that is no change, and a page where nothing changed should say nothing.
function delta(now, was, format, fresh) {
  const el = h('span', { class: 'delta' });
  now = now || 0; was = was || 0;
  if (now === was || (was && now / was > 0.9 && now / was < 1.1)) return el;
  const r = now / was;
  el.textContent = !was && fresh ? fresh : (now > was ? '▲ ' : '▼ ') + (!was ? `from ${format(0)}` : !now ? `from ${format(was)}`
    : r >= 2 ? `${sig3(Number(r.toPrecision(2)))}×` : `${Math.round(Math.abs(r - 1) * 100)}%`);
  el.title = `${format(was)} in the period before`;
  return el;
}
function share(part, whole) {
  const bar = h('i', {});
  bar.style.width = (whole ? Math.max(1, 100 * part / whole) : 0) + '%';
  return h('div', { class: 'share' }, bar);
}
function replicaState(r) {
  const kind = r.state === 'down' ? 'down' : r.draining ? 'draining' : r.state;
  return h('span', { class: kind === 'down' ? 'bad' : '' }, h('i', { class: 'dot ' + kind }), kind,
    notOurs(r) ? h('span', { class: 'dim', title: notOurs(r) }, ' · not ours') : null);
}
// What a replica says of itself over its status stream; blank where it says
// nothing. `notOurs` is what it runs that the bouncer did not send.
function notOurs(r) {
  const st = r.status;
  return st && st.not_ours ? st.text || (st.state === 'other' ? 'other work' : 'a request') : '';
}
const loadColumns = [
  num('Load', (r) => r.status && r.status.util != null ? pct(r.status.util, 1) : ''),
  num('CPU', (r) => r.status && r.status.cpu != null ? `${r.status.cpu.toFixed(2)} / ${int(r.status.cores)}` : ''),
  num('Memory', (r) => r.status ? bytes(r.status.rss_bytes) + (r.status.mem_share != null ? ' · ' + pct(r.status.mem_share, 1) : '') : ''),
];

// ---- charts ----
// Time across, over the whole range asked for, so a stretch without requests
// shows as one. A point is a step of the range, not an instant: counts are
// bars and percentiles are steps, each as wide as the step it is of.

let charts = [];
const H = 160, L = 48, R = 44, T = 8, B = 20;

function niceStep(max, lines) {
  const raw = max / lines, pow = 10 ** Math.floor(Math.log10(raw));
  return [1, 2, 5, 10].map((m) => m * pow).find((v) => v >= raw);
}
function timeTicks(from, to) {
  const len = to - from;
  const every = len <= 900 ? 300 : len <= 3600 ? 600 : len <= 21600 ? 3600 : len <= 86400 ? 14400 : 86400;
  const tz = -new Date(from * 1000).getTimezoneOffset() * 60;
  const out = [];
  for (let t = Math.ceil((from + tz) / every) * every - tz; t <= to; t += every) out.push(t);
  return out.map((t) => [t, every >= 86400 ? md(new Date(t * 1000)) : hm(new Date(t * 1000))]);
}

// spec: { bars, log, format, series: [{name, color, of: point => value}], tip: point => [[name, value]], none }
function chart(data, spec) {
  const points = data.points.filter((p) => spec.series.some((x) => x.of(p) > 0));
  if (!points.length) return h('div', { class: 'empty' }, spec.none);
  const box = h('div', {});
  const legend = spec.bars && spec.series.length > 1 ? h('div', { class: 'note' }, spec.series.map((x) => {
    const key = h('i', { class: 'key' });
    key.style.background = `var(${x.color})`;
    return h('span', {}, key, x.name + '   ');
  })) : null;
  const self = { mark: () => {} };
  charts.push(self);
  let width = 0;
  new ResizeObserver(() => {
    if (box.clientWidth && box.clientWidth !== width) {
      width = box.clientWidth;
      box.replaceChildren(draw(width, data, points, spec, self));
    }
  }).observe(box);
  return h('div', {}, box, legend);
}

function draw(W, data, points, spec, self) {
  const svg = s('svg', { class: 'chart', viewBox: `0 0 ${W} ${H}`, tabindex: 0 });
  const { from, to, step_s: step } = data;
  // A range of no length has no scale; everything in it is at its start.
  const x = (t) => L + (W - L - R) * (Math.min(Math.max(t, from), to) - from) / (to - from || 1);
  // Where a step's bar or mark begins. A step that starts where the range
  // ends has no width left, and is kept as the last pixel inside the plot.
  const left = (t) => Math.max(L, Math.min(x(t), x(t + step) - 1));
  const values = (p) => spec.series.map((sr) => sr.of(p));
  const top = Math.max(...points.map((p) => spec.bars ? values(p).reduce((a, b) => a + b, 0) : Math.max(...values(p))));

  let y, ticks = [];
  if (spec.log) {
    // Decades of a millisecond; the axis spans the ones the data touches.
    const low = Math.min(...points.flatMap((p) => values(p).filter((v) => v > 0)));
    const lo = Math.floor(Math.log10(low)), hi = Math.max(lo + 1, Math.ceil(Math.log10(top)));
    y = (v) => H - B - (H - T - B) * (Math.log10(Math.max(v, 10 ** lo)) - lo) / (hi - lo);
    for (let e = lo; e <= hi; e++) ticks.push(10 ** e);
  } else {
    const each = niceStep(top, 3);
    const max = each * Math.ceil(top / each);
    y = (v) => H - B - (H - T - B) * v / max;
    for (let v = 0; v <= max + each / 2; v += each) ticks.push(v);
  }
  ticks.forEach((v, i) => {
    svg.append(s('line', { class: i ? 'grid' : 'base', x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svg.append(s('text', { x: L - 6, y: y(v), 'text-anchor': 'end', 'dominant-baseline': 'middle' }, spec.format(v)));
  });
  for (const [t, label] of timeTicks(from, to)) {
    svg.append(s('line', { class: 'base', x1: x(t), x2: x(t), y1: H - B, y2: H - B + 3 }));
    svg.append(s('text', { x: x(t), y: H - 4, 'text-anchor': 'middle' }, label));
  }

  const cross = s('rect', { class: 'cross', y: T, height: H - T - B, visibility: 'hidden' });
  svg.append(cross);
  const now = Date.now() / 1000;
  if (spec.bars) {
    for (const p of points) {
      const w = Math.max(1, x(p.t + step) - x(p.t) - 1), x0 = w > 1 ? x(p.t) + 0.5 : left(p.t);
      let base = 0;
      spec.series.forEach((sr) => {
        const v = sr.of(p);
        if (v <= 0) return;
        const rect = s('rect', { x: x0, width: w, y: y(base + v), height: Math.max(0.5, y(base) - y(base + v)), fill: `var(${sr.color})` });
        // A step still being filled is not a drop in traffic.
        if (p.t + step > now) rect.setAttribute('opacity', 0.4);
        svg.append(rect);
        base += v;
      });
    }
  } else {
    const ends = [];
    spec.series.forEach((sr) => {
      let d = '', last = null;
      for (const p of points) {
        const v = sr.of(p);
        if (v <= 0) continue;
        d += last != null && p.t === last ? `V${y(v).toFixed(1)}` : `M${x(p.t).toFixed(1)} ${y(v).toFixed(1)}`;
        d += `H${x(p.t + step).toFixed(1)}`;
        last = p.t + step;
      }
      svg.append(s('path', { d, stroke: `var(${sr.color})` }));
      ends.push({ name: sr.name, at: y(sr.of(points[points.length - 1])) });
    });
    ends.sort((a, b) => a.at - b.at);
    for (let i = 1; i < ends.length; i++) ends[i].at = Math.max(ends[i].at, ends[i - 1].at + 13);
    for (let i = ends.length - 1, low = H - B; i >= 0; i--, low -= 13) ends[i].at = Math.min(ends[i].at, low);
    for (const e of ends) svg.append(s('text', { class: 'end', x: W - R + 6, y: e.at, 'dominant-baseline': 'middle' }, e.name));
  }

  // The step under the pointer is marked on every chart of the page: they
  // share the time axis, so the eye can go straight down.
  self.mark = (t) => {
    const p = t == null ? null : points.find((q) => q.t === t);
    cross.setAttribute('visibility', p ? 'visible' : 'hidden');
    if (p) {
      cross.setAttribute('x', left(p.t));
      cross.setAttribute('width', Math.max(1, x(p.t + step) - x(p.t)));
    }
  };
  const show = (p, clientX, clientY) => {
    charts.forEach((c) => c.mark(p.t));
    const a = new Date(p.t * 1000), b = new Date((p.t + step) * 1000);
    const day = to - from > 86400 ? md(a) + ' ' : '';
    const rows = (spec.tip || ((q) => spec.series.map((sr) => [sr.name, spec.format(sr.of(q))])))(p);
    const tip = $('tooltip');
    tip.replaceChildren(h('div', { class: 'when' }, `${day}${hm(a)}–${hm(b)}`),
      h('div', { class: 'rows' }, rows.flatMap(([name, value]) => [h('span', {}, name), h('b', {}, value)])));
    tip.hidden = false;
    const box = tip.getBoundingClientRect();
    tip.style.left = Math.min(clientX + 14, innerWidth - box.width - 8) + 'px';
    tip.style.top = Math.max(8, clientY - box.height - 10) + 'px';
  };
  const hide = () => { charts.forEach((c) => c.mark(null)); $('tooltip').hidden = true; };
  let at = points.length - 1;
  svg.addEventListener('pointermove', (e) => {
    const box = svg.getBoundingClientRect();
    const t = from + (to - from) * ((e.clientX - box.left) / box.width * W - L) / (W - L - R);
    at = points.reduce((best, p, i) => Math.abs(p.t + step / 2 - t) < Math.abs(points[best].t + step / 2 - t) ? i : best, 0);
    show(points[at], e.clientX, e.clientY);
  });
  svg.addEventListener('pointerleave', hide);
  svg.addEventListener('blur', hide);
  svg.addEventListener('keydown', (e) => {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    at = Math.max(0, Math.min(points.length - 1, at + (e.key === 'ArrowLeft' ? -1 : 1)));
    const box = svg.getBoundingClientRect();
    show(points[at], box.left + box.width * x(points[at].t + step / 2) / W, box.top + 40);
    e.preventDefault();
  });
  return svg;
}

function threeCharts(data, label) {
  const rate = (n) => n / data.step_s;
  const none = `No requests in ${label}.`;
  return h('div', { class: 'charts' },
    section('Requests /s', null, chart(data, {
      bars: true, format: (v) => v >= 10 ? int(v) : sig3(v), none,
      series: [{ name: 'ok', color: '--series-1', of: (p) => rate(p.count - p.not_ok) },
               { name: 'failed', color: '--series-2', of: (p) => rate(p.not_ok) }],
      tip: (p) => [['ok', int(p.count - p.not_ok)], ['failed', `${int(p.not_ok)} (${pct(p.not_ok, p.count)})`]],
    })),
    section('Exec time', null, chart(data, {
      log: true, format: dur, none,
      series: [{ name: 'p99', color: '--series-3', of: (p) => p.p99_ms },
               { name: 'p95', color: '--series-2', of: (p) => p.p95_ms },
               { name: 'p50', color: '--series-1', of: (p) => p.p50_ms }],
    }), h('p', { class: 'note' }, 'percentiles are histogram bucket upper bounds (powers of two)')),
    section('Wait + exec, avg', null, chart(data, {
      bars: true, format: dur, none,
      series: [{ name: 'exec', color: '--series-1', of: (p) => p.avg_ms },
               { name: 'wait', color: '--series-2', of: (p) => p.wait_avg_ms }],
      tip: (p) => [['exec', dur(p.avg_ms)], ['wait', dur(p.wait_avg_ms)], ['wait p50', dur(p.wait_p50_ms)],
        ['wait p95', dur(p.wait_p95_ms)], ['wait p99', dur(p.wait_p99_ms)], ['max wait', dur(p.wait_max_ms)]],
    })));
}

// The numbers of a range in one line: what failed first, since that is what
// the page is opened for.
// `was` is the same of the period before; with no requests in it there
// is nothing to hold a time or a rate against.
function totals(t, seconds, extra, was) {
  const rate = (x) => x.count ? x.not_ok / x.count : 0;
  const moved = (now, then, format) => was && was.count ? delta(now, then, format) : null;
  const kinds = [[t.errors, 'error'], [t.timeouts, 'timeout'], [t.refused, 'busy'], [t.failed, 'io'], [t.cancels, 'cancel']];
  const failedSub = h('span', {}, kinds.flatMap(([n, name], i) => [i ? ' · ' : '', h('span', { class: n ? '' : 'dim' }, `${int(n)} ${name}`)]),
    t.retries ? ` · ${int(t.retries)} retried` : '');
  return strip([
    { label: 'Failed', value: pct(t.not_ok, t.count), lead: true, bad: t.not_ok > 0, sub: failedSub,
      delta: moved(rate(t), rate(was), (x) => pct(x, 1)) },
    { label: 'Exec p99', value: dur(t.p99_ms), sub: `p50 ${dur(t.p50_ms)} · p95 ${dur(t.p95_ms)}`, delta: moved(t.p99_ms, was.p99_ms, dur) },
    { label: 'Wait p99', value: dur(t.wait_p99_ms), delta: moved(t.wait_p99_ms, was.wait_p99_ms, dur),
      sub: `p50 ${dur(t.wait_p50_ms)} · p95 ${dur(t.wait_p95_ms)} · max ${dur(t.wait_max_ms)} · avg ${dur(t.wait_avg_ms)}` },
    { label: 'Requests', value: int(t.count), sub: `${sig3(t.count / seconds)}/s` + (t.cached ? ` · ${int(t.cached)} cached` : ''), delta: was && delta(t.count, was.count, int) },
    ...(extra || []),
  ]);
}
function liveCells(live, pool) {
  const pools = live.pools.filter((p) => !pool || p.name === pool);
  const all = pools.flatMap((p) => p.backends);
  const count = (state) => all.filter((b) => b.state === state).length;
  const queued = pools.reduce((n, p) => n + p.queue.length, 0);
  const limit = pools.reduce((n, p) => n + p.queue_max, 0);
  const down = count('down');
  return [
    { label: 'Queued', value: `${int(queued)} / ${int(limit)}`, bad: limit > 0 && queued >= limit },
    { label: 'Replicas', value: `${all.length - down} up`, bad: down > 0,
      sub: `${count('busy')} busy · ${down} down${pools.some((p) => p.paused) ? ' · paused' : ''}` },
    { label: 'Clients', value: int(live.clients) },
  ];
}
function nowCell(live, pool) {
  const [queued, replicas, clients] = liveCells(live, pool);
  return { label: 'Now', value: replicas.value, bad: replicas.bad, sub: `${replicas.sub} · ${queued.value} queued · ${clients.value} clients` };
}

function samples(rows, empty, st, linked) {
  const dated = st.range[0] > 86400;
  return table([
    { title: 'Time', cell: (r) => stamp(r.ts, dated) },
    num('Wait', (r) => dur(r.wait_ms)), num('Exec', (r) => dur(r.ms)), num('Total', (r) => dur(r.ms + r.wait_ms)),
    { title: 'Outcome', cell: (r) => outcome(r.status) },
    { title: 'Replica', cell: (r) => r.backend || '—' }, { title: 'Client', cell: (r) => r.client },
    { title: 'Request', cls: 'q', cell: (r) => text(r.text, linked ? link('query/' + r.fp) : null) },
  ], rows, empty, true);
}
function poolsOf(live, pool) {
  return live.pools.filter((p) => !pool || p.name === pool);
}
function replicasNow(live, pool) {
  const rows = poolsOf(live, pool).flatMap((p) => p.backends.map((b) => ({ pool: p.name + (p.paused ? ' (paused)' : ''), ...b })));
  return table([
    { title: 'Replica', cell: (r) => r.addr }, { title: 'Pool', cell: (r) => r.pool }, { title: 'State', cell: replicaState },
    ...loadColumns,
    num('Running', (r) => r.running ? dur(r.running.ms) : notOurs(r) && r.status.running_ms != null ? dur(r.status.running_ms) : ''),
    { title: 'Request', cls: 'q one', cell: (r) => r.running || !notOurs(r) ? text(r.running ? r.running.text : '')
      : h('span', { class: 'dim', title: notOurs(r) }, 'not ours · ' + notOurs(r)) },
    num('Served', (r) => int(r.served)), num('Conn failures', (r) => int(r.failures)),
    { title: 'Last error', cell: (r) => r.last_error },
  ], rows, 'No replicas configured.', true);
}

// ---- views ----

const views = {
  async overview(st, live, q) {
    const data = await get('/api/overview' + q());
    return [totals(data.total, data.to - data.from, [nowCell(live, st.pool)], data.previous), threeCharts(data, st.range[2]),
      section('Replicas', null, replicasNow(live, st.pool))];
  },
  async live(st, live) {
    const queued = poolsOf(live, st.pool).flatMap((p) => p.queue.map((x) => ({ pool: p.name, ...x })));
    return [strip(liveCells(live, st.pool)), section('Replicas', null, replicasNow(live, st.pool)),
      section(`Queue (${queued.length})`, null, table([
        { title: 'Pool', cell: (r) => r.pool }, { title: 'Client', cell: (r) => r.client }, num('Waiting', (r) => dur(r.waited_ms)),
        { title: 'Request', cls: 'q one', cell: (r) => text(r.text) }], queued, 'Queue is empty.', true))];
  },
  async queries(st, live, q) {
    const sort = sessionStorage.sort || 'total';
    const data = await get('/api/top' + q({ sort, limit: 200 }));
    const by = (key) => ({ sort: () => { sessionStorage.sort = key; render(true); }, sorted: sort === key });
    const all = data.total;
    return [section(`${int(data.rows.length)} query shapes`, 'literals replaced by ?', table([
      { title: 'Query shape', cls: 'q', cell: (r) => text(r.text, link('query/' + r.fp)) },
      num('Requests', (r) => [int(r.count), delta(r.count, r.previous.count, int, 'new')], by('count')),
      num('Total exec', (r) => dur(r.total_ms), by('total')), num('%', (r) => pct(r.total_ms, all.total_ms)),
      sort === 'total' && { title: '', cell: (r) => share(r.total_ms, all.total_ms) },
      num('Avg ms', (r) => [msCell(r.avg_ms), r.previous.count ? delta(r.avg_ms, r.previous.avg_ms, (v) => msCell(v) + ' ms') : delta(0, 0)], by('avg')),
      num('p95 ms', (r) => msCell(r.p95_ms)), num('Max ms', (r) => msCell(r.max_ms), by('max')),
      num('Wait p95 ms', (r) => msCell(r.wait_p95_ms)),
      num('Failed', (r) => failedCell(r.not_ok, r.count), by('errors')), num('Bytes out', (r) => bytes(r.bytes_out), by('bytes')),
    ], data.rows, `No requests in ${st.range[2]}.`, true),
    h('p', { class: 'note' }, 'percentiles are histogram bucket upper bounds (powers of two) · ▲ ▼ against the period before, where it differs by a tenth or more'))];
  },
  async query(st, live, q) {
    const data = await get('/api/query' + q({ fp: st.arg, limit: 20 }));
    const shape = h('pre', { class: 'q' }, data.text || `No requests with this shape in ${st.range[2]}.`);
    const copy = h('button', { class: 'copy', onclick: () => {
      const select = () => getSelection().selectAllChildren(shape);
      if (navigator.clipboard) navigator.clipboard.writeText(data.text).then(() => { copy.textContent = 'copied'; }, select);
      else select();
    } }, 'copy');
    // Who sends this shape and which replicas run it. `<other>` is where
    // the bouncer counts those past its limit of rows a minute.
    const who = (title, first, rows) => {
      const total = rows.reduce((n, r) => n + r.count, 0);
      return section(title, rows.length >= 20 ? 'the 20 with the most requests' : null, table([
        { title: first, cell: (r) => r.name }, { title: 'Pool', cell: (r) => r.pool },
        num('Requests', (r) => int(r.count)), num('%', (r) => pct(r.count, total)), { title: '', cell: (r) => share(r.count, total) },
        num('Failed', (r) => failedCell(r.not_ok, r.count)), num('Total exec', (r) => dur(r.total_ms)), num('Avg ms', (r) => msCell(r.avg_ms)),
      ], rows, `No requests in ${st.range[2]}.`));
    };
    const kept = 'the bouncer keeps the slowest requests per hour and up to 20 failed per minute, with literals';
    return [h('section', { class: 'plain' },
        h('p', { class: 'note' }, h('a', { href: link('queries') }, '← Queries'), `   ${st.arg}   `, data.text ? copy : null), shape),
      totals(data.total, data.to - data.from, null, data.previous), threeCharts(data, st.range[2]),
      h('div', { class: 'pair' }, who('Clients', 'Address', data.clients), who('Replicas', 'Replica', data.backends)),
      section('Slowest samples', kept, samples(data.slow, `No requests in ${st.range[2]}.`, st, false)),
      section('Failed samples', null, samples(data.failed, `No failed requests in ${st.range[2]}.`, st, false))];
  },
  async slow(st, live, q) {
    const data = await get('/api/slow' + q({ limit: 100 }));
    return [section('Slowest requests', 'by exec time; kept per hour, with literals',
      samples(data.rows, `No requests in ${st.range[2]}.`, st, true))];
  },
  async failed(st, live, q) {
    const data = await get('/api/errors' + q({ limit: 200 }));
    const of = (name) => (data.outcomes.find((o) => o.status === name) || { count: 0 }).count;
    const total = data.outcomes.reduce((n, o) => n + o.count, 0);
    const codes = data.outcomes.filter((o) => o.status !== 'ok' && o.status !== 'cached' && !OUTCOMES.some(([name]) => name === o.status));
    const rows = OUTCOMES.flatMap(([name, meaning]) => name === 'error'
      ? [{ name, meaning, count: codes.reduce((n, c) => n + c.count, 0) }, ...codes.map((c) => ({ name: c.status, count: c.count, code: true }))]
      : [{ name, meaning, count: of(name) }]);
    // Not a failure, but an outcome: listed where the pool has a cache.
    if (of('cached')) rows.push({ name: 'cached', meaning: 'answered from the result cache, not a failure', count: of('cached') });
    const faded = (r, value) => h('span', { class: r.count ? '' : 'dim' }, value);
    return [section('Outcomes', `of ${int(total)} requests in ${st.range[2]}`, table([
        { title: 'Outcome', cls: (r) => r.code ? 'indent' : '', cell: (r) => faded(r, r.name) },
        num('Count', (r) => faded(r, int(r.count))),
        num('% of requests', (r) => faded(r, pct(r.count, total))),
        { title: 'Meaning', cell: (r) => h('span', { class: 'dim' }, r.meaning || '') },
      ], rows, '')),
      section('Failed requests, newest first', 'sampled: up to 20 per minute',
        samples(data.rows, `No failed requests in ${st.range[2]}.`, st, true))];
  },
  async clients(st, live, q) {
    const data = await get('/api/clients' + q({ limit: 200 }));
    const total = data.rows.reduce((n, r) => n + r.count, 0);
    return [section('Clients', null, table([
      { title: 'Address', cell: (r) => r.name }, { title: 'Pool', cell: (r) => r.pool },
      num('Requests', (r) => int(r.count)), num('%', (r) => pct(r.count, total)), { title: '', cell: (r) => share(r.count, total) },
      num('Failed', (r) => failedCell(r.not_ok, r.count)), num('Total exec', (r) => dur(r.total_ms)), num('Avg ms', (r) => msCell(r.avg_ms)),
    ], data.rows, `No clients in ${st.range[2]}.`))];
  },
  async replicas(st, live, q) {
    const data = await get('/api/backends' + q({ limit: 200 }));
    const total = data.rows.reduce((n, r) => n + r.count, 0);
    const none = { count: 0, not_ok: 0, avg_ms: 0 };
    const rows = poolsOf(live, st.pool).flatMap((p) => p.backends.map((b) => (
      { pool: p.name + (p.paused ? ' (paused)' : ''), ...b, ...(data.rows.find((r) => r.pool === p.name && r.name === b.addr) || none) })));
    return [section('Replicas', `requests are of ${st.range[2]}; state is now`, table([
      { title: 'Replica', cell: (r) => r.addr }, { title: 'Pool', cell: (r) => r.pool }, { title: 'State', cell: replicaState },
      ...loadColumns,
      num('Requests', (r) => int(r.count)), num('%', (r) => pct(r.count, total)), { title: '', cell: (r) => share(r.count, total) },
      num('Failed', (r) => failedCell(r.not_ok, r.count)), num('Avg ms', (r) => msCell(r.avg_ms)),
      num('Conn failures', (r) => int(r.failures)), { title: 'Last error', cell: (r) => r.last_error },
    ], rows, 'No replicas configured.'))];
  },
};

// ---- the frame: header, refresh, errors ----

let drawn = 0, ok = false, lastGood = null, paused = false;
const clockNow = () => { const d = new Date(); return `${hm(d)}:${two(d.getSeconds())}`; };
function status(kind, message) {
  $('status').className = kind;
  $('status').replaceChildren(h('i', {}), message);
}

// `changed` is true when the reader picked something else to look at; a
// refresh on the timer redraws in place, without a flicker.
async function render(changed) {
  const st = state();
  const mine = ++drawn;
  const tab = st.tab === 'query' ? 'queries' : st.tab;
  $('tabs').replaceChildren(...TABS.map(([id, title]) => h('a', { href: link(id), class: id === tab ? 'on' : '' }, title)));
  $('range').replaceChildren(...RANGES.map((r) => h('button', {
    class: r === st.range ? 'on' : '', onclick: () => { location.hash = link(here(st), { range: r }).slice(1); },
  }, r[1])));
  $('range').className = st.tab === 'live' ? 'off' : '';
  const to = Math.floor(Date.now() / 1000), from = to - st.range[0];
  const a = new Date(from * 1000), b = new Date(to * 1000);
  $('window').textContent = st.tab === 'live' ? '' : `${st.range[0] > 86400 ? md(a) + ' ' : ''}${hm(a)}–${hm(b)} ${zone()}`;
  document.title = `${(TABS.find(([id]) => id === tab) || TABS[0])[1]} · rfbouncer ${location.host}`;
  if (changed) $('view').classList.add('stale');
  try {
    const live = await get('/api/live');
    $('instance').textContent = `${location.host} · v${live.version} · up ${span(live.uptime_s)}`;
    $('pool').replaceChildren(h('option', { value: '' }, 'all'), ...live.pools.map((p) => h('option', { value: p.name }, p.name)));
    $('pool').value = live.pools.some((p) => p.name === st.pool) ? st.pool : '';
    $('alert').hidden = !live.stats_dropped;
    $('alert').textContent = `statistics dropped for ${int(live.stats_dropped)} requests: counts are low`;
    const q = (extra) => '?' + new URLSearchParams({ from, to, pool: st.pool, ...extra });
    const before = charts;
    charts = [];
    let body;
    try { body = await (views[st.tab] || views.overview)(st, live, q); } catch (e) { charts = before; throw e; }
    if (mine !== drawn) return;
    $('tooltip').hidden = true;
    $('view').replaceChildren(...body.filter(Boolean));
    $('view').classList.remove('broken');
    ok = true;
    lastGood = clockNow();
    status('live', `live · ${REFRESH_S} s · ${lastGood}`);
  } catch (e) {
    if (mine !== drawn) return;
    // What was on the page stays: it is the last thing known, and during an
    // incident that is worth more than an empty page.
    const gone = e.status == null;
    if (!ok || changed || e.status === 404) {
      $('view').replaceChildren(h('div', { class: 'empty' }, gone ? `Cannot reach rfbouncer at ${location.host}: ${e.message}` : e.message));
      ok = false;
    } else {
      $('view').classList.add('broken');
    }
    status('bad', gone ? `no answer from rfbouncer since ${lastGood || clockNow()} · retrying every ${REFRESH_S} s` : e.message);
  }
  $('view').classList.remove('stale');
}

addEventListener('hashchange', () => render(true));
$('pool').addEventListener('change', () => { location.hash = link(here(state()), { pool: $('pool').value }).slice(1); });
// A page being read is left alone: a tooltip open, text selected, or the
// tab in the background.
setInterval(() => {
  const busy = !$('tooltip').hidden || document.hidden || getSelection().toString() !== '';
  if (busy !== paused) {
    paused = busy;
    if (ok && lastGood) status(busy ? '' : 'live', `${busy ? 'paused' : 'live · ' + REFRESH_S + ' s'} · ${lastGood}`);
  }
  if (!busy) render(false);
}, REFRESH_S * 1000);
render(true);
