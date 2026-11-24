// The charts are drawn by hand, so their arithmetic is checked here: where
// the axis lines fall, and that every mark lands inside the plot.
'use strict';
process.env.TZ = 'UTC';

const test = require('node:test');
const assert = require('node:assert/strict');
const { load } = require('./support.js');

const NOW = Date.UTC(2026, 10, 24, 12, 1, 0);
const TO = NOW / 1000, FROM = TO - 3600;
const app = load({ now: NOW });
const { H, L, R, T, B } = app;
const W = 640, EPS = 1e-6;

test('a grid step is 1, 2 or 5 times a power of ten, and no smaller than asked', () => {
  for (let e = -9; e <= 15; e++) {
    for (const m of [1, 1.0001, 1.5, 2, 2.5, 3, 4.999, 5, 6, 7.5, 9.999]) {
      for (const lines of [1, 2, 3, 4, 5, 7]) {
        const max = m * 10 ** e, raw = max / lines;
        const step = app.niceStep(max, lines);
        assert.ok(step >= raw, `niceStep(${max}, ${lines}) = ${step}`);
        assert.ok(step < raw * 10 * (1 + 1e-9), `niceStep(${max}, ${lines}) = ${step} is a decade too large`);
        const lead = step / 10 ** Math.floor(Math.log10(step) + 1e-9);
        assert.ok([1, 2, 5, 10].some((v) => Math.abs(lead - v) < 1e-6), `niceStep(${max}, ${lines}) = ${step}`);
      }
    }
  }
});

test('time ticks are inside the range, evenly spaced, on round times, and few', () => {
  for (const [len] of app.RANGES) {
    for (const end of [TO, TO + 1, TO + 299, TO + 12345, Date.UTC(2026, 0, 1) / 1000, Date.UTC(2026, 0, 1) / 1000 - 1]) {
      const from = end - len;
      const ticks = app.timeTicks(from, end);
      assert.ok(ticks.length >= 1 && ticks.length <= 8, `${len}: ${ticks.length} ticks`);
      const every = ticks.length > 1 ? ticks[1][0] - ticks[0][0] : null;
      ticks.forEach(([t, label], i) => {
        assert.ok(t >= from && t <= end, `${len}: tick ${t} outside ${from}..${end}`);
        if (i) assert.equal(t - ticks[i - 1][0], every);
        if (every) assert.equal(t % every, 0);
        assert.match(label, len > 86400 ? /^\d\d-\d\d$/ : /^\d\d:\d\d$/);
      });
    }
  }
});

test('time ticks fall on round times of the reader\u2019s zone, not of UTC', () => {
  process.env.TZ = 'Asia/Kolkata';
  try {
    const local = load({ now: NOW });
    const hours = local.timeTicks(TO - 21600, TO);
    assert.ok(hours.length >= 5);
    for (const [t, label] of hours) {
      assert.match(label, /^\d\d:00$/);
      // Half past an hour of UTC is a whole hour at +5:30.
      assert.equal(t % 3600, 1800);
    }
    for (const [, label] of local.timeTicks(TO - 900, TO)) assert.match(label, /^\d\d:\d[05]$/);
    const days = local.timeTicks(TO - 604800, TO);
    for (const [t] of days) assert.equal(local.hm(new Date(t * 1000)), '00:00');
  } finally {
    process.env.TZ = 'UTC';
  }
});

// An hour in minute steps: `of(i)` gives the point of step i, or null for a
// step without requests.
function hour(of) {
  const points = [];
  for (let i = 0; i < 60; i++) {
    const p = of(i);
    if (p) points.push({ t: FROM + i * 60, ...p });
  }
  return { from: FROM, to: TO, step_s: 60, points };
}
const num = (el, name) => {
  const v = Number(el.getAttribute(name));
  assert.ok(Number.isFinite(v), `${el.tagName} ${name} = ${el.getAttribute(name)}`);
  return v;
};
const bars = (svg) => svg.all('rect').filter((r) => r.getAttribute('class') !== 'cross');
const axisLabels = (svg) => svg.all('text').filter((t) => t.getAttribute('text-anchor') === 'end').map((t) => Number(t.textContent));
const twoSeries = [{ name: 'ok', color: '--a', of: (p) => p.ok }, { name: 'failed', color: '--b', of: (p) => p.failed }];

test('every bar is inside the plot and stacks without a gap', () => {
  for (const scale of [0.004, 0.3, 1, 7, 12345, 3e9]) {
    const data = hour((i) => i % 7 === 3 ? null : { ok: scale * (1 + i % 5), failed: i % 4 ? 0 : scale * 0.3 });
    const svg = app.draw(W, data, data.points, { bars: true, format: String, series: twoSeries }, {});
    assert.equal(svg.tagName, 'svg');
    assert.equal(svg.all('path').length, 0);
    const rects = bars(svg);
    const expected = data.points.reduce((n, p) => n + (p.ok > 0) + (p.failed > 0), 0);
    assert.equal(rects.length, expected);
    const tops = new Map();
    for (const r of rects) {
      const [x, y, w, hgt] = ['x', 'y', 'width', 'height'].map((a) => num(r, a));
      assert.ok(w >= 1 && hgt > 0, `a bar of ${w} by ${hgt}`);
      assert.ok(x >= L && x + w <= W - R + EPS, `bar ${x}..${x + w} outside ${L}..${W - R}`);
      assert.ok(y >= T - EPS && y + hgt <= H - B + 0.5 + EPS, `bar ${y}..${y + hgt} outside ${T}..${H - B}`);
      // The second series of a step starts where the first one ended.
      if (tops.has(x)) assert.ok(Math.abs(y + hgt - tops.get(x)) < 0.5 + EPS);
      tops.set(x, y);
    }
    // The axis starts at zero, in equal steps, and reaches past the data.
    const ticks = axisLabels(svg);
    const top = Math.max(...data.points.map((p) => p.ok + p.failed));
    assert.equal(ticks[0], 0);
    assert.ok(ticks.length >= 3 && ticks.length <= 5, `${ticks.length} grid lines`);
    assert.ok(ticks[ticks.length - 1] >= top * (1 - 1e-9));
    for (let i = 2; i < ticks.length; i++) assert.ok(Math.abs(ticks[i] - ticks[i - 1] - ticks[1]) < ticks[1] * 1e-9);
  }
});

test('a step still being filled is drawn fainter, and no other', () => {
  const data = hour(() => ({ ok: 5, failed: 0 }));
  const spec = { bars: true, format: String, series: twoSeries };
  // With the clock at the end of the last step, every step is complete.
  assert.equal(bars(app.draw(W, data, data.points, spec, {})).filter((r) => r.getAttribute('opacity') != null).length, 0);
  // Half a minute earlier, the last one is not.
  const svg = load({ now: NOW - 30000 }).draw(W, data, data.points, spec, {});
  const faint = bars(svg).filter((r) => r.getAttribute('opacity') != null);
  assert.equal(faint.length, 1);
  assert.equal(num(faint[0], 'x'), Math.max(...bars(svg).map((r) => num(r, 'x'))));
});

test('a line chart has one path per series, with every coordinate inside the plot', () => {
  const series = ['p99', 'p95', 'p50'].map((name, k) => ({ name, color: `--s${k}`, of: (p) => p[name] }));
  const data = hour((i) => i % 9 === 4 ? null : { p99: 2 ** (10 + i % 6), p95: 2 ** (4 + i % 3), p50: i % 5 ? 0.064 : 0 });
  const svg = app.draw(W, data, data.points, { log: true, format: String, series }, {});
  assert.equal(bars(svg).length, 0);
  const paths = svg.all('path');
  assert.equal(paths.length, series.length);
  paths.forEach((path, k) => {
    assert.equal(path.getAttribute('stroke'), `var(--s${k})`);
    const d = path.getAttribute('d');
    assert.match(d, /^(M[\d.]+ [\d.]+H[\d.]+|V[\d.]+H[\d.]+)+$/);
    for (const [, cmd, a, b] of d.matchAll(/([MVH])([\d.]+)(?: ([\d.]+))?/g)) {
      const xs = cmd === 'H' ? [a] : cmd === 'M' ? [a] : [];
      const ys = cmd === 'V' ? [a] : cmd === 'M' ? [b] : [];
      for (const x of xs) assert.ok(+x >= L - 0.05 && +x <= W - R + 0.05, `x ${x}`);
      for (const y of ys) assert.ok(+y >= T - 0.05 && +y <= H - B + 0.05, `y ${y}`);
    }
  });
  // Each series is named once at the right edge, and the names do not overlap.
  const ends = svg.all('text').filter((t) => t.getAttribute('class') === 'end');
  assert.deepEqual(ends.map((t) => t.textContent).sort(), ['p50', 'p95', 'p99']);
  const ys = ends.map((t) => num(t, 'y')).sort((a, b) => a - b);
  for (let i = 1; i < ys.length; i++) assert.ok(ys[i] - ys[i - 1] >= 13 - EPS);
});

test('the lines of a log axis are decades that hold the data', () => {
  for (const [low, high] of [[0.064, 32768], [1, 1], [3, 7], [0.002, 0.004], [10, 1000], [512, 2 ** 31 / 1000]]) {
    const series = [{ name: 'hi', color: '--a', of: (p) => p.hi }, { name: 'lo', color: '--b', of: (p) => p.lo }];
    const data = hour((i) => ({ hi: high, lo: i % 2 ? low : 0 }));
    const svg = app.draw(W, data, data.points, { log: true, format: String, series }, {});
    const ticks = axisLabels(svg);
    assert.ok(ticks.length >= 2, `${low}..${high}: ${ticks}`);
    for (const v of ticks) {
      const e = Math.log10(v);
      assert.ok(Math.abs(e - Math.round(e)) < 1e-9, `${v} is not a power of ten`);
    }
    for (let i = 1; i < ticks.length; i++) assert.ok(Math.abs(ticks[i] / ticks[i - 1] - 10) < 1e-6);
    assert.ok(ticks[0] <= low * (1 + 1e-9) && ticks[ticks.length - 1] >= high * (1 - 1e-9), `${low}..${high}: ${ticks}`);
    for (const line of svg.all('line')) for (const a of ['x1', 'x2', 'y1', 'y2']) num(line, a);
  }
});

test('series that are zero throughout draw no chart, only the reason', () => {
  const data = hour(() => ({ ok: 0, failed: 0 }));
  for (const points of [data.points, []]) {
    const before = app.charts.length;
    const out = app.chart({ ...data, points }, { bars: true, format: String, series: twoSeries, none: 'No requests.' });
    assert.equal(out.className, 'empty');
    assert.equal(out.textContent, 'No requests.');
    assert.equal(out.all('svg').length, 0);
    assert.equal(app.charts.length, before);
    assert.equal(app.observers.length, 0);
  }
});

test('a chart is drawn once its box has a width, with a legend for stacked bars', () => {
  const data = hour((i) => ({ ok: i, failed: 1 }));
  const out = app.chart(data, { bars: true, format: String, series: twoSeries, none: 'none' });
  const [box, legend] = out.children;
  assert.equal(out.all('svg').length, 0);
  assert.equal(legend.all('i').length, 2);
  const observer = app.observers[app.observers.length - 1];
  assert.equal(observer.el, box);
  observer.callback();
  assert.equal(out.all('svg').length, 0);
  box.clientWidth = W;
  observer.callback();
  assert.equal(out.all('svg').length, 1);
  assert.equal(box.children[0].getAttribute('viewBox'), `0 0 ${W} ${H}`);
  // Points where every series is zero are left out; the first step here
  // still has a failure, so all sixty are drawn.
  assert.equal(bars(box.children[0]).length, 60 + 59);
});

test('the step under the pointer is marked inside the plot, and unmarked again', () => {
  const data = hour(() => ({ ok: 1, failed: 0 }));
  const self = {};
  const svg = app.draw(W, data, data.points, { bars: true, format: String, series: twoSeries }, self);
  const cross = svg.all('rect').find((r) => r.getAttribute('class') === 'cross');
  assert.equal(cross.getAttribute('visibility'), 'hidden');
  self.mark(data.points[30].t);
  assert.equal(cross.getAttribute('visibility'), 'visible');
  assert.ok(num(cross, 'x') >= L && num(cross, 'x') + num(cross, 'width') <= W - R + EPS);
  self.mark(FROM + 7);
  assert.equal(cross.getAttribute('visibility'), 'hidden');
});

// The server answers with a point at `t === to` whenever `to` falls on the
// edge of a step. Its bar was once drawn at W - R + 0.5, past the plot.
test('a step that starts where the range ends is inside the plot too', () => {
  const data = { from: FROM, to: TO, step_s: 60, points: [{ t: TO - 60, ok: 1, failed: 0 }, { t: TO, ok: 1, failed: 0 }] };
  const self = {};
  const svg = app.draw(W, data, data.points, { bars: true, format: String, series: twoSeries }, self);
  const rects = bars(svg);
  assert.equal(rects.length, 2);
  for (const r of rects) {
    assert.ok(num(r, 'x') >= L && num(r, 'x') + num(r, 'width') <= W - R + EPS, `bar ${num(r, 'x')}..${num(r, 'x') + num(r, 'width')}, plot ${L}..${W - R}`);
  }
  // And so is the mark that follows the pointer onto it.
  self.mark(TO);
  const cross = svg.all('rect').find((r) => r.getAttribute('class') === 'cross');
  assert.ok(num(cross, 'x') >= L && num(cross, 'x') + num(cross, 'width') <= W - R + EPS);
});

// Only a hand-made answer has a range of no length, but the page must not
// write NaN into its drawing for one.
test('a range of no length is drawn without a NaN in it', () => {
  for (const spec of [{ bars: true, series: twoSeries }, { log: true, series: twoSeries }]) {
    const data = { from: TO, to: TO, step_s: 60, points: [{ t: TO, ok: 3, failed: 1 }] };
    const svg = app.draw(W, data, data.points, { ...spec, format: String }, {});
    for (const tag of ['rect', 'line', 'text', 'path']) {
      for (const el of svg.all(tag)) {
        for (const [name, value] of Object.entries(el.attrs)) assert.doesNotMatch(String(value), /NaN|Infinity/, `${tag} ${name}=${value}`);
      }
    }
    for (const r of bars(svg)) assert.ok(num(r, 'x') >= L && num(r, 'x') + num(r, 'width') <= W - R + EPS);
  }
});
