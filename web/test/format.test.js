// How the page writes numbers, durations, sizes, shares and moments.
'use strict';
process.env.TZ = 'UTC';

const test = require('node:test');
const assert = require('node:assert/strict');
const { load } = require('./support.js');

const NOW = Date.UTC(2026, 10, 24, 12, 0, 0);
const app = load({ now: NOW });

test('counts are whole and grouped by thousands', () => {
  assert.equal(app.int(0), '0');
  assert.equal(app.int(999.4), '999');
  assert.equal(app.int(1234567.4), '1,234,567');
  assert.equal(app.two(7), '07');
  assert.equal(app.two(12), '12');
});

test('three significant figures, without trailing zeros', () => {
  assert.equal(app.sig3(1234.5), '1230');
  assert.equal(app.sig3(0.012345), '0.0123');
  assert.equal(app.sig3(2), '2');
  assert.equal(app.sig3(1.5), '1.5');
});

test('a duration takes the unit that fits it', () => {
  assert.equal(app.dur(0), '0 ms');
  // A field the server left out is nothing, not NaN.
  assert.equal(app.dur(undefined), '0 ms');
  assert.equal(app.dur(0.5), '500 µs');
  assert.equal(app.dur(0.0001234), '0.123 µs');
  assert.equal(app.dur(1), '1 ms');
  assert.equal(app.dur(12.345), '12.3 ms');
  assert.equal(app.dur(999), '999 ms');
  assert.equal(app.dur(1000), '1 s');
  assert.equal(app.dur(1536), '1.54 s');
  assert.equal(app.dur(60000), '1 m 00 s');
  assert.equal(app.dur(3600000), '1 h 00 m');
});

test('a duration just under a unit rounds to the same amount, in either unit', () => {
  // Rounding to three figures can reach the next unit's first value; both
  // spellings say the same duration.
  assert.equal(app.dur(999.5), '1 s');
  assert.match(app.dur(0.9996), /^(1000 µs|1 ms)$/);
  assert.equal(app.dur(59999), '1 m 00 s');
});

test('a span of seconds names its two largest units', () => {
  assert.equal(app.span(0), '0 s');
  assert.equal(app.span(59.9), '59 s');
  assert.equal(app.span(60), '1 m 00 s');
  assert.equal(app.span(3599), '59 m 59 s');
  assert.equal(app.span(3600), '1 h 00 m');
  assert.equal(app.span(86399), '23 h 59 m');
  assert.equal(app.span(86400), '1 d 0 h');
  assert.equal(app.span(90061), '1 d 1 h');
});

test('a millisecond cell keeps fewer decimals as it grows', () => {
  assert.equal(app.msCell(0), '0.00');
  assert.equal(app.msCell(1.234), '1.23');
  assert.equal(app.msCell(12.34), '12.3');
  assert.equal(app.msCell(123.4), '123');
  assert.equal(app.msCell(12345.6), '12,346');
});

test('bytes go up by 1024 and stop at TiB', () => {
  assert.equal(app.bytes(0), '0 B');
  assert.equal(app.bytes(1023), '1023 B');
  assert.equal(app.bytes(1024), '1.0 KiB');
  assert.equal(app.bytes(1536), '1.5 KiB');
  assert.equal(app.bytes(5 * 1024 ** 3), '5.0 GiB');
  assert.equal(app.bytes(5000 * 1024 ** 4), '5000.0 TiB');
});

test('a share of nothing is zero, and a small one is not shown as zero', () => {
  assert.equal(app.pct(0, 10), '0%');
  assert.equal(app.pct(5, 0), '0%');
  assert.equal(app.pct(undefined, undefined), '0%');
  assert.equal(app.pct(1, 10000), '<0.1%');
  assert.equal(app.pct(1, 1000), '0.1%');
  assert.equal(app.pct(1, 3), '33.3%');
  assert.equal(app.pct(7, 7), '100.0%');
});

test('a moment is written to the millisecond, with its date unless it is of today', () => {
  const then = Date.UTC(2026, 0, 2, 3, 4, 5, 6);
  assert.equal(app.stamp(then, true), '01-02 03:04:05.006');
  assert.equal(app.stamp(then, false), '01-02 03:04:05.006');
  assert.equal(app.stamp(NOW - 3600000 + 42, false), '11:00:00.042');
  assert.equal(app.stamp(NOW - 3600000 + 42, true), '11-24 11:00:00.042');
});

test('the zone is named by its offset from UTC', () => {
  const zoneIn = (tz) => {
    process.env.TZ = tz;
    try { return load().zone(); } finally { process.env.TZ = 'UTC'; }
  };
  assert.equal(zoneIn('UTC'), 'UTC');
  assert.equal(zoneIn('Asia/Kolkata'), 'UTC+5:30');
  // West of Greenwich the sign is a real minus, U+2212.
  assert.equal(zoneIn('Etc/GMT+3'), 'UTC\u22123');
});

// A number that rounds up to the next unit is written in that unit: the
// page once said `1000 ms`, `60 s` and `1024.0 KiB`.
test('no duration or size is written as a full unit of the one below', () => {
  assert.equal(app.dur(0.9996), '1 ms');
  assert.equal(app.dur(0.99949), '999 µs');
  assert.equal(app.dur(999.4), '999 ms');
  assert.equal(app.dur(999.6), '1 s');
  assert.equal(app.dur(59940), '59.9 s');
  assert.equal(app.dur(59950), '1 m 00 s');
  assert.equal(app.dur(60000), '1 m 00 s');
  assert.equal(app.bytes(1023), '1023 B');
  assert.equal(app.bytes(1024 * 1024 - 1), '1.0 MiB');
  assert.equal(app.bytes(1024 * 1023.9), '1023.9 KiB');
  assert.equal(app.bytes(1024 ** 3 - 1), '1.0 GiB');
  assert.equal(app.msCell(99.94), '99.9');
  assert.equal(app.msCell(99.96), '100');
  assert.equal(app.msCell(9.994), '9.99');
  assert.equal(app.msCell(9.996), '10.0');
  // Swept, so that no value between the cases slips through.
  for (let e = -4; e < 9; e += 0.001) {
    const ms = 10 ** e;
    assert.doesNotMatch(app.dur(ms), /^(1000 (µs|ms)|60 s)$/, `dur(${ms})`);
    assert.doesNotMatch(app.bytes(Math.round(ms * 1000)), /^1024/, `bytes(${Math.round(ms * 1000)})`);
    assert.doesNotMatch(app.msCell(ms), /^(10\.00|100\.0)$/, `msCell(${ms})`);
  }
});

// The period before: which way a number moved and by how much.
test('a change against the period before is said only when it is one', () => {
  const said = (now, was, fresh) => app.delta(now, was, app.int, fresh).textContent;
  assert.equal(said(100, 100), '');
  assert.equal(said(105, 100), '');
  assert.equal(said(92, 100), '');
  assert.equal(said(0, 0), '');
  assert.equal(said(undefined, undefined), '');
  assert.equal(said(150, 100), '▲ 50%');
  assert.equal(said(50, 100), '▼ 50%');
  assert.equal(said(250, 100), '▲ 2.5×');
  assert.equal(said(100000, 100), '▲ 1000×');
  assert.equal(said(0, 40), '▼ from 40');
  assert.equal(said(40, 0), '▲ from 0');
  assert.equal(said(40, 0, 'new'), 'new');
  const el = app.delta(150, 100, app.dur);
  assert.equal(el.className, 'delta');
  assert.equal(el.title, '100 ms in the period before');
  // Never a NaN or an Infinity on the page, whatever the two numbers are.
  for (const now of [0, 1e-9, 1, 3, 1e12]) for (const was of [0, 1e-9, 1, 3, 1e12]) assert.doesNotMatch(said(now, was), /NaN|Infinity/);
});

// What a replica says of itself over its status stream.
test('a replica\u2019s own status fills the load columns, and nothing where it says nothing', () => {
  const [load, cpu, memory] = app.loadColumns.map((c) => c.cell);
  const silent = { state: 'idle' };
  assert.deepEqual([load(silent), cpu(silent), memory(silent)], ['', '', '']);
  const r = { state: 'busy', status: { util: 0.456, cpu: 1.5, cores: 8, rss_bytes: 3 * 1024 ** 3, mem_share: 0.75, not_ours: false } };
  assert.deepEqual([load(r), cpu(r), memory(r)], ['45.6%', '1.50 / 8', '3.0 GiB · 75.0%']);
  // No limit, no share; no second frame yet, no rates.
  const first = { status: { util: null, cpu: null, cores: 8, rss_bytes: 1024, mem_share: null } };
  assert.deepEqual([load(first), cpu(first), memory(first)], ['', '', '1.0 KiB']);
});

test('work the bouncer did not send is named, and its text stays text', () => {
  assert.equal(app.notOurs({}), '');
  assert.equal(app.notOurs({ status: { not_ours: false, text: 'x' } }), '');
  assert.equal(app.notOurs({ status: { not_ours: true, state: 'other', text: '' } }), 'other work');
  assert.equal(app.notOurs({ status: { not_ours: true, state: 'request', text: '' } }), 'a request');
  const markup = '<img src=x onerror=alert(1)>';
  const cell = app.replicaState({ state: 'busy', status: { not_ours: true, state: 'request', text: markup } });
  assert.equal(cell.textContent, 'busy · not ours');
  assert.equal(cell.all('img').length, 0);
  assert.equal(cell.all('span')[0].getAttribute('title'), markup);
  // Down wins over draining, and draining over what the replica is doing.
  assert.equal(app.replicaState({ state: 'down', draining: true }).textContent, 'down');
  assert.equal(app.replicaState({ state: 'idle', draining: true }).textContent, 'draining');
});
