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
  assert.match(app.dur(999.5), /^(1000 ms|1 s)$/);
  assert.match(app.dur(0.9996), /^(1000 µs|1 ms)$/);
  assert.match(app.dur(59999), /^(60 s|1 m 00 s)$/);
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
