// What is shown is picked by the address alone: reading it and writing it
// must agree.
'use strict';
process.env.TZ = 'UTC';

const test = require('node:test');
const assert = require('node:assert/strict');
const { load, plain } = require('./support.js');

const at = (hash) => {
  const app = load({ hash });
  return { app, st: plain(app.state()) };
};

test('an empty address is the overview of the last hour, every pool', () => {
  const { st } = at('');
  assert.deepEqual(st, { tab: 'overview', pool: '', range: [3600, '1h', 'the last hour'] });
});

test('the tab, its argument, the pool and the range are read off the address', () => {
  const { st } = at('#query/00ff00ff00ff00ff?pool=main&range=900');
  assert.equal(st.tab, 'query');
  assert.equal(st.arg, '00ff00ff00ff00ff');
  assert.equal(st.pool, 'main');
  assert.equal(st.range[0], 900);
});

test('a range the page does not offer falls back to the last hour', () => {
  for (const range of ['123', 'x', '', '-900', '900.5', '1e9']) {
    assert.equal(at(`#slow?range=${range}`).st.range[0], 3600, range);
  }
});

test('a link keeps the pool and the range, and leaves the defaults out', () => {
  const { app } = at('#slow?pool=main&range=86400');
  assert.equal(app.link('failed'), '#failed?pool=main&range=86400');
  assert.equal(app.link('failed', { pool: '' }), '#failed?range=86400');
  assert.equal(app.link('failed', { pool: '', range: app.RANGES[1] }), '#failed');
  assert.equal(app.here(app.state()), 'slow');
  assert.equal(at('#query/ab').app.here({ tab: 'query', arg: 'ab' }), 'query/ab');
});

test('what a link says is what the page reads back from it', () => {
  const { app } = at('');
  for (const pool of ['', 'main', 'a b', 'a&b=c', 'a#b?c/d', 'ü%41+', '"><script>']) {
    for (const range of app.RANGES) {
      app.location.hash = app.link('query/00ff', { pool, range });
      const st = app.state();
      assert.equal(st.tab, 'query');
      assert.equal(st.arg, '00ff');
      assert.equal(st.pool, pool);
      assert.equal(st.range[0], range[0]);
    }
  }
});
