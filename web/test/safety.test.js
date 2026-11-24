// Every string from a client or a replica must enter the page as text. The
// script has no way to make markup out of a string as long as none of these
// appear in it, so their absence is checked, and so is what `h` does with a
// string that spells markup.
'use strict';
process.env.TZ = 'UTC';

const test = require('node:test');
const assert = require('node:assert/strict');
const { load, source } = require('./support.js');

test('the script never turns a string into markup or code', () => {
  for (const banned of [/innerHTML/, /outerHTML/, /insertAdjacentHTML/, /document\s*\.\s*write/, /\beval\s*\(/,
    /new\s+Function/, /createContextualFragment/, /DOMParser/, /srcdoc/, /setTimeout\s*\(\s*['"`]/]) {
    assert.doesNotMatch(source, banned);
  }
});

test('a string that spells markup becomes a text node', () => {
  const app = load();
  const evil = '<img src=x onerror=alert(1)>';
  const div = app.h('div', { title: evil }, evil, [evil, null, false]);
  assert.equal(div.children.length, 2);
  for (const child of div.children) {
    assert.equal(child.nodeType, 3);
    assert.equal(child.textContent, evil);
  }
  assert.equal(div.all('img').length, 0);
});

test('a request text and an outcome reach a table cell as text', () => {
  const app = load();
  const evil = '<script>alert(1)</script>';
  const table = app.table([{ title: 'Request', cell: (r) => app.text(r.text) }, { title: 'Outcome', cell: (r) => app.outcome(r.status) }],
    [{ text: evil, status: evil }], 'none');
  assert.equal(table.all('script').length, 0);
  const cells = table.all('td');
  assert.equal(cells.length, 2);
  assert.equal(cells[0].textContent, evil);
  assert.equal(cells[1].textContent, 'error: ' + evil);
});

test('a table without rows is its empty message', () => {
  const app = load();
  const empty = app.table([{ title: 'A', cell: () => 'x' }], [], 'Nothing here.');
  assert.equal(empty.className, 'empty');
  assert.equal(empty.textContent, 'Nothing here.');
});

test('an outcome is told in the data\u2019s own words', () => {
  const app = load();
  assert.equal(app.outcome('ok').className, 'dim');
  assert.equal(app.outcome('cancel'), 'cancel');
  assert.equal(app.outcome('timeout').textContent, 'timeout');
  assert.equal(app.outcome('timeout').className, 'bad');
  assert.equal(app.outcome('name').textContent, 'error: name');
});
