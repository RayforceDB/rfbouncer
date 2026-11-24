// Loads web/app.js, a browser script, into a context of its own with just
// enough of a page for it to start. The script is not changed: its top-level
// names are handed out by one line added to the text before it runs, since a
// `const` at the top of a script is not a property of the global object.
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const APP = path.join(__dirname, '..', 'app.js');
const source = fs.readFileSync(APP, 'utf8');

const NAMES = ['TABS', 'RANGES', 'OUTCOMES', 'H', 'L', 'R', 'T', 'B', 'h', 's', 'int', 'sig3', 'dur', 'span', 'msCell',
  'bytes', 'pct', 'two', 'hm', 'md', 'stamp', 'zone', 'state', 'link', 'here', 'table', 'text', 'outcome', 'failedCell',
  'share', 'niceStep', 'timeTicks', 'chart', 'draw', 'delta', 'notOurs', 'replicaState', 'loadColumns', 'totals',
  'replicasNow'];

class Node {
  constructor(tag, ns) {
    this.nodeType = 1;
    this.tagName = tag;
    this.namespace = ns || null;
    this.attrs = {};
    this.children = [];
    this.listeners = {};
    this.style = {};
    this.className = '';
    this.hidden = false;
    this.clientWidth = 0;
    this.classList = { add: () => {}, remove: () => {} };
  }
  setAttribute(k, v) { this.attrs[k] = v; }
  getAttribute(k) { return this.attrs[k]; }
  addEventListener(type, fn) { this.listeners[type] = fn; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  getBoundingClientRect() { return { left: 0, top: 0, width: this.clientWidth, height: 0 }; }
  get textContent() { return this.children.map((c) => c.textContent).join(''); }
  set textContent(v) { this.children = [new Text(v)]; }
  // Every element below this one, in document order, with this tag.
  all(tag) { return this.children.flatMap((c) => c.nodeType === 1 ? [...(c.tagName === tag ? [c] : []), ...c.all(tag)] : []); }
}

// A text node holds a string and nothing else: whatever markup the string
// spells stays a string.
class Text {
  constructor(text) {
    this.nodeType = 3;
    this.textContent = String(text);
  }
}

// `hash` is the address's fragment and `now` the clock, in milliseconds: the
// script asks for the time in several places, and a test must not depend on
// when it runs.
function load({ hash = '', now = Date.UTC(2026, 10, 24, 12, 0, 0) } = {}) {
  const byId = {};
  const observers = [];
  const context = vm.createContext({
    URLSearchParams,
    console,
    __now: now,
    document: {
      createElement: (tag) => new Node(tag),
      createElementNS: (ns, tag) => new Node(tag, ns),
      createTextNode: (text) => new Text(text),
      getElementById: (id) => byId[id] || (byId[id] = new Node('div')),
      hidden: false,
      title: '',
    },
    location: { hash, host: 'test:5000' },
    addEventListener: () => {},
    setInterval: () => 0,
    fetch: () => Promise.reject(new Error('no network in a test')),
    ResizeObserver: class {
      constructor(callback) { this.callback = callback; observers.push(this); }
      observe(el) { this.el = el; }
    },
    sessionStorage: {},
    getSelection: () => ({ toString: () => '', selectAllChildren: () => {} }),
    navigator: {},
    innerWidth: 1280,
  });
  vm.runInContext(`{
    const Real = Date;
    globalThis.Date = class extends Real {
      constructor(...args) { if (args.length) super(...args); else super(__now); }
      static now() { return __now; }
    };
  }`, context);
  const exports = NAMES.join(', ') + ', get charts() { return charts; }';
  vm.runInContext(`${source}\n;globalThis.__app = { ${exports} };`, context, { filename: APP });
  return Object.assign(context.__app, { location: context.location, byId, observers, Node, Text });
}

// Values made inside the context have its own Array and Object, which a
// strict comparison tells from ours; this brings them over.
const plain = (value) => JSON.parse(JSON.stringify(value));

module.exports = { load, plain, source, Node, Text };
