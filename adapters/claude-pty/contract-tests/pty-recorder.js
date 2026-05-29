// Deterministic PTY recorder for the claude-pty adapter contract tests.
// Acts as a drop-in for the wrapper's `write(text)` callback: records every
// character passed in, exposes the concatenated rendered text, supports reset.
// No node-pty dependency; tests run without native-module install.

'use strict';

function makeRecorder() {
  const writes = [];
  function write(chunk) { writes.push({ chunk, ts: Date.now() }); }
  function rendered() { return writes.map(w => w.chunk).join(''); }
  function lines() { return rendered().split('\r').filter(Boolean); }
  function reset() { writes.length = 0; }
  return { write, writes, rendered, lines, reset };
}

module.exports = { makeRecorder };
