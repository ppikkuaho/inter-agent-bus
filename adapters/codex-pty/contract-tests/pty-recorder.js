// Deterministic PTY recorder for the codex-pty adapter contract tests.

'use strict';

function makeRecorder() {
  const writes = [];
  return {
    writes,
    write(chunk) {
      writes.push(String(chunk));
    },
    rendered() {
      return writes.join('');
    },
    lines() {
      return writes.join('').split('\r').filter(Boolean);
    },
  };
}

module.exports = { makeRecorder };
