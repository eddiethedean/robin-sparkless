'use strict';

const path = require('path');
const fs = require('fs');

// Resolve the native addon: prefer same dir (npm pack / install), else workspace target
const candidates = [
  path.join(__dirname, 'robin_sparkless_node.node'),
  path.join(__dirname, 'librobin_sparkless_node.node'),
  path.join(__dirname, '..', 'target', 'release', 'librobin_sparkless_node.dylib'),
  path.join(__dirname, '..', 'target', 'release', 'librobin_sparkless_node.so'),
  path.join(__dirname, '..', 'target', 'release', 'robin_sparkless_node.dll'),
];

let binding = null;
for (const p of candidates) {
  try {
    if (fs.existsSync(p)) {
      binding = require(p);
      break;
    }
  } catch (_) {
    // continue
  }
}

if (!binding) {
  throw new Error(
    'robin-sparkless-node: native addon not found. Run `npm run build` from the repo root (or `cargo build -p robin-sparkless-node --release`).'
  );
}

module.exports = binding;
