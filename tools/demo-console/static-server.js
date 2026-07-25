#!/usr/bin/env node
// Tiny dependency-free static file server for the demo console page.
// No npm install, no framework — just Node's built-in http/fs modules.
//
// Usage: node static-server.js [port] [rootDir]
//   port    default 8842
//   rootDir default: the directory this script lives in
//
// Binds to 127.0.0.1 only (see README security notes).

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.argv[2] || process.env.DEMO_CONSOLE_PORT || 8842);
// realpathSync so the containment checks below compare fully-resolved paths.
// On macOS /tmp is itself a symlink to /private/tmp, so an unresolved ROOT
// would make every single request look like an escape attempt.
const ROOT = fs.realpathSync(path.resolve(process.argv[3] || __dirname));
const HOST = '127.0.0.1';

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

// A bare startsWith(ROOT) is not containment: a SIBLING directory sharing the
// prefix (ROOT ".../demo-console" vs ".../demo-console-secrets/x") passes it.
// Require the path separator, or an exact match on the root itself.
function withinRoot(p) {
  return p === ROOT || p.startsWith(ROOT + path.sep);
}

function safeJoin(root, urlPath) {
  const decoded = decodeURIComponent(urlPath.split('?')[0]);
  const target = path.normalize(path.join(root, decoded));
  if (!withinRoot(target)) return null; // block path traversal
  return target;
}

// Second gate: the string check above cannot see through symlinks, and a link
// INSIDE the root can point anywhere on disk. Resolve the real path and
// re-check containment before reading. A missing file resolves to null (ENOENT
// is expected, not exceptional) so the caller falls back / 404s cleanly.
function resolveInsideRoot(filePath, cb) {
  fs.realpath(filePath, (err, real) => {
    if (err) {
      cb(null);
      return;
    }
    cb(withinRoot(real) ? real : null);
  });
}

function serveFile(filePath, res) {
  const ext = path.extname(filePath);
  fs.readFile(filePath, (readErr, data) => {
    if (readErr) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  const requested = safeJoin(ROOT, req.url === '/' ? '/index.html' : req.url);
  if (!requested) {
    res.writeHead(400);
    res.end('Bad request');
    return;
  }

  resolveInsideRoot(requested, (resolved) => {
    // SPA-ish fallback: unknown paths — and anything whose real path escaped
    // the root via a symlink — still serve index.html.
    if (!resolved) {
      serveFile(path.join(ROOT, 'index.html'), res);
      return;
    }
    fs.stat(resolved, (err, stat) => {
      serveFile(err || !stat.isFile() ? path.join(ROOT, 'index.html') : resolved, res);
    });
  });
});

server.listen(PORT, HOST, () => {
  console.log(`demo-console static server: http://${HOST}:${PORT}/  (root: ${ROOT})`);
});
