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
const ROOT = path.resolve(process.argv[3] || __dirname);
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

function safeJoin(root, urlPath) {
  const decoded = decodeURIComponent(urlPath.split('?')[0]);
  const target = path.normalize(path.join(root, decoded));
  if (!target.startsWith(root)) return null; // block path traversal
  return target;
}

const server = http.createServer((req, res) => {
  let filePath = safeJoin(ROOT, req.url === '/' ? '/index.html' : req.url);
  if (!filePath) {
    res.writeHead(400);
    res.end('Bad request');
    return;
  }

  fs.stat(filePath, (err, stat) => {
    if (err || !stat.isFile()) {
      // SPA-ish fallback: unknown paths still serve index.html
      filePath = path.join(ROOT, 'index.html');
    }
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
  });
});

server.listen(PORT, HOST, () => {
  console.log(`demo-console static server: http://${HOST}:${PORT}/  (root: ${ROOT})`);
});
