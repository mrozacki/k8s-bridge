#!/usr/bin/env node
// status-api.js — tiny HTTP wrapper around status-snapshot.sh.
// Serves the latest kubectl-derived cluster/bridge/Slurm snapshot as JSON
// at GET /status, so index.html can poll it without shelling out itself
// (browsers can't run kubectl). Dependency-free (Node core modules only).
//
// Usage: node status-api.js [port]
//   port default 8843
//
// Binds to 127.0.0.1 only (see README security notes).

'use strict';

const http = require('http');
const { execFile } = require('child_process');
const path = require('path');

const PORT = Number(process.argv[2] || process.env.DEMO_CONSOLE_STATUS_PORT || 8843);
const HOST = '127.0.0.1';
const SCRIPT = path.join(__dirname, 'status-snapshot.sh');

// The console page is served by static-server.js on a DIFFERENT localhost port
// (8842 by default, PAGE_PORT in serve.sh), so /status genuinely is a
// cross-origin fetch and does need CORS. It must NOT be a wildcard though:
// this response carries live cluster topology (node names, queues, workloads,
// JobSets, Slurm state), and with `*` any website the presenter happens to
// visit could read it straight off localhost. So we allowlist the console's
// own origin and echo it back only on an exact match.
const PAGE_PORT = Number(process.env.DEMO_CONSOLE_PAGE_PORT || process.env.DEMO_CONSOLE_PORT || 8842);
const ALLOWED_ORIGINS = new Set([
  `http://127.0.0.1:${PAGE_PORT}`,
  `http://localhost:${PAGE_PORT}`,
]);

function corsHeaders(req) {
  const origin = req.headers.origin;
  // No Origin header (curl, same-origin) needs no CORS; a non-allowlisted
  // origin gets no CORS header at all, so the browser blocks the read.
  if (!origin || !ALLOWED_ORIGINS.has(origin)) return {};
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    Vary: 'Origin',
  };
}

function runSnapshot(cb) {
  execFile('bash', [SCRIPT], { timeout: 10000, maxBuffer: 4 * 1024 * 1024 }, (err, stdout, stderr) => {
    if (err) {
      cb(null, stderr || String(err));
      return;
    }
    cb(stdout, null);
  });
}

const server = http.createServer((req, res) => {
  const cors = corsHeaders(req);

  if (req.method === 'OPTIONS') {
    res.writeHead(204, cors);
    res.end();
    return;
  }

  if (req.url === '/status') {
    runSnapshot((json, errText) => {
      if (json) {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', ...cors });
        res.end(json);
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', ...cors });
        res.end(JSON.stringify({
          generatedAt: new Date().toISOString(),
          error: 'status-snapshot.sh failed (cluster unreachable? kubectl not configured?)',
          detail: String(errText || '').slice(0, 2000),
          nodes: [], clusterQueues: [], workloads: [], jobsets: [], slurmQueue: [], slurmNodes: [],
        }));
      }
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain', ...cors });
  res.end('Not found. Try GET /status');
});

server.listen(PORT, HOST, () => {
  console.log(`demo-console status API: http://${HOST}:${PORT}/status`);
});
