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
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
  };

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
