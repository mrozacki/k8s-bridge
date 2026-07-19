// app.js — demo-console client logic. Vanilla JS, no framework, no build
// step. Responsibilities:
//   1. Render the cheat-sheet (DEMO_STEPS from cheatsheet.js) with
//      click-to-copy command buttons.
//   2. Embed the terminal: an iframe pointed at the ttyd URL (typed
//      commands run there, against whatever shell ttyd was launched with).
//   3. Embed dashboards: a Grafana iframe if a URL is configured (query
//      param ?grafana=... or the GRAFANA_URL the launcher baked in),
//      otherwise poll the local status API and render a bridge-top-style
//      text status strip.
//
// Config resolution order for both URLs: query string param wins, then a
// value serve.sh injects into window.DEMO_CONSOLE_CONFIG at page-load time
// (see index.html), then a same-origin default guess.

(function () {
  'use strict';

  function qparam(name) {
    const u = new URL(window.location.href);
    return u.searchParams.get(name);
  }

  const cfg = window.DEMO_CONSOLE_CONFIG || {};
  const TTYD_URL = qparam('ttyd') || cfg.ttydUrl || (window.location.protocol + '//' + window.location.hostname + ':7681');
  const GRAFANA_URL = qparam('grafana') || cfg.grafanaUrl || '';
  const STATUS_API_URL = qparam('statusApi') || cfg.statusApiUrl || (window.location.protocol + '//' + window.location.hostname + ':8843/status');

  // ---------- Cheat sheet ----------

  function renderCheatsheet() {
    const root = document.getElementById('cheatsheet');
    root.innerHTML = '';
    (window.DEMO_STEPS || []).forEach((step) => {
      const el = document.createElement('div');
      el.className = 'step';
      el.dataset.stepId = String(step.id);

      const head = document.createElement('div');
      head.className = 'step-head';
      head.innerHTML =
        '<span class="step-num">' + String(step.id).padStart(2, '0') + '</span>' +
        '<span class="step-title"></span>' +
        '<span class="step-tag"></span>' +
        '<span class="step-chevron">▸</span>';
      head.querySelector('.step-title').textContent = step.title;
      head.querySelector('.step-tag').textContent = step.tag;
      head.addEventListener('click', () => el.classList.toggle('open'));

      const body = document.createElement('div');
      body.className = 'step-body';

      const say = document.createElement('p');
      say.className = 'step-say';
      say.textContent = '“' + step.say + '”';
      body.appendChild(say);

      step.commands.forEach((cmd) => {
        const row = document.createElement('div');
        row.className = 'cmd-row';
        const text = document.createElement('div');
        text.className = 'cmd-text';
        text.textContent = cmd;
        const btn = document.createElement('button');
        btn.className = 'copy-btn';
        btn.textContent = 'Copy';
        btn.addEventListener('click', () => copyToClipboard(cmd, btn));
        row.appendChild(text);
        row.appendChild(btn);
        body.appendChild(row);
      });

      const copyAll = document.createElement('button');
      copyAll.className = 'copy-all-btn';
      copyAll.textContent = 'Copy all lines';
      copyAll.addEventListener('click', () => copyToClipboard(step.commands.join('\n'), copyAll));
      body.appendChild(copyAll);

      el.appendChild(head);
      el.appendChild(body);
      root.appendChild(el);
    });
  }

  function copyToClipboard(text, btn) {
    const done = () => {
      if (!btn) return;
      const original = btn.textContent;
      btn.textContent = 'Copied';
      btn.classList.add('copied');
      setTimeout(() => {
        btn.textContent = original;
        btn.classList.remove('copied');
      }, 1200);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done).catch(() => fallbackCopy(text, done));
    } else {
      fallbackCopy(text, done);
    }
  }

  function fallbackCopy(text, done) {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); } catch (e) { /* best effort */ }
    document.body.removeChild(ta);
    done();
  }

  // ---------- Terminal (ttyd iframe) ----------

  function setupTerminal() {
    const frame = document.getElementById('terminal-frame');
    const placeholder = document.getElementById('terminal-placeholder');
    const label = document.getElementById('terminal-url-label');
    label.textContent = TTYD_URL;

    frame.addEventListener('error', () => showTerminalPlaceholder());

    // ttyd doesn't answer a simple fetch with permissive CORS in every
    // config, so we optimistically load the iframe and let the placeholder
    // show only if it never loads OR the user explicitly reports failure.
    // A best-effort reachability probe (image trick / fetch no-cors) still
    // helps for the common "ttyd not running" case.
    fetch(TTYD_URL, { mode: 'no-cors' })
      .then(() => { frame.src = TTYD_URL; })
      .catch(() => {
        // Even on fetch failure, try loading the iframe anyway — some
        // browsers block no-cors fetch to other ports more aggressively
        // than the iframe itself.
        frame.src = TTYD_URL;
      });

    // Give it a moment, then if the iframe reports nothing loaded, we keep
    // the placeholder visible behind it (z-index) so presenters always see
    // a helpful message rather than a blank white/black rectangle.
    setTimeout(() => {
      try {
        // If cross-origin, we can't introspect contentDocument — that's
        // fine, it's a strong signal ttyd loaded successfully.
        if (frame.contentWindow) placeholder.style.display = 'none';
      } catch (e) {
        placeholder.style.display = 'none';
      }
    }, 1500);
  }

  function showTerminalPlaceholder() {
    document.getElementById('terminal-placeholder').style.display = 'flex';
  }

  // ---------- Dashboards: Grafana iframe or status strip fallback ----------

  function setupDashboard() {
    const header = document.getElementById('dashboard-mode-label');
    if (GRAFANA_URL) {
      header.textContent = 'Grafana';
      const wrap = document.getElementById('dashboard-body');
      wrap.innerHTML = '<iframe id="grafana-frame" src="' + GRAFANA_URL + '"></iframe>';
    } else {
      header.textContent = 'Status strip (no GRAFANA_URL set)';
      renderStatusStrip();
      setInterval(renderStatusStrip, 5000);
    }
  }

  function badge(v) {
    const s = String(v).toLowerCase();
    if (s === 'true') return '<span class="badge-true">Ready</span>';
    if (s === 'false') return '<span class="badge-false">Not Ready</span>';
    return '<span class="badge-unknown">' + escapeHtml(v) + '</span>';
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  function row(cells) {
    return '<div class="strip-row">' + cells.map(escapeHtml).join('  ') + '</div>';
  }

  async function renderStatusStrip() {
    const wrap = document.getElementById('dashboard-body');
    let data;
    try {
      const resp = await fetch(STATUS_API_URL, { cache: 'no-store' });
      data = await resp.json();
    } catch (e) {
      wrap.innerHTML = '<div id="status-strip"><p class="strip-empty">status API unreachable at ' +
        escapeHtml(STATUS_API_URL) + ' — is status-api.js running? (see serve.sh)</p></div>';
      setStatusPill('bad', 'status API down');
      return;
    }

    if (data.error) {
      setStatusPill('bad', 'cluster unreachable');
    } else {
      setStatusPill('ok', 'live');
    }

    const html = [];
    html.push('<div id="status-strip">');
    html.push('<div class="strip-section"><h3>Context / bridge</h3>');
    html.push(row(['context:', data.context || '(none)']));
    html.push('<div class="strip-row">bridge Ready: ' + badge(data.bridgeReady) + '</div>');
    html.push('</div>');

    html.push('<div class="strip-section"><h3>Nodes</h3>');
    if ((data.nodes || []).length) {
      data.nodes.forEach((n) => html.push(row([n.name, n.status])));
    } else html.push('<p class="strip-empty">(none / unreachable)</p>');
    html.push('</div>');

    html.push('<div class="strip-section"><h3>ClusterQueues</h3>');
    if ((data.clusterQueues || []).length) {
      data.clusterQueues.forEach((q) => html.push(row([q.name, 'pending:' + q.pending])));
    } else html.push('<p class="strip-empty">(none)</p>');
    html.push('</div>');

    html.push('<div class="strip-section"><h3>Workloads</h3>');
    if ((data.workloads || []).length) {
      data.workloads.forEach((w) => html.push(row([w.namespace, w.name, w.queue, 'admitted:' + w.admitted, w.age])));
    } else html.push('<p class="strip-empty">(none)</p>');
    html.push('</div>');

    html.push('<div class="strip-section"><h3>Bridge JobSets (slurm-jobs)</h3>');
    if ((data.jobsets || []).length) {
      data.jobsets.forEach((j) => html.push(row([j.name, 'restarts:' + j.restarts, 'suspended:' + j.suspended])));
    } else html.push('<p class="strip-empty">(no bridge-managed JobSets)</p>');
    html.push('</div>');

    html.push('<div class="strip-section"><h3>Slurm queue (squeue)</h3>');
    if ((data.slurmQueue || []).length) {
      data.slurmQueue.forEach((j) => html.push(row([j.id, j.partition, j.state, j.reason])));
    } else html.push('<p class="strip-empty">(login pod unavailable or empty)</p>');
    html.push('</div>');

    html.push('<div class="strip-section"><h3>Slurm nodes (sinfo)</h3>');
    if ((data.slurmNodes || []).length) {
      data.slurmNodes.forEach((n) => html.push(row([n.node, n.partition, n.state])));
    } else html.push('<p class="strip-empty">(login pod unavailable or empty)</p>');
    html.push('</div>');

    html.push('<div class="strip-section"><h3 style="color:var(--text-dim)">Refreshed</h3>' +
      '<div class="strip-row">' + escapeHtml(data.generatedAt || '') + '</div></div>');
    html.push('</div>');

    wrap.innerHTML = html.join('');
  }

  function setStatusPill(cls, text) {
    const pill = document.getElementById('status-pill');
    pill.className = cls;
    pill.textContent = text;
  }

  // ---------- Boot ----------

  document.addEventListener('DOMContentLoaded', () => {
    renderCheatsheet();
    setupTerminal();
    setupDashboard();
  });
})();
