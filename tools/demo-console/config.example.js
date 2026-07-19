// config.example.js — template for the runtime config the demo console reads.
//
// serve.sh generates tools/demo-console/config.js on every launch, baking in
// the actual ports/URLs it started (ttyd port, status-api port, GRAFANA_URL
// env var if set). That generated config.js is git-ignored so local ports
// never end up committed.
//
// This example is the committed reference. If you ever open index.html without
// going through serve.sh, copy this file to config.js — the empty values make
// app.js fall back to same-origin-hostname guesses.
window.DEMO_CONSOLE_CONFIG = {
  ttydUrl: "",
  statusApiUrl: "",
  grafanaUrl: ""
};
