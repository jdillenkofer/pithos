"use strict";
// htmx is vendored at v2.0.8. Keep CSP strict: no eval, inline scripts, or
// injected indicator styles, and never cache authenticated pages in history.
htmx.config.allowEval = false;
htmx.config.allowScriptTags = false;
htmx.config.includeIndicatorStyles = false;
htmx.config.historyCacheSize = 0;
htmx.config.selfRequestsOnly = true;

document.addEventListener("htmx:configRequest", function (event) {
  const token = document.querySelector('meta[name="csrf-token"]');
  if (token) {
    event.detail.headers["X-CSRF-Token"] = token.content;
  }
});
// Use a full navigation for expired sessions instead of swapping a login form
// into a dashboard polling target.
document.addEventListener("htmx:beforeSwap", function (event) {
  if (event.detail.xhr.responseURL && new URL(event.detail.xhr.responseURL).pathname === "/login") {
    event.detail.shouldSwap = false;
    window.location.assign("/login");
  }
});
