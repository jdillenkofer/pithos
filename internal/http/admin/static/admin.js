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

// Tag editors: "Add new tag" clones the row template, "Remove" deletes a row.
// Without JavaScript the server-rendered blank row can still add one tag.
document.addEventListener("click", function (event) {
  const add = event.target.closest("[data-add-tag]");
  if (add) {
    const editor = add.closest("[data-tag-editor]");
    const rows = editor.querySelector("[data-tag-rows]");
    if (rows.children.length >= Number(editor.dataset.max)) {
      return;
    }
    rows.appendChild(editor.querySelector("template").content.cloneNode(true));
    rows.lastElementChild.querySelector("input").focus();
    updateTagEditor(editor);
    return;
  }
  const remove = event.target.closest("[data-remove-tag]");
  if (remove) {
    const editor = remove.closest("[data-tag-editor]");
    remove.closest("[data-tag-row]").remove();
    updateTagEditor(editor);
  }
});

function updateTagEditor(editor) {
  const count = editor.querySelector("[data-tag-rows]").children.length;
  editor.querySelector("[data-add-tag]").disabled = count >= Number(editor.dataset.max);
  const remaining = editor.querySelector("[data-tag-remaining]");
  if (remaining) {
    remaining.textContent = String(Number(editor.dataset.max) - count);
  }
}

// Buttons with data-confirm open the confirmation dialog. Confirming
// re-submits the form with confirm=yes through the same button; the server
// asks again with a confirmation page when this script does not run.
document.addEventListener("click", function (event) {
  const button = event.target.closest("button[data-confirm]");
  const dialog = document.getElementById("confirm-dialog");
  if (!button || !button.form || !dialog || button.form.querySelector('input[name="confirm"][value="yes"]')) {
    return;
  }
  event.preventDefault();
  dialog.querySelector("[data-confirm-title]").textContent = button.dataset.confirmTitle || "Are you sure?";
  dialog.querySelector("[data-confirm-message]").textContent = button.dataset.confirm;
  dialog.querySelector("[data-confirm-action]").textContent = button.dataset.confirmAction || "Delete";
  dialog.returnValue = "";
  dialog.onclose = function () {
    if (dialog.returnValue !== "confirm") {
      return;
    }
    const confirm = document.createElement("input");
    confirm.type = "hidden";
    confirm.name = "confirm";
    confirm.value = "yes";
    button.form.appendChild(confirm);
    button.form.requestSubmit(button);
  };
  dialog.showModal();
});

// Audit filters: picking a relative range applies it at once; editing a
// date or time switches the range to "Custom".
document.addEventListener("change", function (event) {
  const form = event.target.closest("form[data-audit-filter]");
  if (!form) {
    return;
  }
  if (event.target.name === "range" && event.target.value !== "custom") {
    form.querySelectorAll("[data-range-input]").forEach(function (input) { input.value = ""; });
    form.requestSubmit();
  } else if (event.target.name === "operation") {
    form.requestSubmit();
  }
});
document.addEventListener("input", function (event) {
  if (event.target.matches("[data-range-input]")) {
    const custom = event.target.form.querySelector("[data-custom-range]");
    if (custom) {
      custom.checked = true;
    }
  }
});

// Policy simulator: choosing a credential fills the caller fields.
document.addEventListener("change", function (event) {
  const select = event.target.closest("[data-sim-identity]");
  if (!select) {
    return;
  }
  const option = select.selectedOptions[0];
  const form = select.form;
  const anonymous = option.hasAttribute("data-anonymous");
  if (!anonymous && !option.value) {
    return;
  }
  form.querySelector('[data-sim-field="account"]').value = anonymous ? "" : option.dataset.account;
  form.querySelector('[data-sim-field="principal"]').value = anonymous ? "" : option.dataset.principal;
  form.querySelector('[data-sim-field="accessKey"]').value = anonymous ? "" : option.value;
});

// Lifecycle editors use repeatable rows for tags and transition schedules.
document.addEventListener("click", function (event) {
  const add = event.target.closest("[data-add-row]");
  const remove = event.target.closest("[data-remove-row]");
  const button = add || remove;
  if (!button) return;
  const editor = button.closest("[data-repeat-editor]");
  if (!editor) return;
  const rows = editor.querySelector("[data-repeat-rows]");
  if (add) {
    if (rows.children.length >= Number(editor.dataset.max)) return;
    rows.appendChild(editor.querySelector("template").content.cloneNode(true));
    rows.lastElementChild.querySelector("input, select").focus();
  } else {
    button.closest("[data-repeat-row]").remove();
  }
  editor.querySelector("[data-add-row]").disabled = rows.children.length >= Number(editor.dataset.max);
});

// Uploads are sent with XMLHttpRequest to show progress, speed and time
// remaining. Without JavaScript the form posts normally.
function formatBytes(n) {
  if (n < 1024) return n + " B";
  let value = n / 1024;
  let exponent = 0;
  while (value >= 1024 && exponent < 5) {
    value /= 1024;
    exponent++;
  }
  return value.toFixed(1) + " " + "KMGTPE"[exponent] + "iB";
}

function formatDuration(seconds) {
  seconds = Math.max(0, Math.round(seconds));
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  return (h ? h + "h" : "") + (h || m ? m + "m" : "") + s + "s";
}

// Choosing a file fills in its name while the key is a folder prefix or was
// filled in from an earlier choice.
document.addEventListener("change", function (event) {
  const input = event.target.closest("[data-upload-file]");
  if (!input) return;
  const form = input.form;
  const key = form.querySelector("[data-upload-key]");
  const file = input.files[0];
  form.querySelector("[data-upload-file-size]").textContent = file ? formatBytes(file.size) : "";
  if (!file) return;
  if (key.value === "" || key.value.endsWith("/") || key.value === key.dataset.filled) {
    const prefix = key.value === key.dataset.filled ? key.dataset.prefix : key.value;
    key.value = prefix + file.name;
    key.dataset.filled = key.value;
  }
});

document.addEventListener("submit", function (event) {
  const form = event.target.closest("form[data-upload-form]");
  if (!form) return;
  event.preventDefault();
  startUpload(form);
});

function startUpload(form) {
  const part = function (name) { return form.querySelector("[data-upload-" + name + "]"); };
  const data = new FormData(form);
  const file = part("file").files[0];
  const size = file ? file.size : 0;
  const progress = part("progress");
  const xhr = new XMLHttpRequest();
  const started = performance.now();
  let samples = [{ time: started, loaded: 0 }];
  let loaded = 0;
  let running = true;

  part("fields").hidden = true;
  part("actions").hidden = true;
  part("status").hidden = false;
  part("error").hidden = true;
  part("back").hidden = true;
  part("retry").hidden = true;
  part("cancel").hidden = false;
  part("target").textContent = data.get("key");
  progress.value = 0;
  progress.classList.remove("failed");
  setState("info", "Uploading", "Keep this page open until the upload finishes.");

  function setState(kind, label, note) {
    const state = part("state");
    state.className = "status " + kind;
    state.textContent = label;
    part("note").textContent = note;
  }

  function render() {
    const now = performance.now();
    const sent = Math.min(loaded, size);
    samples.push({ time: now, loaded: sent });
    // The speed is averaged over the last five seconds to keep it steady.
    while (samples.length > 2 && now - samples[0].time > 5000) samples.shift();
    const first = samples[0];
    const seconds = (now - started) / 1000;
    // Finished uploads show the average speed.
    const rate = running ? (now > first.time ? (sent - first.loaded) / ((now - first.time) / 1000) : 0) : seconds > 0 ? sent / seconds : 0;
    const percent = size ? Math.floor((100 * sent) / size) : 0;
    progress.value = percent;
    part("percent").textContent = percent + "%";
    part("bytes").textContent = formatBytes(sent) + " of " + formatBytes(size);
    part("rate").textContent = rate > 0 ? formatBytes(rate) + "/s" : "-";
    part("remaining").textContent = rate > 0 && running && sent < size ? formatDuration((size - sent) / rate) : "-";
    part("elapsed").textContent = formatDuration(seconds);
  }

  function finish(kind, label, note, message) {
    running = false;
    clearInterval(timer);
    window.removeEventListener("beforeunload", warn);
    render();
    progress.classList.toggle("failed", kind === "bad");
    setState(kind, label, note);
    part("cancel").hidden = true;
    if (message) {
      part("error").textContent = message;
      part("error").hidden = false;
    }
    if (kind !== "ok") {
      part("back").hidden = false;
      part("retry").hidden = false;
    }
  }

  function warn(event) {
    event.preventDefault();
  }

  xhr.upload.onprogress = function (event) {
    loaded = event.loaded;
  };
  // Storage confirms the object only after the last bytes are written.
  xhr.upload.onload = function () {
    loaded = size;
    render();
    // Cancelling now could no longer guarantee that nothing was stored.
    part("cancel").hidden = true;
    setState("info", "Finishing", "All data was sent. Waiting for storage to confirm the upload…");
  };
  xhr.onload = function () {
    const url = new URL(xhr.responseURL || form.action, window.location.href);
    if (url.pathname === "/login") {
      window.location.assign("/login");
      return;
    }
    if (xhr.status === 200 && url.pathname === "/objects/detail") {
      finish("ok", "Succeeded", "Opening the object…");
      window.location.assign(url.href);
      return;
    }
    const text = xhr.responseText.trim();
    const message = text && !text.startsWith("<") ? text : "The server answered with status " + xhr.status + ".";
    finish("bad", "Failed", "The object was not stored.", message);
  };
  xhr.onerror = function () {
    finish("bad", "Failed", "The upload did not complete.", "The connection to the server was interrupted.");
  };
  xhr.onabort = function () {
    finish("muted", "Cancelled", "The upload was cancelled. The object was not stored.");
  };
  part("cancel").onclick = function () {
    xhr.abort();
  };
  part("retry").onclick = function () {
    part("status").hidden = true;
    part("fields").hidden = false;
    part("actions").hidden = false;
  };

  const timer = setInterval(render, 500);
  window.addEventListener("beforeunload", warn);
  xhr.open("POST", form.action);
  xhr.setRequestHeader("Accept", "text/plain");
  xhr.send(data);
  render();
}
