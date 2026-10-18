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
