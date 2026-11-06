# Admin Web UI

Pithos embeds its admin UI, Go templates, CSS and vendored htmx in the binary.
There is no CDN dependency or separate frontend build. The admin listener is
separate from the S3 API (`9000`) and monitoring (`9090`); its default port is
`9001`.

## Enable access

The listener is disabled by default; enable it with
`PITHOS_ADMIN_PORT_ENABLED=true`. It inherits `PITHOS_BIND_ADDRESS`
(`0.0.0.0`) and stays disabled when authentication is disabled. An
empty admin allowlist permits **no logins**, even when a credential's S3 policy
allows `*`.

Configure a normal credential with your chosen credential provider, then put
its access-key ID in the admin allowlist:

```sh
export PITHOS_ADMIN_PORT_ENABLED=true
export PITHOS_ADMIN_ACCESS_KEYS="my-admin-access-key"
export PITHOS_ADMIN_SECURE_COOKIE=false # local plain-HTTP use only
./pithos serve
```

Visit `http://localhost:9001/login` and enter that access key and its secret.
Admin cookies are Secure by default; the opt-out above is only for trying the
UI locally without HTTPS (see [Transport and deployment](#transport-and-deployment)).
The allowlist is comma-separated and read at startup. Restart after changing it.
An allowlisted ID must also exist in the active credential provider.

**The MVP has global administrators only.** An allowlisted credential gets full
admin UI access to every account's buckets, objects, credentials, policies,
audit logs and storage diagnostics. Policy/Lua rules do not restrict the admin
UI. The allowlist does **not** grant any additional permission on the S3 API:
S3 ownership checks and policy/Lua authorization are unchanged. Account-scoped
administrators and `admin:*` policy actions are not part of this release.

If the allowlist is nonempty, configure a local SQLite or PostgreSQL database
for sessions. `PITHOS_ADMIN_DATABASE_INDEX` selects its zero-based index in the
configured database container. An invalid index causes startup to fail rather
than falling back to in-memory sessions. An empty allowlist does not require a
session database.

## Transport and deployment

Like the existing endpoints, admin serves **plain HTTP**, not built-in TLS.
Session and login-CSRF cookies are Secure by default, so browsers only send
them over HTTPS. For remote use, terminate HTTPS at a reverse proxy and bind the
listener locally:

```sh
export PITHOS_ADMIN_BIND_ADDRESS=127.0.0.1
```

The proxy may forward plain HTTP to Pithos. For local use without HTTPS, opt
out of Secure cookies:

```sh
export PITHOS_ADMIN_SECURE_COOKIE=false
```

Some browsers accept Secure cookies on `http://localhost`, but others do not; if
login fails with a missing-cookie message, apply the opt-out above. **Plain HTTP
exposes both login credentials and session cookies to network observers.**
Restrict the listener with firewall rules and do not expose it directly to the
internet.

The proxy must preserve the original Host header and forward the admin UI at
the root of its own origin, not a rewritten subpath. Avoid hosting untrusted
applications on that origin. Login rate limits use the socket's client IP, not
untrusted forwarding headers; behind a proxy the per-IP limit is shared by its
clients. Access-key limits apply independently.

For Docker, publish `9001` explicitly if needed. Mount policy files/directories
writable only when UI policy saving is desired. Atomic policy replacement needs
a writable parent directory; a read-only bind mount or a single-file mount may
prevent saving. Never loosen file permissions to work around this.

## Settings

| Environment variable | Default | Meaning |
|---|---|---|
| `PITHOS_ADMIN_PORT_ENABLED` | `false` | Start the admin listener; requires authentication |
| `PITHOS_ADMIN_PORT` | `9001` | Listener port |
| `PITHOS_ADMIN_BIND_ADDRESS` | `BindAddress` | Listener address, inherited dynamically unless overridden |
| `PITHOS_ADMIN_ACCESS_KEYS` | Empty | Comma-separated global-admin access-key IDs |
| `PITHOS_ADMIN_SECURE_COOKIE` | `true` | Add Secure to session and login-CSRF cookies; set `false` for plain-HTTP local use |
| `PITHOS_ADMIN_SESSION_IDLE_MINUTES` | `30` | Session idle timeout |
| `PITHOS_ADMIN_SESSION_MAX_HOURS` | `12` | Absolute session lifetime |
| `PITHOS_ADMIN_DATABASE_INDEX` | `0` | Configured session database index |

Matching CLI flags are `-adminPortEnabled`, `-adminPort`, `-adminBindAddress`,
`-adminAccessKeys`, `-adminSecureCookie`, `-adminSessionIdleMinutes`,
`-adminSessionMaxHours`, and `-adminDatabaseIndex`. Environment variables follow
the same precedence as existing settings. Session durations must be positive
and at most one year when login is configured.

## Sessions and browser security

- Session tokens contain 32 cryptographically random bytes. Only SHA-256 token
  hashes are stored in `admin_session`; cookies are HttpOnly and SameSite=Strict.
- Sessions persist access-key, account and principal IDs, a credential
  fingerprint, a per-session CSRF token, creation/last-seen/expiry timestamps,
  client IP and user agent. Neither the bearer token nor the credential secret
  is stored in the session table.
- Every authenticated request looks up the credential again. A missing or
  changed credential invalidates the session. Changes become visible according
  to the provider's normal refresh behavior. Credentials created or deleted in
  the UI with the SQL provider take effect immediately, for S3 requests and
  admin sessions alike, even when the follow-up full reload fails or background
  reloading is disabled. Existing environment credentials require a restart to
  change.
- Expiration enforces both the idle and absolute limits. Background cleanup
  removes expired rows. Dashboard/job polling counts as activity, but cannot
  extend the absolute lifetime. Logout is a CSRF-protected POST and deletes the
  session immediately.
- Login compares fixed-length secret hashes in constant time. A bounded login
  limiter allows 20 attempts per socket IP and 10 per access key per minute.
- Login has a signed pre-login CSRF token. Every authenticated unsafe request
  requires the session CSRF token; htmx sends it in a header and ordinary forms
  send a hidden field. Cross-site Origin/Fetch Metadata requests are rejected.
- CSP permits only same-origin external scripts/styles and same-origin or
  inline `data:` images (used for stylesheet icons); evaluation and inline
  scripts are disabled in htmx. Responses set `frame-ancestors 'none'`,
  `X-Frame-Options: DENY`, `nosniff`, `Referrer-Policy: same-origin` and
  `Cache-Control: no-store`. Authenticated content is not cached in htmx history.

## Dashboard

The dashboard polls every ten seconds. It shows a storage snapshot (total
size, object count, average object size and bucket count), server status from
the same database health check as monitoring, version and uptime, S3
request/error rates and interval mean latency from the in-process Prometheus
gatherer, deduplication, outbox and replication backlogs when those
collectors are present, and the ten largest buckets. The first sample
establishes a rate baseline; missing metrics are shown as a dash rather than
invented values.

Bucket byte/object gauges sample current completed live objects from configured
metadata databases. Historical versions and delete markers are excluded.
Database labels distinguish replicated stores; do not sum mirrored databases
as though they represented unique objects. Sampling follows the storage gauge
refresh interval (30 seconds by default). Unsupported remote-only configurations
may not provide these gauges.

## Buckets and objects

The console follows the layout of the AWS S3 console. Buckets are listed
across all accounts with their sampled object count and size, and can be
filtered by name. Creation accepts an explicit owner account (defaulting to
the logged-in credential's account) and can enable versioning or Object Lock.
Deletion requires typing the bucket name and still fails for a nonempty or
protected bucket.

Each bucket has Objects, Properties, Permissions and Management tabs:

- **Properties:** overview, Bucket Versioning, tags, Object Lock default
  retention, static website hosting and event notifications. Event notification
  forms support SNS topics, SQS queues and Lambda functions, event-type
  checkboxes, and optional literal prefix/suffix filters. Create, edit or delete
  individual rules, including unnamed rules created through the API. Edits
  preserve other rules, and stale forms are rejected. Deleting one rule or all
  rules requires confirmation. EventBridge has a separate enable/disable form;
  changing it keeps the rules, and deleting rules keeps the EventBridge setting.
  Notification destinations are validated when saving.
- **Permissions:** CORS rules are managed through forms. Create, edit or delete
  individual rules with an optional name, allowed origins, HTTP method
  checkboxes, allowed/exposed headers and an optional preflight cache lifetime.
  Enter origins and headers one per line. Rule order and unrelated rules are
  preserved; stale forms are rejected if the configuration has changed.
  Removing individual rules or all rules requires confirmation.
- **Management:** create, edit or delete lifecycle rules using prefix, tag and exclusive
  object-size filters; day- or UTC-date-based expiration and current-version
  transitions; noncurrent-version expiration and transitions with optional
  newer-version retention; expired delete marker cleanup; and incomplete
  multipart upload cleanup. Add or remove tag and transition rows as needed.
  Existing unnamed API-created rules can also be edited or deleted. A separate
  editor leads to a review of every filter and action before saving, with a
  warning for bucket-wide rules. Removing all rules requires confirmation.

Configuration changes are validated with the same storage/S3 validation
helpers as the API.

Website redirection rules are available when static website hosting is enabled,
not when all requests are redirected to another host. Forms support optional
object-key prefix and HTTP error-status conditions, host/protocol overrides,
prefix or whole-key replacement, and redirect statuses 301, 302, 303, 307 and 308.
Explicit empty prefixes and replacement keys are supported. Both selected
conditions must match; with neither selected, a rule matches every request.
Rules are evaluated in order, and the first match wins. Set a rule's position
in its form or move it up/down in the list. Saving shows a review of the condition,
action and position, with warnings for rules that match every request or are
blocked by an earlier catch-all rule. Deleting one rule or all rules requires
confirmation. Rule edits keep the index and error
documents; hosting-setting edits keep the rules. Switching to redirect-all
requires explicit consent to discard existing rules. Stale website forms are
rejected if the configuration has changed.

The object browser supports prefix navigation with breadcrumbs, bounded
pages with continuation tokens, and version listings including delete
markers. Object pages show an overview, storage class, checksums, metadata,
tags and Object Lock state; tags are edited as key/value rows, and retention
and legal hold are editable for the selected version. The Versions tab lists
every version of the key; selected versions are deleted after typing
`permanently delete`.

Images, video and audio can be opened in the browser and are previewed on
the object page; video supports seeking through HTTP range requests.
Text-like objects, including HTML and SVG, are only ever shown as plain text,
with the first 64 KiB previewed. Other types, including PDF, are download-only.
Viewed and downloaded responses carry `nosniff` and a sandbox CSP, and
downloads use `Content-Disposition: attachment` with
`application/octet-stream`, so object content never runs as active content in
the console's origin.

Uploads use a streaming `multipart/form-data` request directly into `PutObject`,
without buffering a whole object or using temporary upload files. The first
form field is the CSRF token; bucket and key fields precede the single file.
Storage size limits apply. Resumable S3 multipart uploads are deferred.

Object deletion requires confirmation. Bypassing governance retention requires
an additional explicit checkbox; global-admin status includes that capability.
Compliance retention and legal hold are still enforced by storage.

## Credentials

Every built-in provider supplies a metadata-only listing. File and environment
providers are read-only. Only the SQL provider implements credential creation
and deletion. The UI generates a cryptographically random secret and displays
it only in the creation response; copy it then, because listings cannot reveal
it later. Secrets are never put in URLs, audit events or logs. A session cannot
delete its own active credential.

Deleting an SQL credential revokes it fail-closed: the access key stops
authenticating S3 requests and its admin sessions end as soon as the deletion
is attempted, even when the commit outcome is uncertain or the credential was
already removed from the database out of band. A credential whose deletion
failed is restored only by a later successful reload that still finds it in the
database; retry the deletion in that case.

Creating a credential does not automatically add it to the global-admin
allowlist. That allowlist remains a separate startup configuration decision.

## Policies

The policy page shows the configured active `policies.json` or `authorizer.lua`.
JSON validation reuses the strict `validate-policy` compiler. Lua is precompiled
and dry-run/simulation execution is limited to one million Lua VM instructions.
That limit counts interpreted instructions only: it is not a wall-clock or
memory limit, and a single call into a built-in library function (for example
`string.rep` with a huge count) runs to completion before the limit is checked.
A Lua candidate can therefore still use substantial CPU time and memory while
it is validated or simulated, so only trusted administrators should be
allowlisted. Candidates are limited to 1 MiB. The simulator evaluates
the candidate for one request: a caller (anonymous, or account, principal and
access key, which can be filled from a listed credential), an operation from
the supported list, and an optional bucket, key and copy source. Like the S3
server, it resolves bucket owners (`aws:ResourceAccount`), sets the signature
authentication context, answers `NoSuchBucket` for missing buckets, and
denies callers from other accounts at the account boundary before policies
run; the policies' own verdict is still shown. Results explain the effect and
list the action, resource and matching statement IDs. Object tags, headers,
source IP and query parameters are not simulated. The simulator does not
perform an S3 request or change the active policy.

Saving is serialized in-process. It checks the SHA-256 hash captured when the
file was loaded, validates the exact candidate, creates a private durable
`.bak-<UTC timestamp>-<random suffix>` backup,
writes a private temporary file, fsyncs and atomically renames it, syncs the
parent directory, and immediately reloads the authorizer. Conflicts require a
reload instead of silently overwriting concurrent changes. Failed save/reload
attempts restore the old disk/runtime policy when possible and report rollback
failures. Symlink policy paths cannot be saved.

Policy edits cannot lock the current administrator out of this UI because its
allowlist is independent of S3 policies. Thus an admin-policy lockout override
is neither needed nor offered. Policy changes can still lock S3 clients out;
validate and simulate carefully. Save/validation/simulation events are audited
without recording policy contents.

## Audit log

The viewer discovers FileSink paths and serializers by decoding the configured
storage tree. Users cannot submit arbitrary filesystem paths. The event history
lists entries newest first and filters by operation (a list of the operations
present in the log), principal and bucket (with suggestions from the log), and
time: relative presets from 15 minutes to 30 days, or a custom range picked
with date/time pickers. All times are UTC. Request start phases and chain
entries (genesis and grounding) are hidden unless requested. Selecting an
event shows its identity, request, resource, outcome, Object Lock and
integrity details; backend error messages are omitted, the error code is shown.

Console actions are recorded as `Admin…` events, for example `AdminLogin`,
`AdminCreateBucket`, `AdminPutBucketLifecycle`, `AdminDeleteObjectVersions` or
`AdminSavePolicy`.

Verification runs in the background with a progress bar, throughput and
remaining time, and can be started again in place once finished. It reads the
whole file with no size, entry or time limit and checks every entry hash,
chain link and Merkle root, the Ed25519 signature of every entry and Merkle
root, and the ML-DSA-87 signature of every Merkle root. The verification keys
are the `publicKey` values of the log's signing configuration, or are derived
from its local private keys; they therefore prove that the log was signed by
the keys this server is configured with. An Ed25519 key held in Vault can only
be checked when `signing.ed25519.publicKey` is configured; otherwise those
signatures are skipped and the result says so. A failure names the first
failing entry and the reason. Verification is a snapshot of the file's length
when it started; a concurrently incomplete append is reported and may require
retrying. Viewing pages keeps its 128 MiB, 1,000,000-entry and ten-second
limits.

Login, failed credential logins, logout and admin actions go to structured logs
and to the normal signed audit chain when AuditStorageMiddleware is configured
in the live storage stack. Storage operations also retain their normal audit
records. Without an audit middleware/sink there is no persistent audit trail.

## Storage diagnostics

The storage page projects the storage configuration the server started with
(`storage.json`, or the built-in default when that file is missing) into a
middleware/part-store tree using an explicit allowlist of displayable fields. It never renders raw
configuration or provider credentials, encryption/signing keys, passwords,
connection strings or URLs. Unknown types are shown without their fields.

Validation runs against the **live** store, not a second instantiated storage
stack. It uses the same integrity validator as `validate-storage`, with deletion
and force disabled. No request parameter can enable `-delete-corrupted`. The
current validator requires exactly one local database. One validation runs at a
time with no time limit, and the latest report is retained in memory.
Progress is polled through authenticated routes and shown as a progress bar
with throughput and remaining time, estimated from the stored part bytes;
a finished validation can be started again in place. Reports are lost on
restart.

Validation checks **every version** of every object, current and noncurrent,
against the checksums stored for that version. Delete markers carry no data
and are skipped; incomplete multipart uploads are not checked. Objects written
or deleted while validation runs may be reported as failures; rerun validation
to confirm.

Versions are listed and validated one page of 1,000 at a time, so memory use
does not grow with bucket size. The report keeps exact counts of scanned,
passed and failed versions, but lists at most 100 failures; the page states
how many further failures were omitted. Backend error details are not shown in
the UI. For a complete failure list, including error details, run the
`validate-storage` command, whose report is not truncated.

Background cleanup and verification/validation jobs are cancelled and joined on
shutdown before storage or databases are closed.
