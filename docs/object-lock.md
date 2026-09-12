# S3 Object Lock

Pithos supports version-specific WORM retention in `GOVERNANCE` and `COMPLIANCE`
mode and independent `ON`/`OFF` legal holds. SQLite and PostgreSQL enforce
protection inside the same transaction as deletion or protection changes.
Their repositories and migrations are separate implementations.

## Enable protection

Create a bucket with Object Lock and versioning enabled atomically:

```sh
aws --endpoint-url http://localhost:9000 s3api create-bucket \
  --bucket backups --object-lock-enabled-for-bucket
```

Enable an existing bucket, optionally supplying a default retention period:

```sh
aws --endpoint-url http://localhost:9000 s3api put-object-lock-configuration \
  --bucket backups --object-lock-configuration \
  '{"ObjectLockEnabled":"Enabled","Rule":{"DefaultRetention":{"Mode":"GOVERNANCE","Days":30}}}'
```

Activation is permanent. Versioning cannot subsequently be suspended. Removing
`Rule` removes the default without disabling Object Lock. A default requires a
valid mode and exactly one positive `Days` or `Years` value. Existing versions
remain unprotected until retention or a hold is explicitly applied to them.

Defaults apply when a new version is completed. An explicit retention overrides
the default. PUT, CopyObject and multipart initiation accept
`x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date` and
`x-amz-object-lock-legal-hold`. Multipart initiation preserves explicit settings;
the default is calculated at completion. Copy and Append create their own
versions using the destination bucket's defaults; ordinary copy does not inherit
the source's retention or legal hold. PUT with explicit or default retention requires
`Content-MD5` or `x-amz-sdk-checksum-algorithm` through the checksum handling path.

## Retention and legal hold

Use the exact version ID returned by a write or `list-object-versions`:

```sh
aws --endpoint-url http://localhost:9000 s3api put-object-retention \
  --bucket backups --key archive --version-id "$VERSION_ID" \
  --retention '{"Mode":"COMPLIANCE","RetainUntilDate":"2030-01-01T00:00:00Z"}'

aws --endpoint-url http://localhost:9000 s3api put-object-legal-hold \
  --bucket backups --key archive --version-id "$VERSION_ID" \
  --legal-hold '{"Status":"ON"}'
```

An active compliance retention can only be extended. It cannot be shortened,
removed or changed to governance, even with governance bypass. A legal hold
blocks version deletion independently of retention and has no expiry. Setting
its status to `OFF` requires the legal-hold write permission.

An active governance retention may be shortened, removed or bypassed for deletion
only when the request explicitly asks for bypass and the Lua authorizer also
allows `BypassGovernanceRetention`:

```sh
aws --endpoint-url http://localhost:9000 s3api delete-object \
  --bucket backups --key archive --version-id "$VERSION_ID" \
  --bypass-governance-retention
```

This does not bypass compliance retention or legal hold. After the retention
expiry, retention alone no longer blocks deletion. Key-only DELETE can create a
delete marker without deleting protected versions. Multi-Delete returns results
per version. Lifecycle skips protected version expiration, while delete markers
and storage-class transitions remain possible. Nonempty buckets cannot be deleted.

## Authorization

The six operations are `GetObjectLockConfiguration`, `PutObjectLockConfiguration`,
`GetObjectRetention`, `PutObjectRetention`, `GetObjectLegalHold` and
`PutObjectLegalHold`. `BypassGovernanceRetention` is an additional check; it never
replaces authorization of the underlying operation. Multi-Delete checks bypass
for each target version. GET/HEAD include retention and legal-hold headers only
when the corresponding read operation is allowed.

For example, an authenticated administrator policy with a separate bypass key:

```lua
function authorizeRequest(request)
  if request:isAnonymous() then return false end
  if request.operation == "BypassGovernanceRetention" then
    return request.authorization.accessKeyId == "retention-admin"
      and request.bypassGovernanceRetentionRequested
  end
  return request.authorization.accessKeyId == "storage-admin"
    or request.authorization.accessKeyId == "retention-admin"
end
```

Policies can inspect `request.versionID`, `objectLockEnabled`, `objectLockMode`,
`objectLockRetainUntilDate`, `objectLockLegalHold`, `objectLockDays`,
`objectLockYears` and `bypassGovernanceRetentionRequested`.

## Synchronous replication and old versions

Configure a stable `replicationId` and stable `secondaryIds` alongside the primary
and secondary storage configurations. Reordering secondaries must reorder their
IDs with them. Reusing an ID for a different destination is unsupported.

Version, delete-marker and multipart-upload IDs are mapped durably per replica.
A SQL journal retains operations, source bytes and individual acknowledgments.
For a local primary, its mutation and journal entry commit in the same database
transaction. A remote primary requires an explicit `journalDatabase`; its intent
is persisted before the remote call. Success means every replica has confirmed.
Failures remain pending and are retried after restart and by the recovery worker.
An ambiguous remote write can produce another version on retry; Pithos does not
infer a mapping from matching ETags or timestamps. Failures are reported to the
caller and logged, with pending/retry/failure Prometheus metrics.

Before applying lock changes to older, unmapped versions, stop all servers and
other writers using the topology, including lifecycle processing. Then run:

```sh
pithos reconcile-replication --storage-config storage.json \
  --replication-id backups --bucket backups --dry-run
pithos reconcile-replication --storage-config storage.json \
  --replication-id backups --bucket backups
```

Omitting `--bucket` selects all primary buckets; comma-separated names select
several. Dry-run lists missing mappings without dispatching writes; drain any
pending storage outbox first. Startup still opens the configured databases and
runs their normal schema migrations. The command disables its lifecycle/GC and
replication background workers. External writers must remain stopped until it
succeeds; the command cannot pause other processes or remote clients.

The reconciliation copies unmapped versions and delete markers, preserves metadata and
absolute retention deadlines, and keeps existing replica versions. Each bucket's
ordered work and source data are journaled before changing replicas. Defaults are
temporarily removed on destinations so old unprotected versions remain
unprotected, and their restoration is journaled too. A restart resumes pending
work. Missing mappings fail explicitly rather than targeting the latest version.
The journal needs disk space for all source data scheduled for a bucket's reconciliation.

## Audit

Audit format 4 includes the target version, requested and effective protection,
and requested, authorized and actually used governance bypass. These fields are
hashed in binary, JSON and text formats. Historical formats 1–3 retain their
original hash rules. Multi-Delete records each version separately, and HTTP
permission denials reach the configured recorder without a successful storage
call. See [Audit logging](audit-logging.md) for verification and export commands.

A new asynchronous replication mode and verified compatibility with backup
applications such as restic, Kopia or Veeam are outside this feature's scope.

The protocol rules follow the AWS [Object Lock guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
and [Object Lock considerations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html).
