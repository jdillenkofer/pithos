# Configuration

## Environment Variables

### Basic Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_BIND_ADDRESS` | IP address to bind the server to | `0.0.0.0` |
| `PITHOS_PORT` | Port to run the server on | `9000` |
| `PITHOS_DOMAIN` | Domain name of the server | `localhost` |
| `PITHOS_WEBSITE_DOMAIN` | Domain name of the website | `s3-website.localhost` |
| `PITHOS_REGION` | AWS region for authentication | `eu-central-1` |

### Authentication and Authorization

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_AUTHENTICATION_ENABLED` | Enable authentication; explicitly set to `false` for permissive local-development mode | `true` |
| `PITHOS_CREDENTIALS_PROVIDER` | Credential source: `auto`, `environment`, `file`, or `sql`; `auto` selects `file` when a credentials path is set and `environment` otherwise | `auto` |
| `PITHOS_CREDENTIALS_PATH` | Optional path to a reloadable credentials JSON file; when set, environment credentials are ignored | - |
| `PITHOS_CREDENTIALS_RELOAD_INTERVAL_SECONDS` | Interval between background file or SQL credential refreshes; `0` loads only at startup | `5` |
| `PITHOS_CREDENTIALS_DATABASE_INDEX` | Zero-based configured database index used by the SQL provider | `0` |
| `PITHOS_CREDENTIALS_[N]_ACCESS_KEY_ID` | Access Key ID for the Nth user | - |
| `PITHOS_CREDENTIALS_[N]_SECRET_ACCESS_KEY` | Secret Access Key for the Nth user | - |
| `PITHOS_CREDENTIALS_[N]_ACCOUNT_ID` | Required account ID for the Nth credential; determines bucket ownership | - |
| `PITHOS_CREDENTIALS_[N]_PRINCIPAL_ID` | Required stable principal ID for the Nth credential | - |
| `PITHOS_AUTHORIZER_PATH` | Path to the Lua authorization script | `./authorizer.lua` |
| `PITHOS_AUTHORIZER_TYPE` | Authorization backend: `lua` or `policy` | `lua` |
| `PITHOS_POLICY_PATH` | Path to the policy JSON file in policy mode | `./policies.json` |
| `PITHOS_POLICY_RELOAD_INTERVAL_SECONDS` | Policy refresh interval; `0` loads only at startup | `5` |
| `PITHOS_TRUST_FORWARDED_HEADERS` | Trust proxy forwarding headers for `clientIP` and `scheme` (`X-Forwarded-For`, `X-Forwarded-Proto`, `CF-Connecting-IP`) | `false` |
| `PITHOS_TRUSTED_PROXY_CIDRS` | Comma-separated trusted proxy CIDRs; required when forwarded headers are trusted; invalid CIDRs reject startup | - |

> **Note:** Credentials cannot be set via command-line arguments for security reasons; they must be set using environment variables.

The provider settings may also be set with the `-credentialsProvider`,
`-credentialsPath`, `-credentialsReloadIntervalSeconds`, and
`-credentialsDatabaseIndex` command-line flags.
The credential values themselves are never accepted as arguments.

Authorization settings have matching `-authorizerType`, `-authorizerPath`,
`-policyPath`, and `-policyReloadIntervalSeconds` flags. See
[Policy Authorization](policy-authorization.md) for the supported language and
operation matrix.

Pithos reads these variables once when the environment credential provider is
created at startup and caches the resulting credential set. Environment
credential changes require restarting Pithos. Indices may begin at `0` or `1`,
must be contiguous, and loading stops at the first missing or incomplete pair
after the initial index.

Access Key IDs are limited to 128 bytes, secret access keys to 256 bytes, and
account and principal IDs to 256 bytes. All four credential fields are
required, non-empty, opaque, and case-sensitive. To rotate a credential
without changing ownership or policy, configure the old and new entries with
the same account and principal IDs:

```shell
PITHOS_CREDENTIALS_0_ACCESS_KEY_ID=old-key
PITHOS_CREDENTIALS_0_SECRET_ACCESS_KEY=old-secret
PITHOS_CREDENTIALS_0_ACCOUNT_ID=storage-account
PITHOS_CREDENTIALS_0_PRINCIPAL_ID=storage-client
PITHOS_CREDENTIALS_1_ACCESS_KEY_ID=new-key
PITHOS_CREDENTIALS_1_SECRET_ACCESS_KEY=new-secret
PITHOS_CREDENTIALS_1_ACCOUNT_ID=storage-account
PITHOS_CREDENTIALS_1_PRINCIPAL_ID=storage-client
```

The corresponding policy can remain unchanged during the rotation:

```lua
function authorizeRequest(request)
  return request:principalIdEquals("storage-client")
end
```

#### Reloadable credentials file

Set `PITHOS_CREDENTIALS_PATH` to use a JSON credential set instead of the
indexed environment variables:

```json
{
  "credentials": [
    {
      "accessKeyId": "old-key",
      "secretAccessKey": "old-secret",
      "accountId": "storage-account",
      "principalId": "storage-client"
    },
    {
      "accessKeyId": "new-key",
      "secretAccessKey": "new-secret",
      "accountId": "storage-account",
      "principalId": "storage-client"
    }
  ]
}
```

Pithos validates the file before startup succeeds, then refreshes it in the
background at the configured interval. Authenticated requests always read the
most recently valid in-memory snapshot and never wait for file I/O. Valid
updates replace the complete credential set atomically. Malformed, incomplete,
oversized, or duplicate-key updates are rejected and the last valid set remains
active. An empty `credentials` array intentionally revokes every credential.
Set the interval to `0` to keep the startup snapshot for the process lifetime.
Publish changes using an atomic file replacement; when using Kubernetes, mount
the Secret as a volume rather than with `subPath`, which does not receive
automatic updates.

#### SQL credentials

Set `PITHOS_CREDENTIALS_PROVIDER=sql` to read credentials from the
`authentication_credentials` table in a database already configured by
`storage.json`. Database index `0` is the default and is the database used by
the default storage configuration. Enabled rows are periodically loaded into
an in-memory snapshot, so normal authentication does not perform a database
query. Pithos requires a valid initial query result to start; later transient
refresh failures retain the last valid set. Set the reload interval to `0` to
keep the initial snapshot for the process lifetime.

Credentials can initially be provisioned with SQL:

```sql
INSERT INTO authentication_credentials
    (access_key_id, secret_access_key, account_id, principal_id)
VALUES
    ('old-key', 'old-secret', 'storage-account', 'storage-client'),
    ('new-key', 'new-secret', 'storage-account', 'storage-client');
```

Set `enabled` to `FALSE` or delete a row to revoke it. The `version`,
`created_at`, and `updated_at` columns are reserved for managed updates by a
future administration API. Secret access keys are stored in reversible form
because SigV4 verification requires them; protect the database with strict
access controls and storage-level encryption.

Existing installations upgrading from v0.45.x or earlier to v0.46.x or later
must perform the [one-time account ownership update](account-ownership-update.md)
before serving client traffic after the account-ownership migration.

### Storage

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_STORAGE_JSON_PATH` | Path to the storage configuration file | `./storage.json` |
| `PITHOS_SPOOL_DIR` | Directory for temporary files used when operations must spool data to disk | Platform temporary directory |

`PITHOS_SPOOL_DIR` must reference an existing directory writable by the Pithos
process. If it is unset or empty, Pithos uses the platform temporary directory,
including the standard `TMPDIR` environment variable on Unix-like systems.
Spool files are removed when their operation completes, but files left behind
by an interrupted process may need to be cleaned up separately.

### Monitoring

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_MONITORING_PORT` | Port for monitoring endpoints | `9090` |
| `PITHOS_MONITORING_PORT_ENABLED` | Enable/disable the monitoring port | `true` |
| `PITHOS_METRICS_GAUGES_INTERVAL_SECONDS` | Interval in seconds for periodically sampled storage gauges | `30` |

### Logging

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_LOG_LEVEL` | Log level (`debug`, `info`, `warn`, `error`) | - |

## Setting Up Multiple Credentials

You can set up multiple credentials for different users or roles:

```sh
export PITHOS_CREDENTIALS_1_ACCESS_KEY_ID="admin-access-key-id"
export PITHOS_CREDENTIALS_1_SECRET_ACCESS_KEY="admin-secret-access-key"
export PITHOS_CREDENTIALS_1_ACCOUNT_ID="admin-account"
export PITHOS_CREDENTIALS_1_PRINCIPAL_ID="admin"
export PITHOS_CREDENTIALS_2_ACCESS_KEY_ID="my-bucket-admin-access-key-id"
export PITHOS_CREDENTIALS_2_SECRET_ACCESS_KEY="my-bucket-admin-secret-access-key"
export PITHOS_CREDENTIALS_2_ACCOUNT_ID="bucket-account"
export PITHOS_CREDENTIALS_2_PRINCIPAL_ID="bucket-admin"
export PITHOS_CREDENTIALS_3_ACCESS_KEY_ID="my-bucket-readonly-access-key-id"
export PITHOS_CREDENTIALS_3_SECRET_ACCESS_KEY="my-bucket-readonly-secret-access-key"
export PITHOS_CREDENTIALS_3_ACCOUNT_ID="bucket-account"
export PITHOS_CREDENTIALS_3_PRINCIPAL_ID="bucket-reader"
```

## Lua Authorizer Script

Both authorizers ignore forwarding headers unless `PITHOS_TRUST_FORWARDED_HEADERS`
is enabled and the direct peer belongs to an explicitly configured trusted CIDR.
Enabling trust without CIDRs, or configuring any invalid CIDR, prevents startup.
Configure only proxy addresses under your control, not client networks.

`X-Forwarded-For` is read from right to left, skipping trusted proxy hops and
stopping at the first untrusted address. Values further left cannot override
that client address. Multiple header lines are processed as one chain. A
malformed hop encountered during traversal falls back to the direct peer.
`CF-Connecting-IP` is used only when `X-Forwarded-For` is absent.

The trusted ingress must remove or overwrite client-supplied `CF-Connecting-IP`
and `X-Forwarded-Proto`; merely forwarding those headers is unsafe.
`X-Forwarded-Proto` must contain a single `http` or `https` value. Repeated or
comma-separated scheme values are ignored in favor of the direct connection's
scheme. These rules apply equally to Lua and policy authorization.

The Lua authorizer script controls access to all operations, including anonymous requests from the website endpoint. The `authorizeRequest` function receives a `request` object and must return `true` to allow or `false` to deny.

Only actual Lua booleans are accepted. Strings (including `"false"`), numbers
(including `0`), tables, `nil`, and missing return values are authorization
errors and deny access. Startup validates the script with a sample request;
the return type is also checked on every real request. Top-level script return
values never count as authorization decisions.

Malformed query strings and repeated query parameters are rejected with HTTP
400 (`InvalidArgument`) before authentication and before Lua runs. Each query
parameter therefore has at most one value, shared by authorization and the
operation handler. This applies to API and website requests alike.

### Default Behaviour (no authorizer.lua)

When the Lua backend is selected, the authentication setting controls whether
a missing script is allowed:

| Authentication | Default behaviour |
|----------------|-------------------|
| Explicitly disabled | All requests are allowed (permissive mode, suitable for local development) |
| Enabled | Startup fails if the script is missing or unreadable |

Unreadable files, invalid scripts, missing `authorizeRequest` functions, and
invalid return types detected at startup are errors in both modes. The
development fallback applies only to a missing file with authentication
explicitly disabled. The policy backend always requires a valid policy file.

Upgrade note: authenticated deployments that previously relied on the built-in
Lua fallback must now provide an explicit script. To deliberately allow all
authenticated callers within their own account, use:

```lua
function authorizeRequest(request)
  return not request:isAnonymous()
end
```

This grants broad access within the caller's account. Use narrower rules when
principals within an account need different permissions.

Authentication is disabled only when `PITHOS_AUTHENTICATION_ENABLED=false` (or
the equivalent `-authenticationEnabled=false` flag) is set explicitly. In this
mode no credentials are loaded and account-ownership boundaries are not
enforced, because requests have no authenticated account. Do not use this mode
for an internet-facing or multi-tenant deployment. A custom Lua authorizer is
still evaluated when present.

Anonymous API and website reads still pass through `authorizeRequest`; they are
served only when Lua allows the corresponding `GetObject`, `HeadObject`,
`GetObjectVersion`, or `HeadObjectVersion` operation.

Provide the script at the path set by `PITHOS_AUTHORIZER_PATH` (default
`./authorizer.lua`).

### Request Object

| Field | Type | Description |
|-------|------|-------------|
| `request.operation` | `string` | The S3 operation being performed (e.g. `"GetObject"`, `"PutObject"`) |
| `request.authorization.accessKeyId` | `string\|nil` | The Access Key ID of the caller, or `nil` for anonymous requests |
| `request.authorization.accountId` | `string\|nil` | The caller's account ID, or `nil` for anonymous requests |
| `request.authorization.principalId` | `string\|nil` | The configured stable principal ID, or `nil` for anonymous requests |
| `request.authorization.authType` | `string` | `Anonymous`, `REST-HEADER`, or `REST-QUERY-STRING`, from the server's authentication result; use this to distinguish signed headers from presigned URLs |
| `request.resourceAccountId` | `string\|nil` | The owning account of the target bucket; for `CreateBucket`, the caller's account |
| `request.bucket` | `string\|nil` | The bucket name (the destination for copy operations), or `nil` for bucket-list operations |
| `request.key` | `string\|nil` | The object key (the destination for copy operations), or `nil` for bucket-level operations |
| `request.sourceBucket` | `string\|nil` | The copy source bucket for `CopyObject`/`UploadPartCopy`, otherwise `nil` |
| `request.sourceKey` | `string\|nil` | The copy source key for `CopyObject`/`UploadPartCopy`, otherwise `nil` |
| `request.httpRequest.method` | `string` | The incoming HTTP method (for example, `"GET"`, `"PUT"`) |
| `request.httpRequest.path` | `string` | The incoming HTTP path (without query string) |
| `request.httpRequest.query` | `string` | The raw query string without the leading `?` |
| `request.httpRequest.queryParams` | `table<string, string[]>` | Parsed query parameters as provided by Go's `net/url` (`map[string][]string`) |
| `request.httpRequest.headers` | `table<string, string[]>` | HTTP headers as provided by Go's `net/http` (canonical header names) |
| `request.httpRequest.host` | `string` | The incoming request host (from the HTTP Host header / request target host) |
| `request.httpRequest.proto` | `string` | The HTTP protocol version (for example, `"HTTP/1.1"`, `"HTTP/2.0"`) |
| `request.httpRequest.contentLength` | `number\|nil` | The request content length when known; `nil` if unknown |
| `request.httpRequest.remoteAddr` | `string` | The raw peer network address from Go's `RemoteAddr` (`ip:port`) |
| `request.httpRequest.remoteIP` | `string\|nil` | The parsed IP/host portion extracted from `remoteAddr`, when available |
| `request.httpRequest.clientIP` | `string\|nil` | Client IP used for policy checks; derived from trusted forwarding headers when enabled, otherwise `remoteIP` |
| `request.httpRequest.scheme` | `string` | Request scheme (`"https"`/`"http"`); may use trusted `X-Forwarded-Proto` when enabled |
| `request.httpRequest:isMethod(method)` | `boolean` | Returns `true` if the HTTP method matches `method` (case-insensitive) |
| `request.httpRequest:header(name)` | `string\|nil` | Returns the first value for header `name`, or `nil` if absent |
| `request.httpRequest:hasHeader(name)` | `boolean` | Returns `true` if request header `name` is present (header name match is case-insensitive) |
| `request.httpRequest:headerEquals(name, value)` | `boolean` | Returns `true` if request header `name` contains a value exactly matching `value` |
| `request.httpRequest:queryParam(name)` | `string\|nil` | Returns the first value for query parameter `name`, or `nil` if absent |
| `request.httpRequest:hasQueryParam(name)` | `boolean` | Returns `true` if query parameter `name` is present |
| `request.httpRequest:queryParamEquals(name, value)` | `boolean` | Returns `true` if query parameter `name` contains a value exactly matching `value` |
| `request.httpRequest:pathEquals(path)` | `boolean` | Returns `true` if the request path exactly matches `path` |
| `request.httpRequest:pathHasPrefix(prefix)` | `boolean` | Returns `true` if the request path starts with `prefix` |
| `request.httpRequest:hostEquals(host)` | `boolean` | Returns `true` if host exactly matches `host` (case-insensitive) |
| `request.httpRequest:hostHasSuffix(suffix)` | `boolean` | Returns `true` if host ends with `suffix` (case-insensitive) |
| `request.httpRequest:isScheme(scheme)` | `boolean` | Returns `true` if request scheme matches `scheme` (case-insensitive) |
| `request.httpRequest:isProto(proto)` | `boolean` | Returns `true` if request protocol matches `proto` (case-insensitive) |
| `request.httpRequest:clientIPInCIDR(cidr)` | `boolean` | Returns `true` if `clientIP` is inside CIDR `cidr` |
| `request.httpRequest:clientIPInCIDRs(cidrs)` | `boolean` | Returns `true` if `clientIP` is inside any CIDR in `cidrs` |
| `request.httpRequest:remoteIPInCIDR(cidr)` | `boolean` | Returns `true` if `remoteIP` is inside CIDR `cidr` |
| `request:isReadOnly()` | `boolean` | Returns `true` if the operation is read-only |
| `request:isWriteOperation()` | `boolean` | Returns `true` if the operation is not read-only |
| `request:isOperation(operation)` | `boolean` | Returns `true` if `request.operation` matches `operation` |
| `request:isOperationIn(operations)` | `boolean` | Returns `true` if `request.operation` matches any value in `operations` |
| `request:isAnonymous()` | `boolean` | Returns `true` if the request has no credentials (i.e. `accessKeyId` is `nil`) |
| `request:hasAccessKeyId()` | `boolean` | Returns `true` if `request.authorization.accessKeyId` is present |
| `request:accessKeyIdEquals(value)` | `boolean` | Returns `true` if `accessKeyId` exactly matches `value` |
| `request:accessKeyIdIn(values)` | `boolean` | Returns `true` if `accessKeyId` matches any value in `values` |
| `request:hasPrincipalId()` | `boolean` | Returns `true` if `request.authorization.principalId` is present |
| `request:principalIdEquals(value)` | `boolean` | Returns `true` if `principalId` exactly matches `value` |
| `request:principalIdIn(values)` | `boolean` | Returns `true` if `principalId` matches any value in `values` |
| `request:bucketEquals(bucket)` | `boolean` | Returns `true` if request bucket exactly matches `bucket` |
| `request:keyHasPrefix(prefix)` | `boolean` | Returns `true` if request key starts with `prefix` |
| `request:keyHasSuffix(suffix)` | `boolean` | Returns `true` if request key ends with `suffix` |

### Available Operations

`ListBuckets`, `HeadBucket`, `CreateBucket`, `DeleteBucket`, `ListObjects`, `ListObjectVersions`, `HeadObject`, `HeadObjectVersion`, `GetObject`, `GetObjectVersion`, `PutObject`, `CopyObject`, `AppendObject`, `DeleteObject`, `DeleteObjectVersion`, `DeleteObjects`, `ListMultipartUploads`, `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `GetBucketCORS`, `PutBucketCORS`, `DeleteBucketCORS`, `GetBucketLifecycle`, `PutBucketLifecycle`, `DeleteBucketLifecycle`, `GetBucketWebsite`, `PutBucketWebsite`, `DeleteBucketWebsite`, `GetBucketVersioning`, `PutBucketVersioning`, `GetObjectTagging`, `GetObjectVersionTagging`, `PutObjectTagging`, `PutObjectVersionTagging`, `DeleteObjectTagging`, `DeleteObjectVersionTagging`

Requests that target an explicit object version through the `versionId` query parameter use `HeadObjectVersion`, `GetObjectVersion`, `DeleteObjectVersion`, `GetObjectVersionTagging`, `PutObjectVersionTagging`, or `DeleteObjectVersionTagging`. Bucket versioning configuration uses `GetBucketVersioning` and `PutBucketVersioning`; `GET ?versions` uses `ListObjectVersions`.

Server-side copies (`CopyObject` and `UploadPartCopy`, requested via the `x-amz-copy-source` header) are authorized as a single `CopyObject` / `UploadPartCopy` operation. For these operations the request carries both the destination (`request.bucket` / `request.key`) and the copy source (`request.sourceBucket` / `request.sourceKey`), so a policy can reason about both ends in one check.

Account ownership is enforced before Lua runs. Authenticated requests can only
target buckets owned by their account, and copy operations require both source
and destination buckets to have that owner. `ListBuckets` only returns the
caller's buckets. Object, multipart-upload, and part listings are authorized
once and are not filtered item by item. Multi-delete authorizes each entry as
`DeleteObject` or `DeleteObjectVersion`. For S3 compatibility, a missing bucket
returns `404 NoSuchBucket`; an existing bucket owned by another account returns
`403 Forbidden`. Lua cannot override either result.

### Examples

#### Multi-user access control

```lua
GLOBAL_ADMIN_ACCESS_KEY_ID="admin-access-key-id"
MY_BUCKET_ADMIN_ACCESS_KEY_ID="my-bucket-admin-access-key-id"
MY_BUCKET_READONLY_ACCESS_KEY_ID="my-bucket-readonly-access-key-id"

MY_BUCKET="my-bucket"

function authorizeRequest(request)
  bucket = request.bucket
  authorization = request.authorization

  -- Check admin
  if authorization.accessKeyId == GLOBAL_ADMIN_ACCESS_KEY_ID then
    return true
  end

  if bucket == MY_BUCKET then
    if authorization.accessKeyId == MY_BUCKET_ADMIN_ACCESS_KEY_ID then
      return true
    end
    if authorization.accessKeyId == MY_BUCKET_READONLY_ACCESS_KEY_ID then
      return request:isReadOnly()
    end
  end

  return false
end
```

#### Public website bucket

To serve a bucket via the [website endpoint](configuration.md#pithos_website_domain), anonymous `GetObject` and `HeadObject` requests must be allowed. Authenticated requests still require a valid Access Key ID:

```lua
PUBLIC_BUCKET="my-public-bucket"

function authorizeRequest(request)
  -- Allow CORS preflight through for browser uploads.
  if request.httpRequest.method == "OPTIONS" then
    return true
  end

  -- Allow anonymous read access to the public bucket (required for website hosting)
  if request:isAnonymous()
      and request.bucket == PUBLIC_BUCKET
      and request:isOperationIn({"GetObject", "HeadObject"}) then
    return true
  end

  -- All other requests require authentication
  if request:isAnonymous() then
    return false
  end

  return request.authorization.accessKeyId == "my-access-key-id"
end
```

### Object Lock authorization fields

Lock requests expose `request.versionID`, `objectLockEnabled`, `objectLockMode`,
`objectLockRetainUntilDate`, `objectLockLegalHold`, `objectLockDays`,
`objectLockYears`, and `bypassGovernanceRetentionRequested`. The six lock APIs
have independent operation names. Governance bypass requires both the normal
operation and `BypassGovernanceRetention` to be allowed, per version for
Multi-Delete. See the [Lua example](object-lock.md#authorization).
