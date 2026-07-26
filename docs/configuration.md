# Configuration

## Environment Variables

### Basic Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_BIND_ADDRESS` | IP address to bind the server to | `0.0.0.0` |
| `PITHOS_HTTP_ENABLED` | Enable the S3/website HTTP listener | `true` |
| `PITHOS_PORT` | S3/website HTTP port | `9000` |
| `PITHOS_HTTPS_ENABLED` | Enable the S3/website HTTPS listener | `false` |
| `PITHOS_HTTPS_PORT` | S3/website HTTPS port | `9443` |
| `PITHOS_DOMAIN` | Domain name of the server | `localhost` |
| `PITHOS_WEBSITE_DOMAIN` | Domain name of the website | `s3-website.localhost` |
| `PITHOS_REGION` | AWS region for authentication | `eu-central-1` |

HTTP and HTTPS are independent and serve the same S3 and website routes. Pithos
does not redirect HTTP to HTTPS automatically. At least one S3 listener must be
enabled.

### Authentication and Authorization

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_AUTHENTICATION_ENABLED` | Enable/disable authentication | `true` |
| `PITHOS_CREDENTIALS_[N]_ACCESS_KEY_ID` | Access Key ID for the Nth user | - |
| `PITHOS_CREDENTIALS_[N]_SECRET_ACCESS_KEY` | Secret Access Key for the Nth user | - |
| `PITHOS_AUTHORIZER_PATH` | Path to the Lua authorization script | `./authorizer.lua` |
| `PITHOS_TRUST_FORWARDED_HEADERS` | Trust proxy forwarding headers for `clientIP` and `scheme` (`X-Forwarded-For`, `X-Forwarded-Proto`, `CF-Connecting-IP`) | `false` |
| `PITHOS_TRUSTED_PROXY_CIDRS` | Comma-separated trusted proxy CIDRs; used only when forwarded headers are trusted (if unset, all proxy IPs are trusted) | - |

> **Note:** Credentials cannot be set via command-line arguments for security reasons; they must be set using environment variables.

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
| `PITHOS_MONITORING_HTTPS_ENABLED` | Enable the HTTPS monitoring listener | `false` |
| `PITHOS_MONITORING_HTTPS_PORT` | HTTPS port for monitoring endpoints | `9444` |

Monitoring routes remain isolated from S3 routes on both transports.

### TLS and ACME

Both HTTPS listeners share one certificate source. Configure either a static
certificate/key pair or ACME, never both.

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_TLS_CERT_FILE` | PEM certificate chain for static TLS | - |
| `PITHOS_TLS_KEY_FILE` | PEM private key for static TLS | - |
| `PITHOS_ACME_ENABLED` | Manage certificates automatically; enabling this accepts the CA terms | `false` |
| `PITHOS_ACME_DOMAINS` | Comma-separated certificate names | - |
| `PITHOS_ACME_EMAIL` | Optional ACME account email | - |
| `PITHOS_ACME_CACHE_DIR` | Persistent, writable certificate/account cache | - |
| `PITHOS_ACME_CA_DIRECTORY_URL` | ACME directory endpoint | Let's Encrypt production |
| `PITHOS_ACME_CHALLENGE` | `auto`, `http-01`, `tls-alpn-01`, or `dns-01` | `auto` |
| `PITHOS_ACME_DNS_PROVIDER` | DNS provider used by `dns-01` | - |

`auto` enables HTTP-01 when any HTTP listener is active and TLS-ALPN-01 on the
HTTPS listeners. Public ACME validation still reaches port 80 for HTTP-01 and
port 443 for TLS-ALPN-01: forward those public ports to the configured internal
ports. DNS-01 does not require an inbound challenge port and is required for
wildcard certificates.

Supported DNS provider names are `cloudflare`, `route53`, `gcloud`, `azuredns`,
`digitalocean`, `hetzner`, `dnsupdate` (or `rfc2136`), `exec`, and `httpreq`.
Credentials use the corresponding
[lego provider environment variables](https://go-acme.github.io/lego/dns/).
Their `_FILE` forms are supported, which is useful with container secrets.

S3 virtual-host addressing normally needs both the base name and its wildcard
name, for example `s3.example.com,*.s3.example.com`. The wildcard makes
DNS-01 mandatory.

#### Static TLS

```sh
PITHOS_HTTPS_ENABLED=true \
PITHOS_TLS_CERT_FILE=/run/secrets/tls.crt \
PITHOS_TLS_KEY_FILE=/run/secrets/tls.key \
pithos serve
```

This keeps the default HTTP listener active alongside HTTPS. Static files are
loaded once during startup and are not hot-reloaded.

#### Automatic ACME

```sh
PITHOS_HTTPS_ENABLED=true \
PITHOS_ACME_ENABLED=true \
PITHOS_ACME_DOMAINS=s3.example.com \
PITHOS_ACME_EMAIL=admin@example.com \
PITHOS_ACME_CACHE_DIR=/data/acme \
pithos serve
```

Forward public TCP 80 to port 9000 and public TCP 443 to port 9443. To bind
public 443 directly, set `PITHOS_HTTPS_PORT=443` and grant the process
`CAP_NET_BIND_SERVICE` (for Docker, `--cap-add NET_BIND_SERVICE`).

#### DNS wildcard and HTTPS-only

```sh
PITHOS_HTTP_ENABLED=false \
PITHOS_HTTPS_ENABLED=true \
PITHOS_ACME_ENABLED=true \
PITHOS_ACME_DOMAINS='s3.example.com,*.s3.example.com' \
PITHOS_ACME_CACHE_DIR=/data/acme \
PITHOS_ACME_CHALLENGE=dns-01 \
PITHOS_ACME_DNS_PROVIDER=cloudflare \
CLOUDFLARE_DNS_API_TOKEN_FILE=/run/secrets/cloudflare-token \
pithos serve
```

#### Monitoring HTTPS

```sh
PITHOS_MONITORING_HTTPS_ENABLED=true \
PITHOS_HTTPS_ENABLED=true \
PITHOS_TLS_CERT_FILE=/run/secrets/tls.crt \
PITHOS_TLS_KEY_FILE=/run/secrets/tls.key \
pithos serve
```

The monitoring HTTP listener on 9090 remains enabled in this example; set
`PITHOS_MONITORING_PORT_ENABLED=false` for monitoring HTTPS only.

### Logging

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_LOG_LEVEL` | Log level (`debug`, `info`, `warn`, `error`) | - |

## Setting Up Multiple Credentials

You can set up multiple credentials for different users or roles:

```sh
export PITHOS_CREDENTIALS_1_ACCESS_KEY_ID="admin-access-key-id"
export PITHOS_CREDENTIALS_1_SECRET_ACCESS_KEY="admin-secret-access-key"
export PITHOS_CREDENTIALS_2_ACCESS_KEY_ID="my-bucket-admin-access-key-id"
export PITHOS_CREDENTIALS_2_SECRET_ACCESS_KEY="my-bucket-admin-secret-access-key"
export PITHOS_CREDENTIALS_3_ACCESS_KEY_ID="my-bucket-readonly-access-key-id"
export PITHOS_CREDENTIALS_3_SECRET_ACCESS_KEY="my-bucket-readonly-secret-access-key"
```

## Lua Authorizer Script

The Lua authorizer script controls access to all operations, including anonymous requests from the website endpoint. The `authorizeRequest` function receives a `request` object and must return `true` to allow or `false` to deny.

### Default Behaviour (no authorizer.lua)

When no `authorizer.lua` file is found, pithos selects a built-in fallback based on whether credentials are configured:

| Credentials configured | Default behaviour |
|------------------------|-------------------|
| No | All requests are allowed (permissive mode, suitable for local development) |
| Yes | Anonymous requests are denied; authenticated requests are allowed |

To override either default, provide an `authorizer.lua` file at the path set by `PITHOS_AUTHORIZER_PATH`.

### Request Object

| Field | Type | Description |
|-------|------|-------------|
| `request.operation` | `string` | The S3 operation being performed (e.g. `"GetObject"`, `"PutObject"`) |
| `request.authorization.accessKeyId` | `string\|nil` | The Access Key ID of the caller, or `nil` for anonymous requests |
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
| `request:bucketEquals(bucket)` | `boolean` | Returns `true` if request bucket exactly matches `bucket` |
| `request:keyHasPrefix(prefix)` | `boolean` | Returns `true` if request key starts with `prefix` |
| `request:keyHasSuffix(suffix)` | `boolean` | Returns `true` if request key ends with `suffix` |

### Available Operations

`ListBuckets`, `HeadBucket`, `CreateBucket`, `DeleteBucket`, `ListObjects`, `ListObjectVersions`, `HeadObject`, `HeadObjectVersion`, `GetObject`, `GetObjectVersion`, `PutObject`, `CopyObject`, `AppendObject`, `DeleteObject`, `DeleteObjectVersion`, `DeleteObjects`, `ListMultipartUploads`, `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `GetBucketCORS`, `PutBucketCORS`, `DeleteBucketCORS`, `GetBucketLifecycle`, `PutBucketLifecycle`, `DeleteBucketLifecycle`, `GetBucketWebsite`, `PutBucketWebsite`, `DeleteBucketWebsite`, `GetBucketVersioning`, `PutBucketVersioning`, `GetObjectTagging`, `GetObjectVersionTagging`, `PutObjectTagging`, `PutObjectVersionTagging`, `DeleteObjectTagging`, `DeleteObjectVersionTagging`

Requests that target an explicit object version through the `versionId` query parameter use `HeadObjectVersion`, `GetObjectVersion`, `DeleteObjectVersion`, `GetObjectVersionTagging`, `PutObjectVersionTagging`, or `DeleteObjectVersionTagging`. Bucket versioning configuration uses `GetBucketVersioning` and `PutBucketVersioning`; `GET ?versions` uses `ListObjectVersions`.

Server-side copies (`CopyObject` and `UploadPartCopy`, requested via the `x-amz-copy-source` header) are authorized as a single `CopyObject` / `UploadPartCopy` operation. For these operations the request carries both the destination (`request.bucket` / `request.key`) and the copy source (`request.sourceBucket` / `request.sourceKey`), so a policy can reason about both ends in one check.

### Optional List Filtering Hooks

In addition to `authorizeRequest(request)`, you can define optional hooks to filter list results item-by-item:

```lua
function authorizeListBucket(request, bucketName)
  -- Return true if this bucket should be visible in ListBuckets
  return true
end

function authorizeListObject(request, key)
  -- Return true if this key (or common prefix) should be visible in ListObjects
  return true
end

function authorizeDeleteObjectEntry(request, key)
  -- Return true if this key should be deleted in DeleteObjects
  return true
end

function authorizeListMultipartUpload(request, key, uploadId)
  -- Return true if this upload should be visible in ListMultipartUploads
  return true
end

function authorizeListPart(request, partNumber)
  -- Return true if this part should be visible in ListParts
  return true
end
```

If a hook is not defined, items are allowed by default for backward compatibility.

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

To serve a bucket via the [website endpoint](configuration.md#pithos_website_domain), anonymous `GetObject` requests must be allowed. Authenticated requests still require a valid Access Key ID:

```lua
PUBLIC_BUCKET="my-public-bucket"

function authorizeRequest(request)
  -- Allow CORS preflight through for browser uploads.
  if request.httpRequest.method == "OPTIONS" then
    return true
  end

  -- Allow anonymous read access to the public bucket (required for website hosting)
  if request:isAnonymous() and request.operation == "GetObject" and request.bucket == PUBLIC_BUCKET then
    return true
  end

  -- All other requests require authentication
  if request:isAnonymous() then
    return false
  end

  return request.authorization.accessKeyId == "my-access-key-id"
end
```
