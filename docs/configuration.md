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
| `PITHOS_AUTHENTICATION_ENABLED` | Enable/disable authentication | `true` |
| `PITHOS_CREDENTIALS_[N]_ACCESS_KEY_ID` | Access Key ID for the Nth user | - |
| `PITHOS_CREDENTIALS_[N]_SECRET_ACCESS_KEY` | Secret Access Key for the Nth user | - |
| `PITHOS_AUTHORIZER_PATH` | Path to the authorization policy (`.lua` or `.wasm`) | `./authorizer.lua` |
| `PITHOS_AUTHORIZER_TYPE` | Authorizer runtime: `lua` or `wasm` | `lua` |
| `PITHOS_AUTHORIZER_TIMEOUT_MILLIS` | Maximum duration of a single Wasm authorizer call in milliseconds | `100` |
| `PITHOS_AUTHORIZER_MEMORY_LIMIT_PAGES` | Maximum Wasm memory pages per authorizer instance (`64 KiB` per page) | `64` |
| `PITHOS_AUTHORIZER_INSTANCE_POOL_SIZE` | Number of Wasm authorizer instances to keep pooled; `0` uses `GOMAXPROCS`, negative disables pooling | `0` |
| `PITHOS_AUTHORIZER_MAX_DECISION_BYTES` | Maximum Wasm authorizer decision JSON size in bytes | `4096` |
| `PITHOS_TRUST_FORWARDED_HEADERS` | Trust proxy forwarding headers for `clientIP` and `scheme` (`X-Forwarded-For`, `X-Forwarded-Proto`, `CF-Connecting-IP`) | `false` |
| `PITHOS_TRUSTED_PROXY_CIDRS` | Comma-separated trusted proxy CIDRs; used only when forwarded headers are trusted (if unset, all proxy IPs are trusted) | - |

> **Note:** Credentials cannot be set via command-line arguments for security reasons; they must be set using environment variables.

### Storage

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_STORAGE_JSON_PATH` | Path to the storage configuration file | `./storage.json` |

### Monitoring

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_MONITORING_PORT` | Port for monitoring endpoints | `9090` |
| `PITHOS_MONITORING_PORT_ENABLED` | Enable/disable the monitoring port | `true` |

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

## Authorizer Runtime

Pithos supports the built-in Lua authorizer and an experimental Wasm authorizer. Lua remains the default for compatibility. To load a Wasm policy, set `PITHOS_AUTHORIZER_TYPE=wasm` and point `PITHOS_AUTHORIZER_PATH` at a `.wasm` module.

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

## Wasm Authorizer Module

The Wasm authorizer uses the same request model as Lua, but the executable v1 ABI is a small core-Wasm interface so modules can be produced by languages such as Rust, TinyGo, Zig, or AssemblyScript.

The normative type model lives in [`authorizer.wit`](authorizer.wit). The current wazero adapter passes the WIT-shaped input as UTF-8 JSON:

```json
{
  "hook": "request",
  "request": {
    "operation": "GetObject",
    "authorization": { "accessKeyId": "my-access-key-id" },
    "bucket": "my-bucket",
    "key": "photos/cat.jpg",
    "httpRequest": {
      "method": "GET",
      "path": "/photos/cat.jpg",
      "query": "",
      "queryParams": [],
      "headers": [],
      "host": "localhost:9000",
      "proto": "HTTP/1.1",
      "remoteAddr": "127.0.0.1:12345",
      "remoteIP": "127.0.0.1",
      "clientIP": "127.0.0.1",
      "scheme": "http"
    },
    "isReadOnly": true
  }
}
```

The guest module must export:

| Export | Signature | Description |
|--------|-----------|-------------|
| `memory` | WebAssembly memory | Linear memory used for input and output buffers |
| `pithos_alloc` | `(size: i32) -> i32` | Allocates guest memory for a buffer |
| `pithos_free` | `(ptr: i32, len: i32) -> nil` | Frees a guest buffer allocated by `pithos_alloc` |
| `pithos_evaluate` | `(ptr: i32, len: i32) -> i64` | Reads an input JSON buffer and returns a packed output pointer/length |

`pithos_evaluate` returns `(ptr << 32) | len`, where `ptr` and `len` identify a UTF-8 JSON decision buffer:

```json
{ "allow": true, "reason": null }
```

The `hook` field is one of `request`, `list-bucket`, `list-object`, `delete-object-entry`, `list-multipart-upload`, or `list-part`. Resource hook calls include a `resource` object containing the bucket name, key, upload ID, or part number relevant to the hook.

Traps, malformed JSON results, missing exports, allocation failures, timeouts, and tag resolver errors fail closed and deny the request.

See [Wasm Authorizer Examples](wasm-authorizer-examples.md) for Rust and Go policies that implement this ABI.

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
