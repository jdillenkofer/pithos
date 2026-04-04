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
| `PITHOS_AUTHORIZER_PATH` | Path to the Lua authorization script | `./authorizer.lua` |
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
| `request.bucket` | `string\|nil` | The bucket name, or `nil` for bucket-list operations |
| `request.key` | `string\|nil` | The object key, or `nil` for bucket-level operations |
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
| `request.httpRequest:hasXApiKey(value)` | `boolean` | Returns `true` if an `X-Api-Key` request header matches `value` (header name is matched case-insensitively) |
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

`ListBuckets`, `HeadBucket`, `CreateBucket`, `DeleteBucket`, `ListObjects`, `HeadObject`, `GetObject`, `PutObject`, `DeleteObject`, `ListMultipartUploads`, `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `GetBucketWebsite`, `PutBucketWebsite`, `DeleteBucketWebsite`

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
