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
| `request:isReadOnly()` | `boolean` | Returns `true` if the operation is read-only |
| `request:isAnonymous()` | `boolean` | Returns `true` if the request has no credentials (i.e. `accessKeyId` is `nil`) |

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
