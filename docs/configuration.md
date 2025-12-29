# Configuration

## Environment Variables

### Basic Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PITHOS_BIND_ADDRESS` | IP address to bind the server to | `0.0.0.0` |
| `PITHOS_PORT` | Port to run the server on | `9000` |
| `PITHOS_DOMAIN` | Domain name of the server | `localhost` |
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

In the Lua authorizer script, you can implement custom authorization logic based on the access key IDs:

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
    if authorization.accessKeyId == BUCKET_ADMIN_ACCESS_KEY_ID then
      return true
    end
    if authorization.accessKeyId == BUCKET_READONLY_ACCESS_KEY_ID then
      return request:isReadOnly()
    end
  end

  return false
end
```
