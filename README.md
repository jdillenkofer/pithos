# Pithos

Pithos is an S3-compatible object storage server designed for self-hosters, written in Go.

## Features

- S3-compatible API with extensive operation support:
  - Bucket Operations:
    - Create bucket
    - Head bucket
    - Delete bucket
    - List all buckets
  - Object Operations:
    - Head object
    - Get object
    - Put object
    - Delete object
    - List objects in bucket (with prefix and delimiter support)
  - Multipart Upload Operations:
    - Initiate multipart upload
    - Upload part
    - Complete multipart upload
    - Abort multipart upload
    - List multipart uploads
    - List parts
- Authentication using AWS Signature Version 4
- Authorization support via Lua scripts
- Configurable storage backends (local filesystem, S3, etc.)
- Prometheus metrics endpoint for monitoring
- Health monitoring endpoints
- Docker support for easy deployment

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Docker (optional)

### Installation

Clone the repository:

```sh
git clone https://github.com/jdillenkofer/pithos.git
cd pithos
```

### Build and Run
```sh
go build -o pithos ./cmd/pithos.go
./pithos serve
```

### Running Tests
```sh
go test ./... --integration
```
To run the unit tests with a subset of storage backends and features, you can use:
```sh
go test ./...
```

### Docker
You can also run Pithos using Docker:

```sh
docker build -t pithos .
docker run -p 9000:9000 -v $(pwd)/data:/data pithos
```

### Configuration
Pithos can be configured using command-line arguments or environment variables.
For a complete list of available command-line arguments, you can run:
```sh
./pithos serve --help
```

The following environment variables are available:

#### Basic Configuration
- `PITHOS_BIND_ADDRESS`: The IP address to bind the server to (default: `0.0.0.0`)
- `PITHOS_PORT`: The port to run the server on (default: `9000`)
- `PITHOS_DOMAIN`: The domain name of the server (default: `localhost`)
- `PITHOS_REGION`: The AWS region for authentication (default: `eu-central-1`)

#### Authentication and Authorization
- `PITHOS_AUTHENTICATION_ENABLED`: Enable/disable authentication (default: `true`)
- `PITHOS_CREDENTIALS_[N]_ACCESS_KEY_ID`: Access Key ID for the Nth user
- `PITHOS_CREDENTIALS_[N]_SECRET_ACCESS_KEY`: Secret Access Key for the Nth user
- `PITHOS_AUTHORIZER_PATH`: Path to the Lua authorization script (default: `./authorizer.lua`)

Credentials can not be set via command-line arguments for security reasons, they must be set using environment variables.

##### Example
You can set up multiple credentials for different users or roles. For example, you can have an admin user, a bucket admin, and a read-only user. The credentials can be set using environment variables like this:
```sh
export PITHOS_CREDENTIALS_1_ACCESS_KEY_ID="admin-access-key-id"
export PITHOS_CREDENTIALS_1_SECRET_ACCESS_KEY="admin-secret-access-key"
export PITHOS_CREDENTIALS_2_ACCESS_KEY_ID="my-bucket-admin-access-key-id"
export PITHOS_CREDENTIALS_2_SECRET_ACCESS_KEY="my-bucket-admin-secret-access-key"
export PITHOS_CREDENTIALS_3_ACCESS_KEY_ID="my-bucket-readonly-access-key-id"
export PITHOS_CREDENTIALS_3_SECRET_ACCESS_KEY="my-bucket-readonly-secret-access-key"
```

In the Lua authorizer script, you can implement custom authorization logic based on the access key IDs.
Here is an example Lua authorizer script:
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

#### Storage
- `PITHOS_STORAGE_JSON_PATH`: Path to the storage configuration file (default: `./storage.json`)

Pithos supports multiple storage backends that can be configured in the storage configuration. The filepath can be set using the `PITHOS_STORAGE_JSON_PATH` environment variable. If this variable is not set, Pithos will use the default path (e.g. `storage.json`).

The following storage backends and middlewares are available:

##### Storage Types

###### Primary Storage Backends
- **MetadataBlobStorage**: A storage backend that separates metadata and blob storage
  - Supports various metadata stores (SQL databases: SQLite, PostgreSQL)
  - Configurable blob stores (filesystem, SFTP)
- **S3ClientStorage**: Use an existing S3-compatible storage as backend
  - Compatible with other S3-compatible services
  - Configurable endpoint, region, and credentials

###### Enhancement Layers
- **CacheStorage**: Adds caching capabilities to any storage backend
  - Configurable cache policies (LFU, etc.)
  - Support for both in-memory and persistent caching
- **ReplicationStorage**: Enables replication across multiple storage backends
  - Supports primary-replica configuration

###### Storage Middleware
- **ConditionalStorage**: Implements conditional forwarding to different storage backends based on the bucket name
- **PrometheusStorage**: Adds Prometheus metrics for storage operations

###### Blob Store Middleware
- **TinkEncryptionBlobStoreMiddleware**: Advanced encryption using Google Tink with support for AWS KMS, HashiCorp Vault, and local KMS. Features envelope encryption and key rotation capabilities
- **OutboxBlobStore**: Implements outbox pattern for reliable blob operations

The default configuration (using SQLite) looks like this:
```json
{
  "type": "MetadataBlobStorage",
  "db": {
    "type": "RegisterDatabaseReference",
    "refName": "db",
    "db": {
      "type": "SqliteDatabase",
      "dbPath": "./data/pithos.db"
    }
  },
  "metadataStore": {
    "type": "SqlMetadataStore",
    "db": {
      "type": "DatabaseReference",
      "refName": "db"
    }
  },
  "blobStore": {
    "type": "SqlBlobStore",
    "db": {
      "type": "DatabaseReference",
      "refName": "db"
    }
  }
}
```

For PostgreSQL, you can use this configuration:
```json
{
  "type": "MetadataBlobStorage",
  "db": {
    "type": "RegisterDatabaseReference",
    "refName": "db",
    "db": {
      "type": "PostgresDatabase",
      "dbUrl": "postgres://pithos:your-password@localhost:5432/pithos"
    }
  },
  "metadataStore": {
    "type": "SqlMetadataStore",
    "db": {
      "type": "DatabaseReference",
      "refName": "db"
    }
  },
  "blobStore": {
    "type": "SqlBlobStore",
    "db": {
      "type": "DatabaseReference",
      "refName": "db"
    }
  }
}
```

##### Storage Migration
Pithos supports storage migration through the `migrate-storage` subcommand. 
This subcommand allows you to change your storage backend without losing data.
First specify the source and target storage configurations in two storage json files. 
After that simply run the command:
```sh
pithos migrate-storage ./storage_source.json ./storage_target.json
```

The migrate-storage subcommand migrates data bucket by bucket from the source storage to the target storage.
If the target storage bucket is not empty, it will not overwrite an existing object. Instead, it will log an error and exit the command to prevent accidental data loss.

#### Monitoring
We support Prometheus metrics for monitoring the server. You can enable the monitoring endpoints by setting the following environment variables:
- `PITHOS_MONITORING_PORT`: Port for monitoring endpoints (default: `9090`)
- `PITHOS_MONITORING_PORT_ENABLED`: Enable/disable the monitoring port (default: `true`)

#### Logging
For logging we use the standard Go structured logging library. You can configure the log level using the following environment variable:
- `PITHOS_LOG_LEVEL`: Log level for the server (options: `debug`, `info`, `warn`, `error`)

### License
Pithos is licensed under the [MIT License](LICENSE).