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

### Verifying Releases
Pithos releases (Docker images and binaries) are signed using [Cosign](https://github.com/sigstore/cosign) (Sigstore). You can verify the authenticity of the artifacts using the following commands.

#### Verifying Docker Images
```sh
cosign verify jdillenkofer/pithos:latest \
  --certificate-identity-regexp "https://github.com/jdillenkofer/pithos" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

#### Verifying Binaries
Download the `checksums.txt`, `checksums.txt.pem`, and `checksums.txt.sig` files from the release page along with the binary you want to use.

1. Verify the signature of the checksums file:
```sh
cosign verify-blob \
  --certificate checksums.txt.pem \
  --signature checksums.txt.sig \
  checksums.txt \
  --certificate-identity-regexp "https://github.com/jdillenkofer/pithos" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

2. Verify the binary's checksum:
```sh
sha256sum -c checksums.txt --ignore-missing
```

### CLI Subcommands

Pithos provides several subcommands for managing and maintaining the storage server.

#### `serve`
Starts the S3-compatible object storage server.
```sh
pithos serve [options]
```
For a complete list of available command-line arguments, run `./pithos serve --help`.

#### `migrate-storage`
Migrates data between two different storage configurations.
```sh
pithos migrate-storage <source-config.json> <destination-config.json>
```
The migration is performed bucket by bucket. To prevent data loss, it will not overwrite existing objects in the destination.

#### `benchmark-storage`
Measures the performance of a storage configuration.
```sh
pithos benchmark-storage <config.json>
```
This command performs upload and download benchmarks with various object sizes and reports the speeds.

#### `validate-storage`
Checks the integrity of all objects in the storage.
```sh
pithos validate-storage <config.json> [options]
```
Available options:
- `-delete-corrupted`: Delete objects that fail integrity checks.
- `-force`: Force deletion without confirmation.
- `-json`: Output results in JSON format.
- `-output <path>`: Write the validation report to a file.

#### `audit-log`
Provides tools for verifying, dumping, and analyzing audit logs.
```sh
pithos audit-log <verify|dump|stats> [options]
```
Common options:
- `-input-file <path>`: (Required) Path to the audit log file.
- `-input-format <bin|json|text>`: Input format (default: `bin`).
- `-ed25519-public-key <key>`: (Required) Base64 encoded Ed25519 public key or path to key file.
- `-ml-dsa-public-key <key>`: Base64 encoded ML-DSA public key or path to key file.

Subcommand specific options:
- `dump`: Supports `-output-format <json|text|bin>` and `-output-file <path>`.

**Example: Verify an audit log**
```sh
pithos audit-log verify -input-file ./data/audit.log -ed25519-public-key nHBi++...
```

### Configuration

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
- **MetadataPartStorage**: A storage backend that separates metadata and part storage
  - Supports various metadata stores (SQL databases: SQLite, PostgreSQL)
  - Configurable part stores (filesystem, SFTP)
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
- **AuditStorage**: Provides cryptographically signed audit logs for all storage operations
  - Supports multiple output sinks (Binary, JSON, Text)
  - Chained hashing for tamper detection
  - Dual-signatures for grounding: Ed25519 and ML-DSA (Post-Quantum)
  - Automated grounding every 1,000 entries using Merkle Trees for high-integrity checkpoints

###### Part Store Middleware
- **TinkEncryptionPartStoreMiddleware**: Advanced encryption using Google Tink with support for AWS KMS, HashiCorp Vault, and local KMS. Features envelope encryption and key rotation capabilities
- **OutboxPartStore**: Implements outbox pattern for reliable part operations

The default configuration (using SQLite) looks like this:
```json
{
  "type": "MetadataPartStorage",
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
  "partStore": {
    "type": "SqlPartStore",
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
  "type": "MetadataPartStorage",
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
  "partStore": {
    "type": "SqlPartStore",
    "db": {
      "type": "DatabaseReference",
      "refName": "db"
    }
  }
}
```

##### Audit Log Middleware Configuration
You can wrap any storage backend with the `AuditStorageMiddleware` to enable operation logging. This example shows how to configure it with a binary file sink:

```json
{
  "type": "AuditStorageMiddleware",
  "innerStorage": {
     "type": "MetadataPartStorage",
     ...
  },
  "ed25519PrivateKey": "<Base64-encoded-Ed25519-private-key>",
  "mlDsaPrivateKey": "<Base64-encoded-ML-DSA-65-private-key>",
  "sinks": [
    {
      "type": "file",
      "format": "bin",
      "path": "./data/audit.log"
    }
  ]
}
```

##### Audit Log Security Recommendation
To prevent any program (including Pithos) from overwriting or truncating existing audit logs, you can use filesystem-level append-only attributes. This ensures that data can only be added to the end of the file.

**Linux:**
Requires root privileges or `CAP_LINUX_IMMUTABLE`.
```sh
sudo chattr +a ./data/audit.log
```

**macOS:**
- **User level** (Owner can set/unset): `chflags uappnd ./data/audit.log`
- **System level** (Root only): `sudo chflags sappnd ./data/audit.log`

For maximum security on macOS, use `sappnd`. This prevents the Pithos process (if running as the file owner) from removing the protection if it were to be compromised.

To disable the protection (e.g., to archive or delete the log), use:

- **Linux:** `sudo chattr -a ./data/audit.log`
- **macOS:** `chflags nouappnd ./data/audit.log` or `sudo chflags nosappnd ./data/audit.log`

With these attributes set, Pithos can still read the file to retrieve the last hash for the chain, but it cannot delete or modify previous entries.

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

##### Audit Log Verification and Conversion
Pithos provides the `audit-log` subcommand to verify the cryptographic integrity of audit logs and convert them between different formats (Binary, JSON, Text). The tool verifies the hash chain, individual entry signatures, and validates grounding checkpoints (Merkle Roots) every 1,000 entries.

```sh
pithos audit-log [log-file] --public-key <Base64-Ed25519-Key> [--ml-dsa-public-key <Base64-ML-DSA-Key>] [options]
```

Available options:
- `--public-key`: (Required) The Base64 encoded Ed25519 public key corresponding to the private key used for signing entries and grounding events.
- `--ml-dsa-public-key`: (Optional) The Base64 encoded ML-DSA-65 public key used to verify the post-quantum grounding signatures.
- `--format`: Output format. Options: `json` (default), `text`, `bin`.
- `--output`: Output path. Use `-` for stdout (default).

**Example: Convert binary log to human-readable text**
```sh
pithos audit-log ./data/audit.log --public-key nHBi++... --format text --output audit_report.txt
```

#### Monitoring
We support Prometheus metrics for monitoring the server. You can enable the monitoring endpoints by setting the following environment variables:
- `PITHOS_MONITORING_PORT`: Port for monitoring endpoints (default: `9090`)
- `PITHOS_MONITORING_PORT_ENABLED`: Enable/disable the monitoring port (default: `true`)

#### Logging
For logging we use the standard Go structured logging library. You can configure the log level using the following environment variable:
- `PITHOS_LOG_LEVEL`: Log level for the server (options: `debug`, `info`, `warn`, `error`)

### License
Pithos is licensed under the [MIT License](LICENSE).