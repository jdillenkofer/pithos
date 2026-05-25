# Storage Backends

Pithos supports multiple storage backends that can be configured in the storage configuration file. The filepath can be set using the `PITHOS_STORAGE_JSON_PATH` environment variable (default: `storage.json`).

## Storage Types

### Primary Storage Backends

- **MetadataPartStorage**: Separates metadata and part storage
  - Supports various metadata stores (SQL databases: SQLite, PostgreSQL)
  - Configurable part stores (filesystem, SFTP)
- **S3ClientStorage**: Use an existing S3-compatible storage as backend
  - Compatible with other S3-compatible services
  - Configurable endpoint, region, and credentials

### Enhancement Layers

- **CacheStorage**: Adds caching capabilities to any storage backend
  - Configurable cache policies (LFU, etc.)
  - Support for both in-memory and persistent caching
- **ReplicationStorage**: Enables replication across multiple storage backends
  - Supports primary-replica configuration

### Storage Middleware

- **ConditionalStorage**: Conditional forwarding to different storage backends based on bucket name
- **PrometheusStorage**: Adds Prometheus metrics for storage operations
- **AuditStorage**: Provides cryptographically signed audit logs (see [Audit Logging](audit-logging.md))

### Part Store Middleware

- **CompressionPartStoreMiddleware**: Compresses parts based on sample compression ratio checks
  - Supported `compressionAlgorithm` values: `gzip`, `zstd`
  - Defaults: `sampleSizeBytes=65536`, `compressionAlgorithm="zstd"`, `maxCompressionRatio=0.95`
- **TinkEncryptionPartStoreMiddleware**: Advanced encryption using Google Tink with support for AWS KMS, HashiCorp Vault, local KMS, and TPM 2.0
  - Features envelope encryption and key rotation capabilities
  - Supports Post-Quantum Hybrid Encryption using ML-KEM-1024 (FIPS 203)
- **OutboxPartStore**: Implements outbox pattern for reliable part operations
- **ErasureCodedPartStoreMiddleware**: Reed-Solomon erasure coding for part storage
  - Supports streaming reads/writes for large parts
  - Can distribute shards across multiple independent part stores

## Configuration Examples

### SQLite (Default)

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

### PostgreSQL

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

### TPM 2.0 Encryption

```json
{
  "type": "TinkEncryptionPartStoreMiddleware",
  "kmsType": "tpm",
  "tpmPath": "/dev/tpmrm0",
  "tpmPersistentHandle": "0x81000001",
  "tpmKeyFilePath": "./data/tpm-key.json",
  "tpmKeyAlgorithm": "ecc-p384",
  "tpmSymmetricAlgorithm": "aes-256",
  "tpmHMACAlgorithm": "sha256",
  "tpmPassword": "",
  "innerPartStore": {
    "type": "FilesystemPartStore",
    "root": "./data/parts"
  }
}
```

> **Note:** `tpmKeyAlgorithm` supports `rsa-2048`, `rsa-4096`, `ecc-p256` (default), `ecc-p384`, `ecc-p521`, and Brainpool curves (`ecc-brainpool-p256`, `p384`, `p512`). `tpmSymmetricAlgorithm` can be `aes-128` or `aes-256` (default). `tpmPassword` is optional; when set, it provides password-based authorization for TPM key access.

### Compression

```json
{
  "type": "CompressionPartStoreMiddleware",
  "sampleSizeBytes": 65536,
  "compressionAlgorithm": "zstd",
    "maxCompressionRatio": 0.95,
  "innerPartStore": {
    "type": "FilesystemPartStore",
    "root": "./data/parts"
  }
}
```

### Post-Quantum Encryption

```json
{
  "type": "TinkEncryptionPartStoreMiddleware",
  "kmsType": "local",
  "password": "your-strong-password",
  "pqSeed": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
  "innerPartStore": {
    "type": "FilesystemPartStore",
    "root": "./data/parts"
  }
}
```

> **Note:** `pqSeed` must be a 64-byte hex-encoded string. Generate one using `openssl rand -hex 64`. **Warning:** If this seed is lost, encrypted data cannot be decrypted.

### Erasure-Coded Part Storage

```json
{
  "type": "ErasureCodedPartStoreMiddleware",
  "dataShards": 4,
  "parityShards": 2,
  "healScanIntervalSeconds": 604800,
  "streamBlockSize": 65536,
  "partStores": [
    { "type": "FilesystemPartStore", "root": "./data/parts-shard-0" },
    { "type": "FilesystemPartStore", "root": "./data/parts-shard-1" },
    { "type": "FilesystemPartStore", "root": "./data/parts-shard-2" },
    { "type": "FilesystemPartStore", "root": "./data/parts-shard-3" },
    { "type": "FilesystemPartStore", "root": "./data/parts-shard-4" },
    { "type": "FilesystemPartStore", "root": "./data/parts-shard-5" }
  ]
}
```

> **Note:** For erasure-coded storage, `partStores` must contain exactly `dataShards + parityShards` entries. Pithos uses strict write quorum in this mode: all shards must be written successfully.

> **Sizing:** Storage overhead factor is `(dataShards + parityShards) / dataShards`. Failure tolerance is `parityShards` shard/backend losses.

> **`streamBlockSize`:** Per-shard stripe size in bytes used during streaming encode/decode. Larger values usually improve throughput but increase peak memory usage. Default is `65536` when omitted.

> **`healScanIntervalSeconds` (optional):** Background full-part heal scan interval in seconds. Default is `604800` (7 days) when omitted. Set to `0` to disable background scanning.

### Erasure-Coded Filesystem (2+1)

```json
{
  "type": "ErasureCodedPartStoreMiddleware",
  "dataShards": 2,
  "parityShards": 1,
  "healScanIntervalSeconds": 86400,
  "streamBlockSize": 65536,
  "partStores": [
    { "type": "FilesystemPartStore", "root": "/mnt/disk-a/pithos/parts" },
    { "type": "FilesystemPartStore", "root": "/mnt/disk-b/pithos/parts" },
    { "type": "FilesystemPartStore", "root": "/mnt/disk-c/pithos/parts" }
  ]
}
```

> **Deployment note:** Place each `root` on a different physical disk or failure domain to get real resilience benefits.

### Multiple Outbox Instances

You can run multiple `OutboxStorage` and `OutboxPartStore` instances against the same database by setting a unique `outboxId` per instance. All outbox SQL operations are scoped to that ID, so each instance only reads and mutates its own rows.

If `outboxId` is omitted, Pithos uses `"default"` for backward compatibility.

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
    "type": "OutboxPartStore",
    "outboxId": "node-a-part-outbox",
    "db": {
      "type": "DatabaseReference",
      "refName": "db"
    },
    "innerPartStore": {
      "type": "SqlPartStore",
      "db": {
        "type": "DatabaseReference",
        "refName": "db"
      }
    }
  }
}
```

```json
{
  "type": "OutboxStorage",
  "outboxId": "node-a-storage-outbox",
  "db": {
    "type": "DatabaseReference",
    "refName": "db"
  },
  "innerStorage": {
    "type": "S3ClientStorage",
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "accessKey": "your-access-key",
    "secretKey": "your-secret-key"
  }
}
```

## Storage Migration

Pithos supports storage migration through the `migrate-storage` subcommand:

```sh
pithos migrate-storage ./storage_source.json ./storage_target.json
```

The migration is performed bucket by bucket. If the target storage bucket is not empty, it will not overwrite existing objects to prevent accidental data loss.
