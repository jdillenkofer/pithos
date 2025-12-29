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

- **TinkEncryptionPartStoreMiddleware**: Advanced encryption using Google Tink with support for AWS KMS, HashiCorp Vault, local KMS, and TPM 2.0
  - Features envelope encryption and key rotation capabilities
  - Supports Post-Quantum Hybrid Encryption using ML-KEM-1024 (FIPS 203)
- **OutboxPartStore**: Implements outbox pattern for reliable part operations

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
  "innerPartStore": {
    "type": "FilesystemPartStore",
    "root": "./data/parts"
  }
}
```

> **Note:** `tpmKeyAlgorithm` supports `rsa-2048`, `rsa-4096`, `ecc-p256` (default), `ecc-p384`, `ecc-p521`, and Brainpool curves (`ecc-brainpool-p256`, `p384`, `p512`). `tpmSymmetricAlgorithm` can be `aes-128` or `aes-256` (default).

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

## Storage Migration

Pithos supports storage migration through the `migrate-storage` subcommand:

```sh
pithos migrate-storage ./storage_source.json ./storage_target.json
```

The migration is performed bucket by bucket. If the target storage bucket is not empty, it will not overwrite existing objects to prevent accidental data loss.
