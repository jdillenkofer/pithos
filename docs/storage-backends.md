# Storage Backends

Pithos supports multiple storage backends that can be configured in the storage configuration file. The filepath can be set using the `PITHOS_STORAGE_JSON_PATH` environment variable (default: `storage.json`).

## Storage Types

### Primary Storage Backends

- **MetadataPartStorage**: Separates metadata and part storage
  - Supports various metadata stores (SQL databases: SQLite, PostgreSQL)
  - Configurable part stores (filesystem, SFTP, Google Drive)
  - Persists object metadata, object tags, bucket CORS/lifecycle/website configuration, and bucket versioning state in the metadata store
  - Optional named extra part stores with a storage-class mapping, so objects of different classes live in different part stores (see [Storage Class Tiering](#storage-class-tiering-named-part-stores))
  - Emits [bucket event notifications](#bucket-event-notifications) by default, atomically with object mutations
- **S3ClientStorage**: Use an existing S3-compatible storage as backend
  - Compatible with other S3-compatible services
  - Configurable endpoint, region, and credentials
  - Forwards object metadata, bucket versioning, object version listing, and version-aware object deletes to the upstream S3 service

### Enhancement Layers

- **ReplicationStorage**: Enables replication across multiple storage backends
  - Supports primary-replica configuration

### Storage Middleware

- **ConditionalStorage**: Conditional forwarding to different storage backends based on bucket name
- **PrometheusStorage**: Adds Prometheus metrics for storage operations
- **AuditStorage**: Provides cryptographically signed audit logs (see [Audit Logging](audit-logging.md))
- **ObjectCacheStorageMiddleware**: Adds read-through object caching for object storage backends (especially S3)
  - Caches `GetObject` full-object reads and `HeadObject` metadata
  - Invalidates cache entries on successful object mutation operations (`PutObject`, `CopyObject`, `AppendObject`, `DeleteObject`, `DeleteObjects`, `CompleteMultipartUpload`)
  - Bypasses cache for ranged `GetObject` requests and explicit `versionId` reads

### Part Store Middleware

- **CompressionPartStoreMiddleware**: Compresses parts based on sample compression ratio checks
  - Supported `compressionAlgorithm` values: `gzip`, `zstd`
  - Defaults: `sampleSizeBytes=65536`, `compressionAlgorithm="zstd"`, `maxCompressionRatio=0.95`
- **CachePartStore**: Adds caching capabilities to part storage
  - Configurable cache policies (LFU, etc.)
  - Support for both in-memory and persistent caching
  - Skips caching oversized parts via `maxPartSizeBytes` hinting
- **TinkEncryptionPartStoreMiddleware**: Advanced encryption using Google Tink with support for AWS KMS, HashiCorp Vault, local KMS, and TPM 2.0
  - Features envelope encryption and key rotation capabilities
  - Supports Post-Quantum Hybrid Encryption using ML-KEM-1024 (FIPS 203)
  - Uses seekable encrypted part reads for efficient ranged downloads when the inner part store supports seeking
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

`SqlPartStore` accepts an optional `partStoreId` field. Leave it unset for the
default single-store case. When multiple `SqlPartStore` instances share the
same database, give each one a distinct `partStoreId` so part reads,
enumeration, deletion, and garbage collection are scoped to that logical store.

### Storage Class Tiering (Named Part Stores)

`MetadataPartStorage` can route object data to different part stores based on
the object's storage class (`x-amz-storage-class`). The `partStore` field
remains the **default** store (reserved name `default`); `extraPartStores`
defines additional stores by name and `storageClassToPartStore` maps storage
classes onto store names. Classes without a mapping use the default store, so
existing configurations keep working unchanged.

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
    "type": "FilesystemPartStore",
    "root": "./data/parts"
  },
  "extraPartStores": {
    "cold": {
      "type": "FilesystemPartStore",
      "root": "./data/cold-parts"
    }
  },
  "storageClassToPartStore": {
    "GLACIER": "cold",
    "DEEP_ARCHIVE": "cold"
  }
}
```

Notes:

- Each named store accepts the full part store configuration tree, so a class
  target can itself be encrypted, compressed, cached, or erasure-coded.
- Every part records the name of the store it was written to, so remapping a
  class later never breaks reads of existing objects; new writes simply go to
  the new target. Do not remove or rename a store that still holds parts.
- Garbage collection sweeps all configured stores.
- Streaming downloads release their database connection only when **every**
  configured store supports transaction-free reads (everything except
  `SqlPartStore`).

### Bucket Event Notifications

`MetadataPartStorage` supports S3-style bucket event notifications. Per-bucket
rules (which events, which prefixes/suffixes, and which destination ARNs) are
set through the standard S3 API — for example
`aws s3api put-bucket-notification-configuration` — and are stored in the
metadata database. When an object mutation matches a rule, an entry is written
to a durable outbox and delivered asynchronously by a background dispatcher with
at-least-once semantics.

Notifications are **enabled by default**: because the notification layer shares
the `MetadataPartStorage` database, the outbox insert commits in the same
transaction as the object mutation, so an event is never lost or emitted for a
mutation that rolled back. No extra configuration is required to turn them on.

To customize delivery or tune the dispatcher, add an optional `notifications`
block to the `MetadataPartStorage` configuration:

```json
{
  "type": "MetadataPartStorage",
  "db": {
    "type": "RegisterDatabaseReference",
    "refName": "db",
    "db": { "type": "SqliteDatabase", "dbPath": "./data/pithos.db" }
  },
  "metadataStore": {
    "type": "SqlMetadataStore",
    "db": { "type": "DatabaseReference", "refName": "db" }
  },
  "partStore": {
    "type": "SqlPartStore",
    "db": { "type": "DatabaseReference", "refName": "db" }
  },
  "notifications": {
    "notificationDestinations": {
      "arn:aws:sqs:eu-central-1:000000000000:image-jobs": {
        "type": "aws",
        "aws": {
          "region": "eu-central-1",
          "queueUrl": "https://sqs.eu-central-1.amazonaws.com/000000000000/image-jobs"
        }
      },
      "arn:aws:sns:eu-central-1:000000000000:events": {
        "type": "webhook",
        "url": "https://example.com/hooks/s3",
        "payloadFormat": "s3-records"
      }
    },
    "maxAttempts": 8,
    "minBackoffSeconds": 1,
    "maxBackoffSeconds": 300,
    "dispatcherConcurrency": 4,
    "batchSize": 16
  }
}
```

#### `notifications` fields

All fields are optional; omit the whole block to run with defaults.

- `enabled` (default `true`): set to `false` to store bucket notification configurations without emitting or delivering any events.
- `notificationDestinations`: maps a destination ARN (or `eventbridge:<bucket>` for EventBridge) to a custom delivery target. ARNs that are **not** listed here fall back to real AWS delivery (SNS/SQS/Lambda by ARN service; EventBridge default event bus for EventBridge-enabled buckets).
- `outboxId` (default `"default"`): scopes outbox rows; set a unique value per instance when multiple dispatchers share one database.
- `claimLeaseDurationSeconds` (default `30`): how long a claimed entry is leased to a dispatcher before it can be re-claimed.
- `maxAttempts` (default unlimited): after this many failed delivery attempts an entry is dead-lettered and no longer retried. `0` (or omitted) means retry forever.
- `minBackoffSeconds` / `maxBackoffSeconds` (defaults `1` / `300`): bounds of the exponential retry backoff.
- `dispatcherConcurrency` (default `1`): number of entries delivered in parallel per batch.
- `batchSize` (default `1`): maximum number of entries claimed before dispatching.

#### Destination types

Each entry in `notificationDestinations` has a `type`:

- `aws`: deliver to AWS SNS, SQS, or Lambda (chosen by the ARN service) or to EventBridge. The optional `aws` object overrides ambient AWS configuration with `region`, `endpoint`, `accessKeyId`, `secretAccessKey`, `sessionToken`, and `queueUrl` (used directly for SQS instead of resolving the queue URL from the ARN).
- `webhook`: HTTP `POST` (or `method`) to `url`, with optional `headers` and `tls`.
- `rabbitmq`: publish to a `queue` or `exchange` on `url` (with `routingKey`, `exchangeType`, `durable`, `auth`, `tls`).
- `kafka`: publish to `topic` on `brokers`, with an optional `keyTemplate`, `auth` (`plain`, `sasl-scram-sha-256`, `sasl-scram-sha-512`), and `tls`.

Every destination accepts `payloadFormat`: `s3-records` (default, the AWS S3 event records envelope) or `eventbridge` (the AWS EventBridge envelope).

#### Destination validation and test events

When a bucket notification configuration is applied, Pithos validates the
configured destinations and publishes one synchronous `s3:TestEvent` to each
unique destination. If any test delivery fails, the request fails and the
previous configuration is left unchanged. Send the request with the
`x-amz-skip-destination-validation: true` header to skip both validation and the
test events. Test events are delivered synchronously and are never written to the
durable outbox.

#### Metrics

The dispatcher exposes Prometheus metrics under the `pithos_notification_`
prefix: `pending_entries` and `dead_lettered_entries` gauges, and
`claimed_entries_total`, `published_entries_total`, `failed_publishes_total`,
`retry_attempts_total`, and `publish_latency_seconds`, labeled by `outbox_id`,
`destination_type`, and `payload_format`.

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

### Object Cache Middleware

```json
{
  "type": "ObjectCacheStorageMiddleware",
  "maxObjectSizeBytes": 67108864,
  "cacheReadErrorsAsMiss": true,
  "cache": {
    "type": "GenericCache",
    "cachePersistor": {
      "type": "InMemoryPersistor"
    },
    "cacheEvictionPolicy": {
      "type": "LfuEvictionPolicy",
      "evictionCheckers": [
        {
          "type": "FixedSizeLimit",
          "maxSizeLimit": 2147483648
        }
      ]
    }
  },
  "innerStorage": {
    "type": "S3ClientStorage",
    "baseEndpoint": "https://s3.amazonaws.com",
    "region": "us-east-1",
    "accessKeyId": "${AWS_ACCESS_KEY_ID}",
    "secretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "usePathStyle": false
  }
}
```

> **Note:** This middleware currently caches only full-object reads of the latest object version. Ranged reads and explicit `versionId` reads are always fetched from the inner storage. Concurrent cache misses for the same object are coalesced to avoid duplicate backend reads.

### Versioning and Metadata Upgrades

SQL-backed `MetadataPartStorage` runs embedded SQLite/PostgreSQL migrations when the database starts. The object metadata migration adds columns for user-controllable system metadata and an `object_user_metadata` table for `x-amz-meta-*` values. The versioning migration adds bucket versioning state and object version columns; existing completed objects are marked as the latest `null` version so they remain accessible after upgrading.

Filesystem, SFTP, and Google Drive part stores can serve object bytes without holding a database transaction open for the entire download. This is also used through compatible part-store middlewares, including compression, cache, outbox, erasure coding, and Tink encryption. SQL part stores keep the read transaction open to preserve snapshot semantics for database-backed part content.

### Cache Part Store

```json
{
  "type": "CachePartStore",
  "maxPartSizeBytes": 67108864,
  "cacheReadErrorsAsMiss": true,
  "cache": {
    "type": "GenericCache",
    "cachePersistor": {
      "type": "InMemoryPersistor"
    },
    "cacheEvictionPolicy": {
      "type": "EvictNothingEvictionPolicy"
    }
  },
  "innerPartStore": {
    "type": "FilesystemPartStore",
    "root": "./data/parts"
  }
}
```

> **Note:** Parts larger than `maxPartSizeBytes` are read/written through the inner store and marked as oversized to avoid repeated cache write attempts.

### Google Drive Part Store

Stores parts as files in a dedicated folder of a personal Google Drive. Pithos authenticates as a regular Google account via OAuth; access tokens are refreshed automatically from the stored refresh token for the lifetime of the process, so no re-authentication is needed after the one-time setup.

```json
{
  "type": "GoogleDrivePartStore",
  "clientId": "1234567890-abcdef.apps.googleusercontent.com",
  "clientSecret": { "type": "EnvKey", "envKey": "PITHOS_GDRIVE_CLIENT_SECRET" },
  "token": { "type": "EnvKey", "envKey": "PITHOS_GDRIVE_TOKEN" },
  "folderName": "pithos-parts"
}
```

| Field | Description |
| --- | --- |
| `clientId` | OAuth client id of your own Google Cloud OAuth client (see setup below). |
| `clientSecret` | OAuth client secret. |
| `token` | The OAuth token JSON printed by `pithos gdrive-auth`. It must contain a `refresh_token`. |
| `folderName` | Optional Drive folder for the part files (default `pithos-parts`). The folder is created by pithos on first start. |

#### Setup

1. **Create a Google Cloud project** at <https://console.cloud.google.com/> (any personal Google account works) and enable the **Google Drive API** under *APIs & Services → Library*.
2. **Configure the OAuth consent screen** (*APIs & Services → OAuth consent screen*): choose *External*, fill in the required fields, and **publish the app** (*Publishing status → In production*). While the app is in *Testing* status, Google expires refresh tokens after 7 days, which would break unattended operation. Publishing an unverified app is fine for personal use; verification is not required.
3. **Create an OAuth client** (*APIs & Services → Credentials → Create credentials → OAuth client ID*) of type **TVs and Limited Input devices**. This client type enables the device flow that `pithos gdrive-auth` uses. Note the client id and client secret.
4. **Authorize pithos** (one-time, works on headless servers):

   ```sh
   pithos gdrive-auth -client-id <CLIENT_ID> -client-secret <CLIENT_SECRET>
   ```

   The command prints a `https://www.google.com/device` URL and a short code. Open the URL in a browser on any device (phone, laptop, …), enter the code, and approve the access. The command then prints the token JSON to stdout.
5. **Store the token** in the `token` field of the configuration, or in an environment variable referenced via `{"type": "EnvKey", "envKey": "..."}` as in the example above, and start pithos.

#### Notes

- Pithos requests only the `drive.file` OAuth scope: it can access just the files and folders it created itself, not the rest of the Drive. This also means the part folder cannot be a pre-existing folder created in the Drive UI — pithos creates (or re-finds) it by name on start.
- Access token refresh is automatic. The refresh token in the config stays valid until revoked in the Google account's security settings (or after ~6 months of complete inactivity).
- Uploads cost a single Drive API call per part: the file is created under its final name during the write and simply deleted again if the transaction rolls back. Parts of uncommitted or crashed transactions are invisible to readers (reads go through committed metadata only) and are removed by the part garbage collector after its grace window. Deletes run after the commit with bounded parallelism (8 concurrent calls).
- Because Drive charges at least one API round trip per part, the S3 client's multipart chunk size directly controls throughput: prefer large chunks (e.g. `aws configure set s3.multipart_chunksize 64MB` or rclone's `--s3-chunk-size 64M`). The `OutboxPartStore` wrapper additionally moves all Drive calls off the request path.
- Ranged object reads only download the parts overlapping the range, and the part readers are seekable: a range starting in the middle of a part is served with an HTTP `Range` request against Drive instead of downloading and discarding the part's head.
- The Drive API has per-user request quotas and noticeably higher latency than object stores. For frequently read data, combine it with the [Cache Part Store](#cache-part-store) or use it as a cold tier via [Storage Class Tiering](#storage-class-tiering-named-part-stores).

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
