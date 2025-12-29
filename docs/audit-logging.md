# Audit Logging

Pithos provides cryptographically signed audit logs for all storage operations via the `AuditStorageMiddleware`.

## Features

- Multiple output sinks (Binary, JSON, Text)
- Chained hashing for tamper detection
- Dual-signatures for grounding: Ed25519 and ML-DSA (Post-Quantum)
- Automated grounding every 1,000 entries using Merkle Trees for high-integrity checkpoints

## Configuration

### Using Local Keys

```json
{
  "type": "AuditStorageMiddleware",
  "innerStorage": {
     "type": "MetadataPartStorage",
     ...
  },
  "signing": {
    "ed25519": {
      "privateKey": "<Base64-encoded-Ed25519-private-key-or-path>"
    },
    "mlDsa87": {
      "privateKey": "<Base64-encoded-ML-DSA-87-private-key-or-path>"
    }
  },
  "sinks": [
    {
      "type": "FileSink",
      "path": "./data/audit.log",
      "serializer": {
        "type": "BinarySerializer"
      }
    }
  ]
}
```

### Using HashiCorp Vault for Ed25519 Signing

```json
{
  "type": "AuditStorageMiddleware",
  "innerStorage": { ... },
  "signing": {
    "ed25519": {
      "vault": {
        "address": "https://vault.example.com:8200",
        "roleId": "my-app-role-id",
        "secretId": "my-app-role-secret-id",
        "keyPath": "transit/audit-log-key"
      }
    },
    "mlDsa87": {
      "privateKey": "./keys/mldsa_priv.key"
    }
  },
  "sinks": [
    {
      "type": "FileSink",
      "path": "./data/audit.log",
      "serializer": {
        "type": "JsonSerializer",
        "indent": true
      }
    }
  ]
}
```

## Key Generation

Generate key pairs for the audit log:

```sh
pithos audit-log keygen [-f <filename>]
```

- Without `-f`: keys are printed to stdout
- With `-f`: keys are saved to files (e.g., `mykey_ed25519`, `mykey_ed25519.pub`, etc.)

The **private keys** must be kept secret and added to your `storage.json`. The **public keys** are used with the `verify` command.

## Verification

Verify the cryptographic integrity of audit logs:

```sh
pithos audit-log verify -input-file [log-file] -ed25519-public-key <key> -ml-dsa-87-public-key <key> [options]
```

**Options:**
- `-ed25519-public-key`: (Required) Base64 encoded Ed25519 public key
- `-ml-dsa-87-public-key`: (Required) Base64 encoded ML-DSA-87 public key
- `-input-format`: Input format (`bin` default, `json`)
- `-output-format`: Output format for dump (`json` default, `text`, `bin`)
- `-output-file`: Output path (use `-` for stdout)

**Example: Convert binary log to human-readable text**

```sh
pithos audit-log dump -input-file ./data/audit.log -ed25519-public-key nHBi++... -ml-dsa-87-public-key v9A2s... -output-format text -output-file audit_report.txt
```

## Security Recommendation

To prevent overwriting or truncating existing audit logs, use filesystem-level append-only attributes.

### Linux

Requires root privileges or `CAP_LINUX_IMMUTABLE`:

```sh
sudo chattr +a ./data/audit.log
```

To disable: `sudo chattr -a ./data/audit.log`

### macOS

- **User level** (Owner can set/unset): `chflags uappnd ./data/audit.log`
- **System level** (Root only): `sudo chflags sappnd ./data/audit.log`

For maximum security on macOS, use `sappnd`. This prevents the Pithos process from removing the protection if compromised.

To disable: `chflags nouappnd ./data/audit.log` or `sudo chflags nosappnd ./data/audit.log`

With these attributes set, Pithos can still read the file to retrieve the last hash for the chain, but it cannot delete or modify previous entries.
