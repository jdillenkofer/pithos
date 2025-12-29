# CLI Reference

Pithos provides several subcommands for managing and maintaining the storage server.

## `serve`

Starts the S3-compatible object storage server.

```sh
pithos serve [options]
```

For a complete list of available command-line arguments, run `./pithos serve --help`.

## `migrate-storage`

Migrates data between two different storage configurations.

```sh
pithos migrate-storage <source-config.json> <destination-config.json>
```

The migration is performed bucket by bucket. To prevent data loss, it will not overwrite existing objects in the destination.

## `benchmark-storage`

Measures the performance of a storage configuration.

```sh
pithos benchmark-storage <config.json>
```

This command performs upload and download benchmarks with various object sizes and reports the speeds.

## `validate-storage`

Checks the integrity of all objects in the storage.

```sh
pithos validate-storage <config.json> [options]
```

**Options:**
- `-delete-corrupted`: Delete objects that fail integrity checks
- `-force`: Force deletion without confirmation
- `-json`: Output results in JSON format
- `-output <path>`: Write the validation report to a file

## `audit-log`

Provides tools for verifying, dumping, analyzing, and generating keys for audit logs.

```sh
pithos audit-log <verify|dump|stats|keygen> [options]
```

**Common options:**
- `-input-file <path>`: (Required for verify|dump|stats) Path to the audit log file
- `-input-format <bin|json|text>`: Input format (default: `bin`)
- `-ed25519-public-key <key>`: (Required for verify|dump|stats) Base64 encoded Ed25519 public key or path to key file
- `-ml-dsa-87-public-key <key>`: (Required for verify|dump|stats) Base64 encoded ML-DSA-87 public key or path to key file

**Subcommand specific options:**
- `dump`: Supports `-output-format <json|text|bin>` and `-output-file <path>`

### Generate Key Pairs

```sh
pithos audit-log keygen [-f <filename>]
```

- If no `-f` flag is provided, keys are printed to stdout
- If `-f` is provided, keys are saved to files (e.g., `mykey_ed25519`, `mykey_ed25519.pub`, etc.)
- Pithos will prompt for confirmation before overwriting existing files

The **private keys** must be kept secret and added to your `storage.json` configuration to enable signing. The **public keys** are used with the `verify` command.

### Verify an Audit Log

```sh
pithos audit-log verify -input-file ./data/audit.log -ed25519-public-key nHBi++... -ml-dsa-87-public-key v9A2s...
```

## `tpm-info`

Queries TPM (Trusted Platform Module) hardware capabilities to detect supported features.

```sh
pithos tpm-info [-tpm-path <path>]
```

**Options:**
- `-tpm-path`: Path to the TPM device (default: `/dev/tpmrm0`)

This command performs trial key creations to detect:
- Supported asymmetric algorithms (RSA, ECC) for `tpmKeyAlgorithm`
- Supported HMAC algorithms for `tpmHMACAlgorithm`
- Supported symmetric algorithms (AES) for `tpmSymmetricAlgorithm`

Useful for verifying TPM compatibility before configuring the `TinkEncryptionPartStoreMiddleware` with TPM support.
