# Pithos

> [!WARNING]
> **Beta Software**: Pithos follows semantic versioning and is currently below v1.0.0. Breaking changes to the API, configuration format, or storage layout may occur between minor versions. Please review the changelog before upgrading. Pithos is not recommended for production use with important data.

Pithos is an S3-compatible object storage server designed for self-hosters, written in Go.

## Features

- S3-compatible API with extensive operation support:
  - Bucket Operations: Create, Head, Delete, List buckets, CORS, lifecycle, website, and versioning
  - Object Operations: Head, Get, Put, Copy, Append, Delete, multi-delete, List objects, List object versions
  - Multipart Upload Operations: Initiate, Upload, Upload Part Copy, Complete, Abort, List
- Object metadata, object tagging, delete markers, and S3-style bucket versioning
- Authentication using AWS Signature Version 4
- Authorization support via Lua scripts
- Configurable storage backends (local filesystem, S3, etc.)
- Prometheus metrics endpoint for monitoring
- Health monitoring endpoints
- Docker support for easy deployment

## Quick Start

```sh
git clone https://github.com/jdillenkofer/pithos.git
cd pithos
go build -o pithos ./cmd/pithos.go
./pithos serve
```

Or with Docker:

```sh
docker build -t pithos .
docker run -p 9000:9000 -v $(pwd)/data:/data pithos
```

## Documentation

For detailed documentation, see the [docs](docs/) directory:

- [Getting Started](docs/getting-started.md) – Installation, build, and Docker setup
- [CLI Reference](docs/cli-reference.md) – All commands and options
- [Configuration](docs/configuration.md) – Environment variables and authorization
- [Wasm Authorizer Examples](docs/wasm-authorizer-examples.md) – Rust and Go authorizer policies
- [S3 API Behavior](docs/s3-api.md) – Object metadata, versioning, and SDK-facing semantics
- [Storage Backends](docs/storage-backends.md) – Storage types and configuration examples
- [Audit Logging](docs/audit-logging.md) – Audit middleware and verification
- [Verifying Releases](docs/verifying-releases.md) – Cosign verification

## License

Pithos is licensed under the [MIT License](LICENSE).
