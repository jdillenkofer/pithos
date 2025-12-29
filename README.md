# Pithos

Pithos is an S3-compatible object storage server designed for self-hosters, written in Go.

## Features

- S3-compatible API with extensive operation support:
  - Bucket Operations: Create, Head, Delete, List buckets
  - Object Operations: Head, Get, Put, Delete, List objects
  - Multipart Upload Operations: Initiate, Upload, Complete, Abort, List
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
- [Storage Backends](docs/storage-backends.md) – Storage types and configuration examples
- [Audit Logging](docs/audit-logging.md) – Audit middleware and verification
- [Verifying Releases](docs/verifying-releases.md) – Cosign verification

## License

Pithos is licensed under the [MIT License](LICENSE).