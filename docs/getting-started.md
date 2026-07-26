# Getting Started

## Prerequisites

- Go 1.25 or higher
- Docker (optional)

## Installation

Clone the repository:

```sh
git clone https://github.com/jdillenkofer/pithos.git
cd pithos
```

## Build and Run

```sh
go build -o pithos ./cmd/pithos.go
./pithos serve
```

## Running Tests

Run all tests including integration tests:
```sh
go test ./... --integration
```

Run unit tests only (subset of storage backends and features):
```sh
go test ./...
```

## Docker

Build and run Pithos using Docker:

```sh
docker build -t pithos .
docker run -p 9000:9000 -v $(pwd)/data:/data pithos
```

> **PKCS#11 limitation:** The default Docker image is a statically linked
> binary in a `scratch` image and cannot dynamically load PKCS#11 provider
> libraries. Mounting a provider library into the container is not sufficient.
> Run a dynamically linked native build or build a custom image with a runtime
> compatible with the PKCS#11 provider. See
> [PKCS#11 Encryption](storage-backends.md#pkcs11-encryption).

The image uses `/tmp` for temporary spool files by default. For large uploads,
you can mount a dedicated filesystem and select it with `PITHOS_SPOOL_DIR`.
The mounted directory must be writable by the image's UID `10001`:

```sh
docker run -p 9000:9000 \
  -v $(pwd)/data:/data \
  -v /fast-disk/pithos-spool:/spool \
  -e PITHOS_SPOOL_DIR=/spool \
  pithos
```

Alternatively, use a size-limited in-memory temporary filesystem:

```sh
docker run -p 9000:9000 \
  -v $(pwd)/data:/data \
  --tmpfs /tmp:rw,nosuid,nodev,size=20g,uid=10001,gid=10001 \
  pithos
```
