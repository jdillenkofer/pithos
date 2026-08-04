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
go build -o pithos ./cmd
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
