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

Release images for `linux/amd64` and `linux/arm64` are published to GitHub
Container Registry and Docker Hub. Pull and run the GHCR image with:

```sh
docker pull ghcr.io/jdillenkofer/pithos:latest
docker run -p 9000:9000 -v "$(pwd)/data:/data" ghcr.io/jdillenkofer/pithos:latest
```

The equivalent Docker Hub image is `jdillenkofer/pithos:latest`. To build the
image locally instead, run `docker build -t pithos .` and substitute `pithos`
for the published image name in the examples below.

The image uses `/tmp` for temporary spool files by default. For large uploads,
you can mount a dedicated filesystem and select it with `PITHOS_SPOOL_DIR`.
The mounted directory must be writable by the image's UID `10001`:

```sh
docker run -p 9000:9000 \
  -v "$(pwd)/data:/data" \
  -v /fast-disk/pithos-spool:/spool \
  -e PITHOS_SPOOL_DIR=/spool \
  ghcr.io/jdillenkofer/pithos:latest
```

Alternatively, use a size-limited in-memory temporary filesystem:

```sh
docker run -p 9000:9000 \
  -v "$(pwd)/data:/data" \
  --tmpfs /tmp:rw,nosuid,nodev,size=20g,uid=10001,gid=10001 \
  ghcr.io/jdillenkofer/pithos:latest
```
