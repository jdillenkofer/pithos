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
