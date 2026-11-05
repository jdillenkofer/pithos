FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.10.0@sha256:40fe21f44bb8cddf1eb1226db190d0a984118f62ee02a91b77d99448d28264a6 AS xx

# The builder runs on the build platform and cross-compiles against the target's
# musl, so no platform is emulated.
FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine3.24@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS app-builder

COPY --from=xx / /

ARG SKIP_TESTS=false
ARG VERSION=devel
ARG COMMIT=unknown
ARG DIRTY=unknown
ARG DATE=unknown

# build-base for the native tests; clang and lld for cross-compiling.
RUN apk add --no-cache build-base clang lld

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

# The tests run natively on the build platform.
# Match the CI registry prefix for implicit Testcontainers images (Ryuk).
RUN if [ "$SKIP_TESTS" = "false" ]; then TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX=ghcr.io go test ./... -v; fi

# Create non-root user (UID 10001)
RUN adduser -D -u 10001 appuser

# Create runtime directories with correct permissions
RUN mkdir -m 1777 /tmp-dir
RUN mkdir -p /data && chown 10001:10001 /data

ARG TARGETPLATFORM
RUN xx-apk add --no-cache gcc musl-dev

# With -trimpath, Go does not record -ldflags in the binary, so the build
# information lists them separately.
RUN ldflags="-linkmode external -s -w -extldflags=-static-pie -X github.com/jdillenkofer/pithos/internal/buildinfo.version=${VERSION} -X github.com/jdillenkofer/pithos/internal/buildinfo.commit=${COMMIT} -X github.com/jdillenkofer/pithos/internal/buildinfo.dirty=${DIRTY} -X github.com/jdillenkofer/pithos/internal/buildinfo.date=${DATE}" && \
    CGO_ENABLED=1 xx-go build -trimpath -ldflags="$ldflags" -buildmode=pie -o /go/bin/pithos ./cmd && \
    xx-verify --static /go/bin/pithos && \
    { \
      printf 'target platform: %s\n' "$TARGETPLATFORM"; \
      printf 'c compiler version: %s\n' "$(xx-clang --version | sed -n 1p)"; \
      printf 'ldflags: %s\n' "$ldflags"; \
      go version -m /go/bin/pithos; \
    } > /go/bin/build-info.txt

# Release binary export: docker buildx build --target binary --output type=local,dest=out .
FROM scratch AS binary
COPY --from=app-builder /go/bin/pithos /go/bin/build-info.txt /

FROM scratch

LABEL org.opencontainers.image.title="Pithos" \
      org.opencontainers.image.description="An S3-compatible object storage server for self-hosters" \
      org.opencontainers.image.documentation="https://github.com/jdillenkofer/pithos/tree/main/docs" \
      org.opencontainers.image.source="https://github.com/jdillenkofer/pithos" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Used when neither -spoolDir nor PITHOS_SPOOL_DIR is set.
ENV TMPDIR=/tmp

# Copy binary and minimal passwd file for user mapping
COPY --from=app-builder --chown=0:0 --chmod=0755 /go/bin/pithos /usr/local/bin/pithos
COPY --from=app-builder /etc/passwd /etc/passwd
COPY --from=app-builder /etc/ssl/certs /etc/ssl/certs
COPY --from=app-builder --chown=10001:10001 /tmp-dir /tmp
COPY --from=app-builder --chown=10001:10001 /data /data

EXPOSE 9000 9090

# Run as non-root user
USER 10001

ENTRYPOINT ["/usr/local/bin/pithos", "serve"]
