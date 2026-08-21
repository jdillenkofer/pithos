FROM golang:1.26.6-alpine3.24@sha256:3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83 AS app-builder

ARG SKIP_TESTS=false
ARG VERSION=devel
ARG COMMIT=unknown
ARG DIRTY=unknown

RUN apk add --no-cache build-base

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN if [ "$SKIP_TESTS" = "false" ]; then go test ./... -v; fi

# Create non-root user (UID 10001)
RUN adduser -D -u 10001 appuser

# Create runtime directories with correct permissions
RUN mkdir -m 1777 /tmp-dir
RUN mkdir -p /data && chown 10001:10001 /data

RUN go build -ldflags="-linkmode external -s -w -extldflags=-static-pie -X github.com/jdillenkofer/pithos/internal/buildinfo.version=${VERSION} -X github.com/jdillenkofer/pithos/internal/buildinfo.commit=${COMMIT} -X github.com/jdillenkofer/pithos/internal/buildinfo.dirty=${DIRTY}" -buildmode=pie -o /go/bin/pithos ./cmd

# Change ownership of the binary to appuser
RUN chown 10001:10001 /go/bin/pithos

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
COPY --from=app-builder /go/bin/pithos /usr/local/bin/pithos
COPY --from=app-builder /etc/passwd /etc/passwd
COPY --from=app-builder /etc/ssl/certs /etc/ssl/certs
COPY --from=app-builder --chown=10001:10001 /tmp-dir /tmp
COPY --from=app-builder --chown=10001:10001 /data /data

EXPOSE 9000

# Run as non-root user
USER 10001

ENTRYPOINT ["/usr/local/bin/pithos", "serve"]
