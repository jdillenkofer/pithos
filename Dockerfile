FROM golang:1.25.7-alpine3.22 AS app-builder

ARG SKIP_TESTS=false

RUN apk add --no-cache build-base

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN if [ "$SKIP_TESTS" = "false" ]; then go test ./... -v; fi

# Create non-root user (UID 10001)
RUN adduser -D -u 10001 appuser

# Create a temporary directory with correct permissions
RUN mkdir -m 1777 /tmp-dir

RUN go install -ldflags='-linkmode external -s -w -extldflags "-static-pie"' -buildmode=pie cmd/pithos.go

# Change ownership of the binary to appuser
RUN chown 10001:10001 /go/bin/pithos

FROM scratch

WORKDIR /app

# Copy binary and minimal passwd file for user mapping
COPY --from=app-builder /go/bin/pithos /usr/local/bin/pithos
COPY --from=app-builder /etc/passwd /etc/passwd
COPY --from=app-builder /etc/ssl/certs /etc/ssl/certs
COPY --from=app-builder --chown=10001:10001 /tmp-dir /tmp

EXPOSE 9000

# Run as non-root user
USER 10001

ENTRYPOINT ["/usr/local/bin/pithos", "serve"]

