FROM golang:1.25.0-alpine3.22 AS app-builder

ARG SKIP_TESTS=false

RUN apk add --no-cache build-base

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN if [ "$SKIP_TESTS" = "false" ]; then go test ./... -v; fi

RUN go install -ldflags='-linkmode external -s -w -extldflags "-static-pie"' -buildmode=pie cmd/pithos.go

FROM scratch

WORKDIR /app

COPY --from=app-builder /go/bin/pithos /usr/local/bin/pithos

EXPOSE 9000

ENTRYPOINT ["/usr/local/bin/pithos", "serve"]

