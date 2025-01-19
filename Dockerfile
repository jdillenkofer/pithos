FROM golang:1.23.5-alpine3.21 AS app-builder

RUN apk add --no-cache build-base

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN go test ./... -v -timeout 30m

RUN go install -ldflags='-s -w -extldflags "-static"' cmd/pithos.go

FROM scratch

WORKDIR /app

COPY --from=app-builder /go/bin/pithos /usr/local/bin/pithos

EXPOSE 9000

ENTRYPOINT ["/usr/local/bin/pithos", "serve"]

