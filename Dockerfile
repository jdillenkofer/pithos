FROM golang:1.23.4-alpine3.21 AS app-builder

RUN apk add build-base

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN go test ./... -v -timeout 30m

RUN go install cmd/pithos.go

FROM alpine:3.21.0

WORKDIR /app

COPY --from=app-builder /go/bin/pithos ./pithos

EXPOSE 9000

ENTRYPOINT ["./pithos", "serve"]

