FROM golang:1.23.2-alpine3.20 AS app-builder

RUN apk add build-base

WORKDIR /go/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN go test ./... -v

RUN go install cmd/pithos.go

FROM alpine:3.20.3

WORKDIR /app

COPY --from=app-builder /go/bin/pithos ./pithos

EXPOSE 9000

ENTRYPOINT ["./pithos"]

