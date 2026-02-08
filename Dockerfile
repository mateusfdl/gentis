FROM golang:alpine AS builder

RUN apk add --no-cache git make upx

WORKDIR /build

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -a \
    -installsuffix cgo \
    -ldflags="-s -w -extldflags '-static'" \
    -trimpath \
    -tags netgo \
    -o gentis \
    ./cmd/gentis

RUN upx --best --lzma gentis

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/gentis /gentis

EXPOSE 9000

ENTRYPOINT ["/gentis"]
CMD ["-addr", "0.0.0.0:9000"]
