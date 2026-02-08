.PHONY: all build test clean run client fmt vet proto proto-lint

BINARY_NAME=gentis
CLIENT_NAME=client
BUILD_DIR=.
VERSION?=dev
COMMIT?=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

all: test build

build:
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/gentis

build-optimized:
	@CGO_ENABLED=0 go build \
		-a \
		-ldflags="-s -w -extldflags '-static' -X main.version=$(VERSION) -X main.commit=$(COMMIT)" \
		-trimpath \
		-tags netgo \
		-o $(BUILD_DIR)/$(BINARY_NAME) \
		./cmd/gentis
	@ls -lh $(BUILD_DIR)/$(BINARY_NAME)

build-minimal:
	@CGO_ENABLED=0 go build \
		-a \
		-ldflags="-s -w -extldflags '-static' -X main.version=$(VERSION) -X main.commit=$(COMMIT)" \
		-trimpath \
		-tags netgo \
		-o $(BUILD_DIR)/$(BINARY_NAME) \
		./cmd/gentis
	@which upx > /dev/null 2>&1 || (echo "UPX not found. Install with: sudo apt-get install upx-ucl" && exit 1)
	@upx --best --lzma $(BUILD_DIR)/$(BINARY_NAME)
	@ls -lh $(BUILD_DIR)/$(BINARY_NAME)

test:
	@go test -v -race ./...

coverage:
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html

fmt:
	@go fmt ./...

vet:
	@go vet ./...

run: build
	@./$(BINARY_NAME)

.PHONY: docker-build docker-build-alpine docker-run docker-stop

docker-build:
	@docker build -t gentis:latest -t gentis:$(VERSION) .

docker-run:
	@docker run -d --name gentis -p 9000:9000 gentis:latest

lint: fmt vet

proto:
	@buf generate

proto-lint:
	@buf lint

help:
	@echo "Available targets:"
	@echo ""
	@echo "Building:"
	@echo "  make build            - Build the server (standard)"
	@echo "  make build-optimized  - Build optimized server (no UPX)"
	@echo "  make build-minimal    - Build minimal server (with UPX)"
	@echo "  make client           - Build the example client"
	@echo "  make all-build        - Build server and client"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build       - Build Docker image (scratch)"
	@echo "  make docker-build-alpine - Build Alpine Docker image"
	@echo "  make docker-run         - Run Docker container"
	@echo "  make docker-stop        - Stop Docker container"
	@echo ""
	@echo "Testing:"
	@echo "  make test       - Run tests"
	@echo "  make coverage   - Generate test coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt        - Format code"
	@echo "  make vet        - Run go vet"
	@echo "  make lint       - Run fmt and vet"
	@echo ""
	@echo "Proto:"
	@echo "  make proto      - Generate protobuf/gRPC code"
	@echo "  make proto-lint - Lint proto files"
	@echo ""
	@echo "Other:"
	@echo "  make run        - Build and run server"
	@echo "  make clean      - Remove build artifacts"
	@echo "  make deps       - Download dependencies"
