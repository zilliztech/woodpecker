BIN_DIR := $(PWD)/bin
PROTOC_GEN_GO_VERSION := v1.36.2
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
PROTOC_GEN_GO_VTPROTO_VERSION := v0.6.0

# Default target - show help
.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help message
	@echo "🚀 Woodpecker Build System"
	@echo "=========================="
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "💡 For more advanced builds, use:"
	@echo "   ./build/build_bin.sh --help     # Binary builds"
	@echo "   ./build/build_image.sh --help   # Docker builds"
	@echo "   ./deployments/deploy.sh help    # Cluster management"

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

# Install protoc-gen-go
$(BIN_DIR)/protoc-gen-go: | $(BIN_DIR)
	@echo "Installing protoc-gen-go..."
	@GOBIN=$(BIN_DIR) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

# Install protoc-gen-go-grpc
$(BIN_DIR)/protoc-gen-go-grpc: | $(BIN_DIR)
	@echo "Installing protoc-gen-go-grpc..."
	@GOBIN=$(BIN_DIR) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

# Install protoc-gen-go-vtproto
$(BIN_DIR)/protoc-gen-go-vtproto: | $(BIN_DIR)
	@echo "Installing protoc-gen-go-vtproto..."
	@GOBIN=$(BIN_DIR) go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@$(PROTOC_GEN_GO_VTPROTO_VERSION)

# Install all tools
.PHONY: install
install: $(PROTOC_DIR) $(BIN_DIR)/protoc-gen-go $(BIN_DIR)/protoc-gen-go-grpc $(BIN_DIR)/protoc-gen-go-vtproto ## Install all protobuf tools
	@echo "All tools installed in $(BIN_DIR)."

.PHONY: proto
proto: ## Generate protobuf code
	cd proto && \
	protoc \
		--go_out=. \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${BIN_DIR}/protoc-gen-go" \
		--go-grpc_out=. \
		--go-grpc_opt require_unimplemented_servers=false,paths=source_relative \
		--plugin protoc-gen-go-grpc="${BIN_DIR}/protoc-gen-go-grpc" \
		--go-vtproto_out=. \
		--go-vtproto_opt paths=source_relative \
		--plugin protoc-gen-go-vtproto="${BIN_DIR}/protoc-gen-go-vtproto" \
		--go-vtproto_opt=features=marshal+unmarshal+unmarshal_unsafe+size+pool+equal+clone \
		*.proto

proto_clean:
	rm -f */*.pb.go

.PHONY: build
build:
	@bash -c 'source $(PWD)/scripts/setenv.sh && go build -v -o bin/woodpecker ./cmd'

.PHONY: test
test:
	@bash -c 'source $(PWD)/scripts/setenv.sh && go test -cover -race ./...'

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run --config .golangci.yml ./...

.PHONY: lint-fix
lint-fix: ## Run golangci-lint with auto-fix
	golangci-lint run --config .golangci.yml --fix ./...

.PHONY: fmt
fmt: ## Format code with gofumpt and gci
	gofumpt -l -w .
	gci write . --section standard --section default --section "prefix(github.com/zilliztech/woodpecker)" --skip-generated

.PHONY: fmt-check
fmt-check: ## Check code formatting (CI)
	@echo "Checking gofumpt..."
	@test -z "$$(gofumpt -l .)" || (echo "Files need formatting:"; gofumpt -l .; exit 1)
	@echo "Checking gci..."
	@gci diff . --section standard --section default --section "prefix(github.com/zilliztech/woodpecker)" --skip-generated | tee /dev/stderr | (! grep .)
	@echo "All files formatted correctly."

.PHONY: unit-test
unit-test: ## Run unit tests only (excludes integration/benchmark/stability/docker)
	go test -race -short -cover -coverprofile=coverage.out -covermode=atomic -failfast -timeout=20m \
		$$(go list ./... | grep -v -E '(tests/integration|tests/benchmark|tests/stability|tests/docker)')

.PHONY: integration-test
integration-test: ## Run all integration tests (requires etcd + MinIO)
	go test -race -cover -failfast -timeout=45m -v ./tests/integration/...

.PHONY: integration-test-e2e-local
integration-test-e2e-local: ## Run e2e tests with local filesystem storage
	go test -race -cover -failfast -timeout=20m -v \
		-run "/LocalFsStorage" \
		./tests/integration/...

.PHONY: integration-test-e2e-objectstorage
integration-test-e2e-objectstorage: ## Run e2e tests with object storage (MinIO)
	go test -race -cover -failfast -timeout=20m -v \
		-run "/ObjectStorage" \
		./tests/integration/...

.PHONY: integration-test-e2e-service
integration-test-e2e-service: ## Run e2e tests with service storage + failover
	go test -race -cover -failfast -timeout=25m -v \
		-run "/ServiceStorage" \
		./tests/integration/...
	go test -race -cover -failfast -timeout=25m -v \
		-run "^TestStagedStorageService" \
		./tests/integration/...

.PHONY: integration-test-components
integration-test-components: ## Run component integration tests (non-e2e)
	go test -race -cover -failfast -timeout=20m -v \
		-skip "^(TestOpenWriter|TestOpenInternal|TestRepeated|TestWriterClose|TestClientRecreation|TestMultiClient|TestConcurrentWriteAnd|TestConcurrentReader|TestReadTheWritten|TestReadWriteLoop|TestMultiAppendSync|TestTailRead|TestConcurrentWriteWith|TestTruncate|TestWriteAndTruncate|TestMultiSegment|TestReadBefore|TestSegmentCleanup|TestStagedStorageService)" \
		./tests/integration/...

clean: ## Clean built binaries
	rm -f $(BIN_DIR)/*
	rm -rf $(PWD)/cmake_build
	rm -rf $(PWD)/external/output

# Docker targets (using new build system)
docker: ## Build Docker image (auto-detect architecture)
	./build/build_image.sh ubuntu22.04 auto -t woodpecker:latest

# Multi-architecture Docker build
docker-multiarch: ## Build multi-architecture Docker image
	./build/build_image.sh ubuntu22.04 multiarch -t woodpecker:latest

# Build binary for Linux (basic build commands)
build-linux: ## Build Linux binary (AMD64)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -o bin/woodpecker ./cmd

# Build binary for Linux ARM64
build-linux-arm64: ## Build Linux binary (ARM64)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v -o bin/woodpecker ./cmd

# Auto-detect architecture build (using new build system)
build-auto: ## Build binary (auto-detect architecture)
	./build/build_bin.sh auto

# Cluster management (delegated to deploy.sh for consistency)
cluster-up: ## Start the complete cluster
	cd deployments && ./deploy.sh up

cluster-down: ## Stop the cluster
	cd deployments && ./deploy.sh down

cluster-clean: ## Stop cluster and remove volumes
	cd deployments && ./deploy.sh clean

cluster-logs: ## View cluster logs
	cd deployments && ./deploy.sh logs

cluster-status: ## Check cluster status
	cd deployments && ./deploy.sh status

cluster-test: ## Test cluster connectivity
	cd deployments && ./deploy.sh test

# ==================== CPP BUILD ========
INSTALL_PATH := $(PWD)/bin
LIBRARY_PATH := $(PWD)/lib
OS := $(shell uname -s)
mode = Release
use_asan = OFF
ifeq ($(USE_ASAN), ON)
	use_asan = ${USE_ASAN}
	CGO_LDFLAGS := $(shell go env CGO_LDFLAGS) -fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address
	CGO_CFLAGS := $(shell go env CGO_CFLAGS) -fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address
	MILVUS_GO_BUILD_TAGS := $(MILVUS_GO_BUILD_TAGS),use_asan
endif

use_opendal = OFF
ifdef USE_OPENDAL
	use_opendal = ${USE_OPENDAL}
endif

# default git branch is dev
export GIT_BRANCH=dev

ifeq (${ENABLE_AZURE}, false)
	AZURE_OPTION := -Z
endif


build-3rdparty:
	@echo "Build thirdparty using conanfile.py ..."
	@(env bash $(PWD)/scripts/3rdparty_build.sh -o ${use_opendal} -t ${mode})

build-external: build-3rdparty
	@echo "Building Milvus external cpp library ..."
	@(env bash $(PWD)/scripts/external_build.sh -t ${mode} -a ${use_asan} ${AZURE_OPTION} -o ${use_opendal} )
