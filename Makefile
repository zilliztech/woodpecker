BIN_DIR := $(PWD)/bin
PROTOC_GEN_GO_VERSION := v1.36.2
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
PROTOC_GEN_GO_VTPROTO_VERSION := v0.6.0

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
install: $(PROTOC_DIR) $(BIN_DIR)/protoc-gen-go $(BIN_DIR)/protoc-gen-go-grpc $(BIN_DIR)/protoc-gen-go-vtproto
	@echo "All tools installed in $(BIN_DIR)."

.PHONY: proto
proto:
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

clean:
	rm -f $(BIN_DIR)/*
	rm -rf $(PWD)/cmake_build
	rm -rf $(PWD)/external/output

docker:
	docker build -t woodpecker:latest .

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
	@echo "Fixing milvus-storage.pc ..."
	@bash $(PWD)/scripts/fix_milvus_storage_pc.sh