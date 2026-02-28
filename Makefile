.PHONY: build build-rust build-go clean test bench

RUST_DIR := rust
GO_DIR := go
RUST_TARGET_DIR := $(RUST_DIR)/target/release

# Use Command Line Tools SDK to avoid Xcode license issues
export SDKROOT ?= /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk
export DEVELOPER_DIR ?= /Library/Developer/CommandLineTools

build: build-rust build-go

build-rust:
	cd $(RUST_DIR) && cargo build --release --workspace

build-go: build-rust
	mkdir -p bin
	cd $(GO_DIR) && CGO_ENABLED=1 \
		CGO_LDFLAGS="-L../$(RUST_TARGET_DIR) -lsearch_ffi" \
		go build -buildvcs=false -o ../bin/search-server .

clean:
	cd $(RUST_DIR) && cargo clean
	rm -rf bin/

test:
	cd $(RUST_DIR) && cargo test --workspace

bench:
	./scripts/bench_run.sh 10000

bench-large:
	./scripts/bench_run.sh 100000
