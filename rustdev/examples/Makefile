# Makefile for EloqStore examples

CXX = g++
CXXFLAGS = -std=c++17 -Wall -O2
RUST_TARGET = ../target/release
LDFLAGS = -L$(RUST_TARGET) -leloqstore -lpthread -ldl -lm

# Build all examples
all: cpp_example simple_cpp build_rust

# Build the Rust library in release mode
build_rust:
	@echo "Building Rust library..."
	cd .. && cargo build --release --features ffi

# Build C++ FFI example
cpp_example: cpp_ffi_example.cpp build_rust
	@echo "Building C++ FFI example..."
	$(CXX) $(CXXFLAGS) cpp_ffi_example.cpp $(LDFLAGS) -o cpp_example

# Build simple C++ example
simple_cpp: simple_cpp_example.cpp build_rust
	@echo "Building simple C++ FFI example..."
	$(CXX) $(CXXFLAGS) simple_cpp_example.cpp $(LDFLAGS) -o simple_cpp_example

# Run native Rust example
run_native:
	@echo "Running native Rust example..."
	cargo run --example native_usage

# Run C++ FFI example
run_cpp: cpp_example
	@echo "Running C++ FFI example..."
	LD_LIBRARY_PATH=$(RUST_TARGET) ./cpp_example

# Run simple C++ example
run_simple: simple_cpp
	@echo "Running simple C++ example..."
	LD_LIBRARY_PATH=$(RUST_TARGET) ./simple_cpp_example

# Clean build artifacts
clean:
	rm -f cpp_example
	rm -rf ./example_data ./cpp_example_data
	cd .. && cargo clean

# Help
help:
	@echo "Available targets:"
	@echo "  all        - Build all examples"
	@echo "  cpp_example - Build C++ FFI example"
	@echo "  run_native - Run native Rust example"
	@echo "  run_cpp    - Run C++ FFI example"
	@echo "  clean      - Clean build artifacts"
	@echo "  help       - Show this help message"

.PHONY: all build_rust run_native run_cpp clean help