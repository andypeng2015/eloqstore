#!/bin/bash
# Script to build and run both EloqStore examples

set -e

echo "======================================"
echo "  EloqStore Examples Demonstration"
echo "======================================"
echo

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored headers
print_header() {
    echo -e "${BLUE}$1${NC}"
}

# Function to print success
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    print_error "Rust is not installed. Please install from https://rustup.rs/"
    exit 1
fi

# Check if C++ compiler is installed
if ! command -v g++ &> /dev/null; then
    print_error "g++ is not installed. Please install a C++ compiler."
    exit 1
fi

# Build the Rust library
print_header "Building EloqStore Rust library..."
if cargo build --release --features ffi > /dev/null 2>&1; then
    print_success "Rust library built successfully"
else
    print_error "Failed to build Rust library"
    exit 1
fi

echo

# Option menu
echo "Select which example to run:"
echo "1) Native Rust example"
echo "2) C++ FFI example"
echo "3) Both examples"
echo "4) Exit"
echo

read -p "Enter your choice (1-4): " choice

echo

case $choice in
    1)
        print_header "Running Native Rust Example..."
        echo "=========================================="
        cargo run --example native_usage 2>/dev/null || {
            print_error "Native Rust example failed"
            exit 1
        }
        print_success "Native Rust example completed"
        ;;

    2)
        print_header "Building C++ FFI Example..."
        cd examples
        if make cpp_example > /dev/null 2>&1; then
            print_success "C++ example built successfully"
        else
            print_error "Failed to build C++ example"
            exit 1
        fi

        echo
        print_header "Running C++ FFI Example..."
        echo "=========================================="
        LD_LIBRARY_PATH=../target/release ./cpp_example || {
            print_error "C++ FFI example failed"
            exit 1
        }
        print_success "C++ FFI example completed"
        cd ..
        ;;

    3)
        # Run Native Rust example
        print_header "Running Native Rust Example..."
        echo "=========================================="
        cargo run --example native_usage 2>/dev/null || {
            print_error "Native Rust example failed"
            exit 1
        }
        print_success "Native Rust example completed"

        echo
        echo "=========================================="
        echo

        # Build and run C++ example
        print_header "Building C++ FFI Example..."
        cd examples
        if make cpp_example > /dev/null 2>&1; then
            print_success "C++ example built successfully"
        else
            print_error "Failed to build C++ example"
            exit 1
        fi

        echo
        print_header "Running C++ FFI Example..."
        echo "=========================================="
        LD_LIBRARY_PATH=../target/release ./cpp_example || {
            print_error "C++ FFI example failed"
            exit 1
        }
        print_success "C++ FFI example completed"
        cd ..
        ;;

    4)
        echo "Exiting..."
        exit 0
        ;;

    *)
        print_error "Invalid choice"
        exit 1
        ;;
esac

echo
echo "======================================"
echo -e "${GREEN}All examples completed successfully!${NC}"
echo "======================================"
echo

# Cleanup
rm -rf example_data cpp_example_data examples/cpp_example 2>/dev/null

echo "Temporary data directories cleaned up."
echo
echo "To learn more about EloqStore:"
echo "  - Read the README.md for documentation"
echo "  - Check examples/ directory for source code"
echo "  - Run 'cargo test' to execute the test suite"
echo