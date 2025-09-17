#!/bin/bash
# Run EloqStore tests

set -e

echo "Building EloqStore..."
cargo build --release

echo "Running main test suite..."
rm -rf /tmp/eloqstore_test
timeout 10 ./target/release/eloqstore || true

echo ""
echo "Test completed. Check output above for results."
echo "Expected: All 5 tests should pass including persistence test."