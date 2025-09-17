#!/bin/bash
# Run stress test on ramdisk

set -e

# Create ramdisk directory if it doesn't exist
if [ ! -d "/mnt/ramdisk" ]; then
    echo "Creating /mnt/ramdisk directory..."
    sudo mkdir -p /mnt/ramdisk
    sudo mount -t tmpfs -o size=2G tmpfs /mnt/ramdisk
    sudo chmod 777 /mnt/ramdisk
fi

# Check if ramdisk is mounted
if ! mountpoint -q /mnt/ramdisk; then
    echo "Mounting ramdisk..."
    sudo mount -t tmpfs -o size=2G tmpfs /mnt/ramdisk
    sudo chmod 777 /mnt/ramdisk
fi

echo "=== Running EloqStore Stress Test on /mnt/ramdisk ==="
echo "Available space on ramdisk:"
df -h /mnt/ramdisk

# Clean up any existing test data
rm -rf /mnt/ramdisk/eloqstore_stress_*

# Run stress test with smaller parameters for initial testing
echo ""
echo "Building release version for better performance..."
cargo build --release --example stress_test

echo ""
echo "Running stress test..."
RUST_LOG=info cargo run --release --example stress_test

echo ""
echo "Cleaning up test data..."
rm -rf /mnt/ramdisk/eloqstore_stress_*

echo "Test complete!"