#!/bin/bash

# Script to run EloqStore tests with Address Sanitizer
# Usage: ./run_with_asan.sh [test_executable] [test_args...]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILD_DIR="${SCRIPT_DIR}/../build"
REPORT_DIR="${SCRIPT_DIR}/../asan_reports"

# Create report directory
mkdir -p "$REPORT_DIR"

# ASAN options
export ASAN_OPTIONS="verbosity=1:halt_on_error=0:print_stats=1:check_initialization_order=1:detect_stack_use_after_return=1:strict_string_checks=1:print_module_map=1"
export LSAN_OPTIONS="suppressions=${SCRIPT_DIR}/lsan.supp:print_suppressions=0"
export ASAN_SYMBOLIZER_PATH=$(which llvm-symbolizer)

# Parse arguments
TEST_EXECUTABLE=""
TEST_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --help)
            echo "Usage: $0 [test_executable] [test_args...]"
            echo ""
            echo "Run tests with Address Sanitizer enabled."
            echo "Make sure tests were built with -DWITH_ASAN=ON"
            exit 0
            ;;
        *)
            if [[ -z "$TEST_EXECUTABLE" ]]; then
                TEST_EXECUTABLE="$1"
            else
                TEST_ARGS="$TEST_ARGS $1"
            fi
            shift
            ;;
    esac
done

# Function to run test with ASAN
run_with_asan() {
    local TEST_EXE="$1"
    local TEST_NAME=$(basename "$TEST_EXE")
    local LOG_FILE="$REPORT_DIR/${TEST_NAME}_asan.log"

    echo -e "${GREEN}Running $TEST_NAME with AddressSanitizer...${NC}"

    # Run test and capture both stdout and stderr
    if $TEST_EXE $TEST_ARGS > "$LOG_FILE" 2>&1; then
        # Check if ASAN found any issues
        if grep -q "ERROR: AddressSanitizer\|ERROR: LeakSanitizer" "$LOG_FILE"; then
            echo -e "${RED}✗ ASAN detected issues in $TEST_NAME${NC}"
            grep "ERROR:" "$LOG_FILE" | head -5
            echo -e "${YELLOW}Full report: $LOG_FILE${NC}"
            return 1
        else
            echo -e "${GREEN}✓ No issues detected${NC}"
            return 0
        fi
    else
        echo -e "${RED}✗ Test failed: $TEST_NAME${NC}"
        tail -20 "$LOG_FILE"
        echo -e "${YELLOW}Full report: $LOG_FILE${NC}"
        return 1
    fi
}

# Main execution
if [[ -z "$TEST_EXECUTABLE" ]]; then
    echo -e "${YELLOW}No test specified, running all ASAN-enabled tests...${NC}"

    # Check if build was done with ASAN
    if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]] || ! grep -q "WITH_ASAN:BOOL=ON" "$BUILD_DIR/CMakeCache.txt"; then
        echo -e "${RED}Warning: Build may not have ASAN enabled${NC}"
        echo "Rebuild with: cmake .. -DWITH_ASAN=ON"
    fi

    # Find all test executables
    TEST_EXECUTABLES=$(find "$BUILD_DIR" -name "*_test" -type f -executable 2>/dev/null || true)

    if [[ -z "$TEST_EXECUTABLES" ]]; then
        echo -e "${RED}No test executables found in $BUILD_DIR${NC}"
        exit 1
    fi

    FAILED_TESTS=""
    TOTAL_FAILED=0

    for TEST_EXE in $TEST_EXECUTABLES; do
        echo ""
        if ! run_with_asan "$TEST_EXE"; then
            TEST_NAME=$(basename "$TEST_EXE")
            FAILED_TESTS="$FAILED_TESTS $TEST_NAME"
            ((TOTAL_FAILED++))
        fi
    done

    echo -e "\n${GREEN}========================================${NC}"
    if [[ $TOTAL_FAILED -eq 0 ]]; then
        echo -e "${GREEN}All tests passed ASAN checks!${NC}"
    else
        echo -e "${RED}$TOTAL_FAILED tests failed ASAN checks:${NC}"
        echo -e "${RED}Failed tests: $FAILED_TESTS${NC}"
        echo -e "${YELLOW}Check reports in: $REPORT_DIR${NC}"
        exit 1
    fi
else
    # Run specific test
    if [[ ! -f "$TEST_EXECUTABLE" ]]; then
        TEST_EXECUTABLE="$BUILD_DIR/$TEST_EXECUTABLE"
    fi

    if [[ ! -f "$TEST_EXECUTABLE" ]]; then
        echo -e "${RED}Error: Test executable not found: $TEST_EXECUTABLE${NC}"
        exit 1
    fi

    if ! run_with_asan "$TEST_EXECUTABLE"; then
        exit 1
    fi
fi