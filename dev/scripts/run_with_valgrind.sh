#!/bin/bash

# Script to run EloqStore tests with valgrind memory checking
# Usage: ./run_with_valgrind.sh [test_executable] [test_args...]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default valgrind options
VALGRIND_OPTS="--leak-check=full --show-leak-kinds=all --track-origins=yes --verbose"
VALGRIND_OPTS="$VALGRIND_OPTS --suppressions=${SCRIPT_DIR}/valgrind.supp"

# Parse command line arguments
TEST_EXECUTABLE=""
TEST_ARGS=""
GENERATE_SUPPRESSIONS=false
CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --generate-suppressions)
            GENERATE_SUPPRESSIONS=true
            shift
            ;;
        --check-only)
            CHECK_ONLY=true
            shift
            ;;
        --help)
            echo "Usage: $0 [options] test_executable [test_args...]"
            echo ""
            echo "Options:"
            echo "  --generate-suppressions  Generate suppressions for known issues"
            echo "  --check-only            Only check for leaks, don't run full analysis"
            echo "  --help                  Show this help message"
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

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILD_DIR="${SCRIPT_DIR}/../build"
REPORT_DIR="${SCRIPT_DIR}/../valgrind_reports"

# Create report directory
mkdir -p "$REPORT_DIR"

# Check if valgrind is installed
if ! command -v valgrind &> /dev/null; then
    echo -e "${RED}Error: valgrind is not installed${NC}"
    echo "Please install valgrind: sudo apt-get install valgrind"
    exit 1
fi

# If no test specified, run all tests
if [[ -z "$TEST_EXECUTABLE" ]]; then
    echo -e "${YELLOW}No test specified, running all tests with valgrind...${NC}"

    # Find all test executables
    TEST_EXECUTABLES=$(find "$BUILD_DIR" -name "*_test" -type f -executable 2>/dev/null || true)

    if [[ -z "$TEST_EXECUTABLES" ]]; then
        echo -e "${RED}No test executables found in $BUILD_DIR${NC}"
        exit 1
    fi

    TOTAL_LEAKS=0
    FAILED_TESTS=""

    for TEST_EXE in $TEST_EXECUTABLES; do
        TEST_NAME=$(basename "$TEST_EXE")
        echo -e "\n${GREEN}Running valgrind on $TEST_NAME...${NC}"

        REPORT_FILE="$REPORT_DIR/${TEST_NAME}_valgrind.xml"
        LOG_FILE="$REPORT_DIR/${TEST_NAME}_valgrind.log"

        if [[ "$CHECK_ONLY" == true ]]; then
            OPTS="--leak-check=summary"
        else
            OPTS="$VALGRIND_OPTS --xml=yes --xml-file=$REPORT_FILE"
        fi

        # Run test with valgrind
        if valgrind $OPTS --log-file="$LOG_FILE" "$TEST_EXE" $TEST_ARGS 2>&1; then
            # Check for memory leaks in log
            if grep -q "definitely lost: 0 bytes" "$LOG_FILE" && \
               grep -q "indirectly lost: 0 bytes" "$LOG_FILE"; then
                echo -e "${GREEN}✓ No memory leaks detected${NC}"
            else
                echo -e "${RED}✗ Memory leaks detected!${NC}"
                grep "definitely lost\|indirectly lost" "$LOG_FILE"
                FAILED_TESTS="$FAILED_TESTS $TEST_NAME"
                ((TOTAL_LEAKS++))
            fi
        else
            echo -e "${RED}✗ Test failed under valgrind${NC}"
            FAILED_TESTS="$FAILED_TESTS $TEST_NAME"
        fi
    done

    echo -e "\n${GREEN}========================================${NC}"
    if [[ $TOTAL_LEAKS -eq 0 ]]; then
        echo -e "${GREEN}All tests passed without memory leaks!${NC}"
    else
        echo -e "${RED}$TOTAL_LEAKS tests had memory issues:${NC}"
        echo -e "${RED}Failed tests: $FAILED_TESTS${NC}"
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

    TEST_NAME=$(basename "$TEST_EXECUTABLE")
    REPORT_FILE="$REPORT_DIR/${TEST_NAME}_valgrind.xml"
    LOG_FILE="$REPORT_DIR/${TEST_NAME}_valgrind.log"

    echo -e "${GREEN}Running valgrind on $TEST_NAME...${NC}"

    if [[ "$GENERATE_SUPPRESSIONS" == true ]]; then
        VALGRIND_OPTS="$VALGRIND_OPTS --gen-suppressions=all"
    fi

    if [[ "$CHECK_ONLY" == true ]]; then
        OPTS="--leak-check=summary"
    else
        OPTS="$VALGRIND_OPTS --xml=yes --xml-file=$REPORT_FILE"
    fi

    # Run with valgrind
    valgrind $OPTS --log-file="$LOG_FILE" "$TEST_EXECUTABLE" $TEST_ARGS

    echo -e "\n${GREEN}Valgrind report saved to: $LOG_FILE${NC}"

    # Check results
    if grep -q "ERROR SUMMARY: 0 errors" "$LOG_FILE"; then
        echo -e "${GREEN}✓ No errors detected${NC}"
    else
        echo -e "${RED}✗ Errors detected!${NC}"
        grep "ERROR SUMMARY" "$LOG_FILE"
    fi

    if grep -q "definitely lost: 0 bytes" "$LOG_FILE" && \
       grep -q "indirectly lost: 0 bytes" "$LOG_FILE"; then
        echo -e "${GREEN}✓ No memory leaks detected${NC}"
    else
        echo -e "${YELLOW}⚠ Potential memory leaks:${NC}"
        grep "definitely lost\|indirectly lost\|possibly lost" "$LOG_FILE"
    fi
fi