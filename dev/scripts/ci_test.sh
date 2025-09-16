#!/bin/bash

# CI/CD test runner script for EloqStore
# This script runs the complete test suite suitable for CI environments

set -e

# Colors for output (disabled in CI environments)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEV_DIR="${SCRIPT_DIR}/.."
PROJECT_ROOT="${DEV_DIR}/.."
BUILD_DIR="${DEV_DIR}/build"

# Default options
BUILD_TYPE="${BUILD_TYPE:-Debug}"
RUN_UNIT_TESTS=true
RUN_INTEGRATION_TESTS=true
RUN_STRESS_TESTS=false
RUN_COVERAGE=false
RUN_SANITIZERS=false
TEST_SEED="${ELOQ_TEST_SEED:-$(date +%s)}"
WORK_DIR="${ELOQ_TEST_WORK_DIR:-/mnt/ramdisk}"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --no-unit)
            RUN_UNIT_TESTS=false
            shift
            ;;
        --no-integration)
            RUN_INTEGRATION_TESTS=false
            shift
            ;;
        --with-stress)
            RUN_STRESS_TESTS=true
            shift
            ;;
        --with-coverage)
            RUN_COVERAGE=true
            shift
            ;;
        --with-sanitizers)
            RUN_SANITIZERS=true
            shift
            ;;
        --seed)
            TEST_SEED="$2"
            shift 2
            ;;
        --work-dir)
            WORK_DIR="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --build-type TYPE      Build type (Debug/Release, default: Debug)"
            echo "  --no-unit             Skip unit tests"
            echo "  --no-integration      Skip integration tests"
            echo "  --with-stress         Run stress tests"
            echo "  --with-coverage       Generate coverage report"
            echo "  --with-sanitizers     Run with address sanitizer"
            echo "  --seed SEED           Random seed for tests"
            echo "  --work-dir DIR        Working directory for tests"
            echo "  --help               Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}EloqStore CI Test Runner${NC}"
echo -e "${BLUE}======================================${NC}"
echo "Build Type: $BUILD_TYPE"
echo "Test Seed: $TEST_SEED"
echo "Work Directory: $WORK_DIR"
echo ""

# Function to run a command and check its result
run_step() {
    local STEP_NAME="$1"
    shift
    echo -e "${YELLOW}[$(date +%H:%M:%S)] Running: $STEP_NAME${NC}"

    if "$@"; then
        echo -e "${GREEN}[$(date +%H:%M:%S)] ✓ $STEP_NAME completed successfully${NC}"
        return 0
    else
        echo -e "${RED}[$(date +%H:%M:%S)] ✗ $STEP_NAME failed${NC}"
        return 1
    fi
}

# Create work directory
run_step "Creating work directory" mkdir -p "$WORK_DIR/ci_test"

# Build main project if needed
if [ ! -f "${PROJECT_ROOT}/build/libeloqstore.a" ]; then
    echo -e "${YELLOW}Main project not built, building...${NC}"
    run_step "Building main project" bash -c "
        cd '$PROJECT_ROOT' && \
        mkdir -p build && \
        cd build && \
        cmake .. -DCMAKE_BUILD_TYPE=$BUILD_TYPE && \
        make -j$(nproc)
    "
fi

# Configure test build
CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=$BUILD_TYPE"

if [[ "$RUN_COVERAGE" == true ]] && [[ "$BUILD_TYPE" == "Debug" ]]; then
    CMAKE_FLAGS="$CMAKE_FLAGS -DCMAKE_CXX_FLAGS=\"--coverage -fprofile-arcs -ftest-coverage\""
fi

if [[ "$RUN_SANITIZERS" == true ]]; then
    CMAKE_FLAGS="$CMAKE_FLAGS -DWITH_ASAN=ON"
fi

# Build test suite
run_step "Configuring test build" bash -c "
    cd '$DEV_DIR' && \
    mkdir -p build && \
    cd build && \
    cmake .. $CMAKE_FLAGS
"

run_step "Building test suite" bash -c "
    cd '$BUILD_DIR' && \
    make -j$(nproc)
"

# Export test configuration
export ELOQ_TEST_SEED="$TEST_SEED"
export ELOQ_TEST_WORK_DIR="$WORK_DIR"

# Track test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
TEST_RESULTS=""

# Function to run a test and track results
run_test() {
    local TEST_NAME="$1"
    local TEST_CMD="$2"

    ((TOTAL_TESTS++))

    echo -e "\n${BLUE}Running $TEST_NAME...${NC}"

    if eval "$TEST_CMD"; then
        ((PASSED_TESTS++))
        TEST_RESULTS="${TEST_RESULTS}\n${GREEN}✓ $TEST_NAME${NC}"
    else
        ((FAILED_TESTS++))
        TEST_RESULTS="${TEST_RESULTS}\n${RED}✗ $TEST_NAME${NC}"
    fi
}

# Run unit tests
if [[ "$RUN_UNIT_TESTS" == true ]]; then
    echo -e "\n${BLUE}========== Unit Tests ==========${NC}"

    # Find and run all unit test executables
    for test_exe in ${BUILD_DIR}/tests/unit/*_test; do
        if [[ -f "$test_exe" ]] && [[ -x "$test_exe" ]]; then
            test_name=$(basename "$test_exe")
            run_test "Unit: $test_name" "$test_exe --seed=$TEST_SEED"
        fi
    done

    # Run other categorized tests
    for category in core io storage task sharding utils; do
        for test_exe in ${BUILD_DIR}/tests/${category}/*_test; do
            if [[ -f "$test_exe" ]] && [[ -x "$test_exe" ]]; then
                test_name=$(basename "$test_exe")
                run_test "$category: $test_name" "$test_exe"
            fi
        done
    done
fi

# Run integration tests
if [[ "$RUN_INTEGRATION_TESTS" == true ]]; then
    echo -e "\n${BLUE}========== Integration Tests ==========${NC}"

    test_exe="${BUILD_DIR}/integration_test"
    if [[ -f "$test_exe" ]]; then
        run_test "Integration Test" "$test_exe"
    fi

    test_exe="${BUILD_DIR}/concurrent_test"
    if [[ -f "$test_exe" ]]; then
        run_test "Concurrency Test" "$test_exe"
    fi

    test_exe="${BUILD_DIR}/data_integrity_test"
    if [[ -f "$test_exe" ]]; then
        run_test "Data Integrity Test" "$test_exe"
    fi
fi

# Run stress tests (shorter duration for CI)
if [[ "$RUN_STRESS_TESTS" == true ]]; then
    echo -e "\n${BLUE}========== Stress Tests ==========${NC}"

    test_exe="${BUILD_DIR}/stress_test"
    if [[ -f "$test_exe" ]]; then
        run_test "Basic Stress Test" "$test_exe --iterations=1000 --seed=$TEST_SEED"
    fi

    test_exe="${BUILD_DIR}/randomized_stress_test"
    if [[ -f "$test_exe" ]]; then
        run_test "Randomized Stress Test" "$test_exe --duration=60 --seed=$TEST_SEED"
    fi
fi

# Run with sanitizers if enabled
if [[ "$RUN_SANITIZERS" == true ]]; then
    echo -e "\n${BLUE}========== Sanitizer Tests ==========${NC}"

    if [[ -f "${SCRIPT_DIR}/run_with_asan.sh" ]]; then
        run_test "ASAN Check" "${SCRIPT_DIR}/run_with_asan.sh edge_case_test"
    fi
fi

# Generate coverage report
if [[ "$RUN_COVERAGE" == true ]] && [[ "$BUILD_TYPE" == "Debug" ]]; then
    echo -e "\n${BLUE}========== Coverage Report ==========${NC}"

    if [[ -f "${SCRIPT_DIR}/generate_coverage.sh" ]]; then
        run_step "Generating coverage report" "${SCRIPT_DIR}/generate_coverage.sh" --summary --xml
    fi
fi

# Print summary
echo -e "\n${BLUE}======================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}======================================${NC}"
echo -e "Total Tests: $TOTAL_TESTS"
echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
echo -e "Failed: ${RED}$FAILED_TESTS${NC}"
echo -e "\nResults:$TEST_RESULTS"

# Set exit code based on results
if [[ $FAILED_TESTS -eq 0 ]]; then
    echo -e "\n${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "\n${RED}$FAILED_TESTS tests failed${NC}"
    exit 1
fi