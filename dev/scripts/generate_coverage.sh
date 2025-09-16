#!/bin/bash

# Script to generate code coverage reports for EloqStore tests
# Usage: ./generate_coverage.sh [--html] [--xml] [--summary]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="${SCRIPT_DIR}/../.."
DEV_DIR="${SCRIPT_DIR}/.."
BUILD_DIR="${DEV_DIR}/build"
COVERAGE_DIR="${DEV_DIR}/coverage"

# Default options
GENERATE_HTML=false
GENERATE_XML=false
SHOW_SUMMARY=false
CLEAN_BUILD=false
MIN_COVERAGE=80

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --html)
            GENERATE_HTML=true
            shift
            ;;
        --xml)
            GENERATE_XML=true
            shift
            ;;
        --summary)
            SHOW_SUMMARY=true
            shift
            ;;
        --clean)
            CLEAN_BUILD=true
            shift
            ;;
        --min-coverage)
            MIN_COVERAGE=$2
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --html              Generate HTML report"
            echo "  --xml               Generate XML report (for CI)"
            echo "  --summary           Show coverage summary only"
            echo "  --clean             Clean build before generating coverage"
            echo "  --min-coverage N    Minimum coverage threshold (default: 80)"
            echo "  --help              Show this help message"
            echo ""
            echo "Note: Requires building with coverage flags enabled"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# If no output format specified, default to HTML and summary
if [[ "$GENERATE_HTML" == false ]] && [[ "$GENERATE_XML" == false ]] && [[ "$SHOW_SUMMARY" == false ]]; then
    GENERATE_HTML=true
    SHOW_SUMMARY=true
fi

# Check for required tools
check_tool() {
    if ! command -v $1 &> /dev/null; then
        echo -e "${RED}Error: $1 is not installed${NC}"
        echo "Please install: sudo apt-get install $2"
        exit 1
    fi
}

check_tool lcov lcov
check_tool genhtml lcov
check_tool gcov gcc

echo -e "${BLUE}EloqStore Code Coverage Report Generator${NC}"
echo "========================================="

# Clean and rebuild with coverage if requested
if [[ "$CLEAN_BUILD" == true ]]; then
    echo -e "${YELLOW}Cleaning and rebuilding with coverage flags...${NC}"

    rm -rf "$BUILD_DIR"
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    cmake .. -DCMAKE_BUILD_TYPE=Debug \
             -DCMAKE_CXX_FLAGS="--coverage -fprofile-arcs -ftest-coverage" \
             -DCMAKE_C_FLAGS="--coverage -fprofile-arcs -ftest-coverage" \
             -DCMAKE_EXE_LINKER_FLAGS="--coverage"

    make -j$(nproc)
    cd -
else
    # Check if build has coverage enabled
    if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
        echo -e "${RED}Error: Build directory not found${NC}"
        echo "Run with --clean to build with coverage enabled"
        exit 1
    fi

    # Check for .gcno files (compile-time coverage files)
    if ! find "$BUILD_DIR" -name "*.gcno" -print -quit | grep -q .; then
        echo -e "${RED}Error: No coverage files found${NC}"
        echo "Rebuild with coverage flags:"
        echo "  cmake .. -DCMAKE_CXX_FLAGS=\"--coverage\""
        exit 1
    fi
fi

# Clean previous coverage data
echo -e "${YELLOW}Cleaning previous coverage data...${NC}"
rm -rf "$COVERAGE_DIR"
mkdir -p "$COVERAGE_DIR"

# Reset coverage counters
find "$BUILD_DIR" -name "*.gcda" -delete 2>/dev/null || true

# Run all tests to generate coverage data
echo -e "${GREEN}Running tests to generate coverage data...${NC}"

cd "$BUILD_DIR"
ctest --output-on-failure || true
cd -

# Check if any coverage data was generated
if ! find "$BUILD_DIR" -name "*.gcda" -print -quit | grep -q .; then
    echo -e "${RED}Error: No coverage data generated${NC}"
    echo "Tests may not have run successfully"
    exit 1
fi

# Capture coverage data
echo -e "${YELLOW}Capturing coverage data...${NC}"

lcov --capture \
     --directory "$BUILD_DIR" \
     --output-file "$COVERAGE_DIR/coverage_raw.info" \
     --no-external \
     --rc lcov_branch_coverage=1

# Filter out system headers and test files
echo -e "${YELLOW}Filtering coverage data...${NC}"

lcov --remove "$COVERAGE_DIR/coverage_raw.info" \
     '/usr/*' \
     '*/tests/*' \
     '*/test/*' \
     '*/catch2/*' \
     '*/build/*' \
     '*/dev/*' \
     --output-file "$COVERAGE_DIR/coverage.info" \
     --rc lcov_branch_coverage=1

# Generate summary if requested
if [[ "$SHOW_SUMMARY" == true ]]; then
    echo -e "\n${BLUE}Coverage Summary:${NC}"
    echo "=================="

    # Extract coverage percentages
    SUMMARY=$(lcov --summary "$COVERAGE_DIR/coverage.info" 2>&1)

    # Parse line coverage
    LINE_COV=$(echo "$SUMMARY" | grep "lines\.\.\.\.\.\." | sed 's/.*: \([0-9.]*\)%.*/\1/')
    FUNC_COV=$(echo "$SUMMARY" | grep "functions\.\.\." | sed 's/.*: \([0-9.]*\)%.*/\1/')
    BRANCH_COV=$(echo "$SUMMARY" | grep "branches\.\.\.\." | sed 's/.*: \([0-9.]*\)%.*/\1/')

    echo "$SUMMARY"

    # Check against minimum threshold
    if (( $(echo "$LINE_COV < $MIN_COVERAGE" | bc -l) )); then
        echo -e "\n${RED}✗ Coverage is below minimum threshold of ${MIN_COVERAGE}%${NC}"
        EXIT_CODE=1
    else
        echo -e "\n${GREEN}✓ Coverage meets minimum threshold of ${MIN_COVERAGE}%${NC}"
        EXIT_CODE=0
    fi
fi

# Generate HTML report if requested
if [[ "$GENERATE_HTML" == true ]]; then
    echo -e "\n${YELLOW}Generating HTML report...${NC}"

    genhtml "$COVERAGE_DIR/coverage.info" \
            --output-directory "$COVERAGE_DIR/html" \
            --title "EloqStore Code Coverage Report" \
            --legend \
            --show-details \
            --highlight \
            --demangle-cpp \
            --rc genhtml_branch_coverage=1

    echo -e "${GREEN}✓ HTML report generated: $COVERAGE_DIR/html/index.html${NC}"

    # Try to open in browser if display is available
    if [[ -n "$DISPLAY" ]]; then
        if command -v xdg-open &> /dev/null; then
            xdg-open "$COVERAGE_DIR/html/index.html" 2>/dev/null &
        fi
    fi
fi

# Generate XML report if requested (for CI integration)
if [[ "$GENERATE_XML" == true ]]; then
    echo -e "\n${YELLOW}Generating XML report...${NC}"

    # Convert to Cobertura XML format (common CI format)
    if command -v lcov_cobertura &> /dev/null; then
        lcov_cobertura "$COVERAGE_DIR/coverage.info" \
                       --output "$COVERAGE_DIR/coverage.xml" \
                       --base-dir "$PROJECT_ROOT"

        echo -e "${GREEN}✓ XML report generated: $COVERAGE_DIR/coverage.xml${NC}"
    else
        echo -e "${YELLOW}Warning: lcov_cobertura not found${NC}"
        echo "Install with: pip install lcov_cobertura"
    fi
fi

# Generate per-file coverage report
echo -e "\n${BLUE}Top 10 Files by Coverage:${NC}"
echo "=========================="

lcov --list "$COVERAGE_DIR/coverage.info" | \
    grep -E "\.cpp|\.h" | \
    sort -t '|' -k2 -nr | \
    head -10 | \
    while IFS='|' read file cov; do
        # Color code based on coverage
        COV_NUM=$(echo "$cov" | grep -oE "[0-9]+\.[0-9]+")
        if (( $(echo "$COV_NUM >= 90" | bc -l) )); then
            COLOR=$GREEN
        elif (( $(echo "$COV_NUM >= 70" | bc -l) )); then
            COLOR=$YELLOW
        else
            COLOR=$RED
        fi

        printf "${COLOR}%-60s %s${NC}\n" "$file" "$cov"
    done

echo -e "\n${BLUE}Files with Low Coverage (<50%):${NC}"
echo "==============================="

lcov --list "$COVERAGE_DIR/coverage.info" | \
    grep -E "\.cpp|\.h" | \
    while IFS='|' read file cov; do
        COV_NUM=$(echo "$cov" | grep -oE "[0-9]+\.[0-9]+" | head -1)
        if [[ -n "$COV_NUM" ]] && (( $(echo "$COV_NUM < 50" | bc -l) )); then
            printf "${RED}%-60s %s${NC}\n" "$file" "$cov"
        fi
    done

exit ${EXIT_CODE:-0}