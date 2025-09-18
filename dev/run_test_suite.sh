#!/bin/bash

# Comprehensive test runner for EloqStore
cd "$(dirname "$0")/build_test" || exit 1

echo "Starting EloqStore Test Suite Execution..."
echo "========================================="
echo ""

# Arrays to track results
declare -a PASSED_TESTS=()
declare -a FAILED_TESTS=()
declare -a CRASHED_TESTS=()
declare -a TIMEOUT_TESTS=()

TOTAL_TESTS=0
TOTAL_ASSERTIONS_PASSED=0
TOTAL_ASSERTIONS_FAILED=0

# Run each test with timeout
for test in *_test; do
    if [ -f "$test" ] && [ -x "$test" ]; then
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        echo -n "Running $test... "

        # Run test with timeout, capture output
        if timeout 5 ./"$test" > /tmp/${test}.out 2>&1; then
            # Check if test actually passed
            if grep -q "All tests passed" /tmp/${test}.out; then
                PASSED_TESTS+=("$test")
                echo "✅ PASSED"
                # Extract assertion count
                ASSERTIONS=$(grep "All tests passed" /tmp/${test}.out | grep -oE "[0-9]+ assertions" | grep -oE "[0-9]+")
                if [ -n "$ASSERTIONS" ]; then
                    TOTAL_ASSERTIONS_PASSED=$((TOTAL_ASSERTIONS_PASSED + ASSERTIONS))
                fi
            else
                FAILED_TESTS+=("$test")
                echo "❌ FAILED"
                # Extract failure details
                grep -E "test cases:|assertions:" /tmp/${test}.out | tail -1
            fi
        else
            EXIT_CODE=$?
            if [ $EXIT_CODE -eq 124 ]; then
                TIMEOUT_TESTS+=("$test")
                echo "⏱️ TIMEOUT"
            else
                CRASHED_TESTS+=("$test")
                echo "💥 CRASHED (exit code: $EXIT_CODE)"
                # Show crash reason if available
                if grep -q "SIGSEGV" /tmp/${test}.out; then
                    echo "  └─ Segmentation fault"
                elif grep -q "SIGABRT" /tmp/${test}.out; then
                    echo "  └─ Assertion failure/abort"
                fi
            fi
        fi
    fi
done

echo ""
echo "========================================="
echo "Test Suite Execution Complete"
echo "========================================="
echo ""

# Summary
echo "📊 SUMMARY"
echo "----------"
echo "Total Tests Run: $TOTAL_TESTS"
echo ""

echo "✅ Passed: ${#PASSED_TESTS[@]} tests"
if [ ${#PASSED_TESTS[@]} -gt 0 ]; then
    for test in "${PASSED_TESTS[@]}"; do
        echo "   - $test"
    done
fi
echo ""

echo "❌ Failed: ${#FAILED_TESTS[@]} tests"
if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    for test in "${FAILED_TESTS[@]}"; do
        echo "   - $test"
    done
fi
echo ""

echo "💥 Crashed: ${#CRASHED_TESTS[@]} tests"
if [ ${#CRASHED_TESTS[@]} -gt 0 ]; then
    for test in "${CRASHED_TESTS[@]}"; do
        echo "   - $test"
    done
fi
echo ""

echo "⏱️ Timeout: ${#TIMEOUT_TESTS[@]} tests"
if [ ${#TIMEOUT_TESTS[@]} -gt 0 ]; then
    for test in "${TIMEOUT_TESTS[@]}"; do
        echo "   - $test"
    done
fi
echo ""

# Calculate percentages
if [ $TOTAL_TESTS -gt 0 ]; then
    PASS_RATE=$(echo "scale=1; ${#PASSED_TESTS[@]} * 100 / $TOTAL_TESTS" | bc)
    echo "📈 Pass Rate: $PASS_RATE%"

    if [ ${#PASSED_TESTS[@]} -gt 0 ]; then
        echo "✓ Total Assertions Passed: $TOTAL_ASSERTIONS_PASSED"
    fi
fi

# Final verdict
echo ""
echo "========================================="
if [ ${#PASSED_TESTS[@]} -ge 20 ]; then
    echo "🎉 VERDICT: Test Framework is FUNCTIONAL"
elif [ ${#PASSED_TESTS[@]} -ge 10 ]; then
    echo "⚠️ VERDICT: Test Framework PARTIALLY WORKING"
else
    echo "❌ VERDICT: Test Framework has SIGNIFICANT ISSUES"
fi
echo "========================================="