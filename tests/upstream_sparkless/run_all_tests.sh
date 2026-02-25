#!/bin/bash
# Test runner for overhauled test suite
#
# Usage:
#   bash tests/run_all_tests.sh
#   SPARKLESS_TEST_WORKERS=12 bash tests/run_all_tests.sh
#   bash tests/run_all_tests.sh -n 12
#
# For Robin mode: SPARKLESS_TEST_BACKEND=robin bash tests/run_all_tests.sh -n 12

# Activate virtual environment if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
if [ -z "$VIRTUAL_ENV" ] && [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
fi

# Ensure project root is the first entry on PYTHONPATH so the local package is exercised
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH}"

# Parse -n N from arguments (e.g. -n 12)
WORKERS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -n)
            WORKERS="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

echo "Running Sparkless Test Suite (Overhauled)"
echo "=========================================="

# Check backend selection
BACKEND="${SPARKLESS_TEST_BACKEND:-mock}"
echo "Backend: $BACKEND"
if [ "$BACKEND" = "pyspark" ]; then
    echo "⚠️  Running with PySpark (slower)"
elif [ "$BACKEND" = "both" ]; then
    echo "⚠️  Running with both backends (comparison mode - slowest)"
elif [ "$BACKEND" = "robin" ]; then
    echo "Running with Robin backend (Sparkless + robin-sparkless)"
    export SPARKLESS_BACKEND=robin
    export SPARKLESS_TEST_BACKEND=robin
fi
echo ""

# Determine worker count: -n arg > SPARKLESS_TEST_WORKERS > default (8, or 4 for Robin to reduce worker crashes)
if [ -n "$WORKERS" ]; then
    # User explicitly passed -n N
    WORKER_COUNT="$WORKERS"
elif [ -n "${SPARKLESS_TEST_WORKERS:-}" ]; then
    WORKER_COUNT="${SPARKLESS_TEST_WORKERS}"
else
    if [ "$BACKEND" = "robin" ]; then
        # Robin mode: use fewer workers by default to reduce "node down" crashes and xdist KeyError (see docs/robin_mode_worker_crash_investigation.md)
        WORKER_COUNT="4"
    else
        WORKER_COUNT="8"
    fi
fi

# Check if pytest-xdist works by doing a minimal collect with -n
PARALLEL_FLAGS=""
if python3 -m pytest --co -n 1 -q tests/unit/ >/dev/null 2>&1; then
    echo "✅ pytest-xdist available - using parallel execution (${WORKER_COUNT} workers)"
    PARALLEL_FLAGS="-n ${WORKER_COUNT} --dist loadfile"
else
    echo "⚠️  pytest-xdist not available - running serially"
    echo "   Install with: pip install pytest-xdist"
fi

# Step 1: Unit tests - run in parallel
echo "Running unit tests..."
# Use timeout wrapper to prevent hangs (30 minutes max per test phase)
"$SCRIPT_DIR/run_with_timeout.sh" 1800 python3 -m pytest tests/unit/ -v $PARALLEL_FLAGS --tb=short -m "not performance"
unit_exit=$?
if [ $unit_exit -eq 124 ]; then
    echo "❌ Unit tests timed out after 30 minutes"
fi

# Step 2: Parity tests - validate PySpark parity using expected outputs (no PySpark required)
echo "Running parity tests (PySpark parity validation)..."
echo "  - DataFrame operations parity"
echo "  - Function operations parity" 
echo "  - Join operations parity"
echo "  - SQL operations parity"
echo "  - Internal operations parity"
"$SCRIPT_DIR/run_with_timeout.sh" 1800 python3 -m pytest tests/parity/ -v $PARALLEL_FLAGS --tb=short
parity_exit=$?
if [ $parity_exit -eq 124 ]; then
    echo "❌ Parity tests timed out after 30 minutes"
fi

# Step 3: Performance tests - run in parallel
echo "Running Performance tests (parallel)..."
"$SCRIPT_DIR/run_with_timeout.sh" 600 python3 -m pytest tests/unit/ -v $PARALLEL_FLAGS -m performance --tb=short
performance_exit=$?
if [ $performance_exit -eq 124 ]; then
    echo "❌ Performance tests timed out after 10 minutes"
fi

# Check if performance tests exist (exit code 5 means no tests found)
if [ $performance_exit -eq 5 ]; then
    echo "No performance tests found, skipping..."
    performance_exit=0
fi

# Step 4: Documentation tests - run in parallel
echo "Running documentation tests..."
"$SCRIPT_DIR/run_with_timeout.sh" 300 python3 -m pytest tests/documentation/ -v $PARALLEL_FLAGS --tb=short
doc_exit=$?
if [ $doc_exit -eq 124 ]; then
    echo "❌ Documentation tests timed out after 5 minutes"
fi

# Generate test summary
echo ""
echo "Test Summary"
echo "============"
echo "Unit tests: $([ $unit_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"
echo "Parity tests: $([ $parity_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"
echo "Performance tests: $([ $performance_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"
echo "Documentation tests: $([ $doc_exit -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")"

# Count total tests
total_tests=$(python3 -c "
import subprocess
import sys
try:
    result = subprocess.run([sys.executable, '-m', 'pytest', '--collect-only', '-q', 'tests/'], 
                          capture_output=True, text=True)
    lines = result.stdout.split('\n')
    for line in lines:
        if 'collected' in line and 'item' in line:
            print(line.split()[0])
            break
except:
    print('Unknown')
" 2>/dev/null)

echo "Total tests: $total_tests"

# Final result
if [ $unit_exit -ne 0 ] || [ $parity_exit -ne 0 ] || [ $performance_exit -ne 0 ] || [ $doc_exit -ne 0 ]; then
    echo ""
    echo "❌ Test suite FAILED"
    exit 1
else
    echo ""
    echo "✅ Test suite PASSED"
    echo "✅ All tests completed successfully without PySpark runtime dependency"
    echo "✅ Comprehensive compatibility testing across all major Sparkless features"
    exit 0
fi