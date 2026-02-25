#!/bin/bash
# Test runner for comparison testing with both PySpark and sparkless

# Activate virtual environment if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
if [ -z "$VIRTUAL_ENV" ] && [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
fi

# Ensure project root is the first entry on PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH}"

echo "Running Comparison Tests (PySpark vs Mock-Spark)"
echo "================================================="
echo ""
echo "This will run tests with both backends and compare results."
echo ""

# Set backend to both for comparison
export SPARKLESS_TEST_BACKEND=both

# Check if pytest-xdist is available
if python3 -c "import pytest_xdist" 2>/dev/null; then
    echo "✅ pytest-xdist available"
    PARALLEL_FLAGS="-n 4"  # Use fewer workers for comparison (slower)
else
    echo "⚠️  pytest-xdist not available - running serially"
    PARALLEL_FLAGS=""
fi

# Check if PySpark is available
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "❌ PySpark is not available"
    echo "   Install with: pip install pyspark"
    echo "   Or use: pip install sparkless[generate-outputs]"
    exit 1
fi

echo "✅ PySpark is available"
echo ""

# Run tests with comparison mode
# Filter to tests marked for comparison or all if no filter specified
TEST_PATH="${1:-tests/}"

echo "Running comparison tests: $TEST_PATH"
echo ""

python3 -m pytest "$TEST_PATH" \
    -v \
    $PARALLEL_FLAGS \
    --tb=short \
    -m "backend('both') or not backend" \
    --backend=both

exit_code=$?

echo ""
if [ $exit_code -eq 0 ]; then
    echo "✅ Comparison tests completed"
    echo "✅ All results match between PySpark and sparkless"
else
    echo "❌ Comparison tests found differences"
    echo "   Review test output for details"
fi

exit $exit_code

