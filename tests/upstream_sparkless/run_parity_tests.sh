#!/bin/bash
# Test runner for unified PySpark parity tests

# Activate virtual environment if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
if [ -z "$VIRTUAL_ENV" ] && [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
fi

# Ensure project root is the first entry on PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH}"

echo "Running PySpark Parity Tests"
echo "============================"
echo ""

# Check if pytest-xdist is available for parallel execution
if python3 -c "import pytest_xdist" 2>/dev/null; then
    echo "✅ pytest-xdist available - using parallel execution (8 workers)"
    PARALLEL_FLAGS="-n 8"
else
    echo "⚠️  pytest-xdist not available - running serially"
    echo "   Install with: pip install pytest-xdist"
    PARALLEL_FLAGS=""
fi

# Run all parity tests
echo "Running all parity tests..."
python3 -m pytest tests/parity/ -v $PARALLEL_FLAGS --tb=short

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "✅ All parity tests passed!"
else
    echo ""
    echo "❌ Some parity tests failed"
fi

exit $exit_code

