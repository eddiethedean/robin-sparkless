#!/bin/bash
# Test runner script for compatibility testing
# Runs a subset of critical tests to verify sparkless compatibility

set -e

echo "=========================================="
echo "Mock-Spark Compatibility Test Runner"
echo "=========================================="
echo "Python version: $(python --version)"
echo "PySpark version: $(python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'Not installed')"
echo "Java version: $(java -version 2>&1 | head -n 1)"
echo "=========================================="
echo ""

# Test 1: Import test
echo "[1/5] Testing package import..."
if python -c "import sparkless; print(f'sparkless version: {sparkless.__version__}')"; then
    echo "✓ Import successful"
else
    echo "✗ Import failed"
    exit 1
fi
echo ""

# Test 2: Basic operations
echo "[2/5] Testing basic operations..."
if python -m pytest tests/unit/test_basic_operations.py -v --tb=short; then
    echo "✓ Basic operations passed"
else
    echo "✗ Basic operations failed"
    exit 1
fi
echo ""

# Test 3: Data types
echo "[3/5] Testing data types..."
if python -m pytest tests/unit/test_data_types.py -v --tb=short; then
    echo "✓ Data types passed"
else
    echo "✗ Data types failed"
    exit 1
fi
echo ""

# Test 4: SQL operations
echo "[4/5] Testing SQL operations..."
if python -m pytest tests/unit/test_sql_ddl_catalog.py -v --tb=short; then
    echo "✓ SQL operations passed"
else
    echo "✗ SQL operations failed"
    exit 1
fi
echo ""

# Test 5: Basic compatibility
echo "[5/5] Testing PySpark compatibility..."
if python -m pytest tests/compatibility/test_basic_compatibility.py -v --tb=short; then
    echo "✓ Compatibility tests passed"
else
    echo "✗ Compatibility tests failed"
    exit 1
fi
echo ""

echo "=========================================="
echo "All tests passed successfully!"
echo "=========================================="

