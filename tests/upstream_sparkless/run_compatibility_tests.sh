#!/bin/bash
# Convenience script to run compatibility matrix tests

set -e

echo "=========================================="
echo "Mock-Spark Compatibility Matrix Testing"
echo "=========================================="
echo ""
echo "This will test sparkless against multiple Python and PySpark"
echo "version combinations using Docker."
echo ""
echo "Estimated time: 30-60 minutes"
echo "Estimated disk space: 5-10 GB"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

# Run the compatibility tests
python tests/compatibility_matrix/run_matrix_tests.py

echo ""
echo "=========================================="
echo "Tests completed! Check COMPATIBILITY_REPORT.md for results."
echo "=========================================="

