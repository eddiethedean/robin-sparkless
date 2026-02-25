#!/bin/bash
# Test a single Python/PySpark combination to verify setup works

set -e

PYTHON_VERSION=${1:-3.10}
PYSPARK_VERSION=${2:-3.3.4}
JAVA_VERSION=${3:-11}

echo "=========================================="
echo "Testing Single Combination"
echo "=========================================="
echo "Python: $PYTHON_VERSION"
echo "PySpark: $PYSPARK_VERSION"
echo "Java: $JAVA_VERSION"
echo "=========================================="

# Build image
echo ""
echo "Building Docker image..."
docker build \
    --build-arg PYTHON_VERSION=$PYTHON_VERSION \
    --build-arg PYSPARK_VERSION=$PYSPARK_VERSION \
    --build-arg JAVA_VERSION=$JAVA_VERSION \
    -f tests/compatibility_matrix/Dockerfile.template \
    -t sparkless-test:test \
    .

# Run tests
echo ""
echo "Running tests..."
docker run --rm sparkless-test:test

echo ""
echo "=========================================="
echo "Test completed successfully!"
echo "=========================================="

