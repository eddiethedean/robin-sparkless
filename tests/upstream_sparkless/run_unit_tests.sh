#!/bin/bash

# Fast unit tests for Mock-Spark
# These tests run directly against Mock-Spark without real PySpark

echo "🚀 Running Mock-Spark Unit Tests (Fast Mode)"
echo "============================================="

# Run unit tests (from tests directory)
python -m pytest unit/ -v --tb=short

echo ""
echo "✅ Unit tests completed!"
echo "💡 These tests run much faster than compatibility tests since they don't use real PySpark"
