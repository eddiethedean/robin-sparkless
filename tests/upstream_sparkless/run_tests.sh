#!/bin/bash

# Set up Java environment for PySpark
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Run tests with proper environment
echo "🚀 Running MockSpark tests with PySpark 3.2.4 and Java 11..."
echo "Java Home: $JAVA_HOME"
echo "Python Version: $(python --version)"
echo "PySpark Version: $(python -c 'import pyspark; print(pyspark.__version__)')"
echo ""

# Run all tests (from tests directory)
python -m pytest . -v --tb=short

echo ""
echo "✅ Test run completed!"
