#!/bin/bash
# Script to generate expected outputs from PySpark

echo "Generating Expected Outputs from PySpark"
echo "========================================"

# Check if PySpark is available
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "❌ PySpark not available. Install with:"
    echo "   pip install sparkless[generate-outputs]"
    echo "   or"
    echo "   pip install pyspark delta-spark"
    exit 1
fi

# Check if expected outputs directory exists
if [ ! -d "tests/expected_outputs" ]; then
    echo "❌ Expected outputs directory not found. Run setup first."
    exit 1
fi

echo "✅ PySpark available"
echo "📁 Expected outputs directory: tests/expected_outputs"
echo ""

# Define PySpark versions to generate outputs for
PYSPARK_VERSIONS=("3.2") # Add more versions as needed

# Define categories to generate outputs for
CATEGORIES=("dataframe_operations" "functions" "sql_operations" "window_operations" "joins" "arrays" "datetime" "windows" "null_handling" "set_operations" "chained_operations")

OUTPUT_DIR="tests/expected_outputs"

echo "Generating expected outputs for Mock-Spark compatibility tests..."
echo "-----------------------------------------------------------------"

# Loop through each PySpark version and category to generate outputs
for version in "${PYSPARK_VERSIONS[@]}"; do
    export SPARKLESS_PYSPARK_VERSION=$version
    echo "Generating outputs for PySpark version: $version"
    for category in "${CATEGORIES[@]}"; do
        echo "  Category: $category"
        python3 tests/tools/generate_expected_outputs.py --category "$category" --pyspark-version "$version" --output-dir "$OUTPUT_DIR"
        if [ $? -ne 0 ]; then
            echo "Error generating outputs for category $category, PySpark version $version. Aborting."
            exit 1
        fi
    done
done

echo "-----------------------------------------------------------------"
echo "Expected output generation complete."
unset SPARKLESS_PYSPARK_VERSION

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Expected outputs generated successfully"
    echo "📊 Check tests/expected_outputs/ for generated files"
    echo ""
    echo "Next steps:"
    echo "1. Run tests: ./tests/run_all_tests.sh"
    echo "2. Or run specific tests: pytest tests/compatibility/"
else
    echo "❌ Failed to generate expected outputs"
    exit 1
fi
