#!/bin/bash
# Simple test runner with basic progress monitoring

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Function to run tests with basic progress monitoring
run_tests() {
    local test_pattern=$1
    local test_name=$2
    local parallel=$3
    local markers=$4
    
    echo ""
    echo "${CYAN}📋 ${test_name}${NC}"
    echo "${BLUE}================================${NC}"
    
    # Run tests and capture output
    if [ "$parallel" = "true" ]; then
        python3 -m pytest $test_pattern -v -n 8 --dist loadfile $markers --tb=short
    else
        python3 -m pytest $test_pattern -v $markers --tb=short
    fi
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "${GREEN}✅ ${test_name} completed successfully${NC}"
    else
        echo "${RED}❌ ${test_name} had failures${NC}"
    fi
    
    return $exit_code
}

# Main execution
echo "${WHITE}🧪 Running Mock Spark Test Suite${NC}"
echo "${BLUE}================================${NC}"
echo ""

# Step 1: Non-Delta tests
echo "${YELLOW}Starting Step 1: Non-Delta tests...${NC}"
run_tests "tests/" "Step 1: Non-Delta tests (parallel, 8 cores)" "true" "-m 'not delta and not performance'"
step1_exit=$?

# Step 2: Delta tests  
echo "${YELLOW}Starting Step 2: Delta tests...${NC}"
run_tests "tests/" "Step 2: Delta tests (serial)" "false" "-m 'delta'"
step2_exit=$?

# Step 3: Performance tests
echo "${YELLOW}Starting Step 3: Performance tests...${NC}"
run_tests "tests/" "Step 3: Performance tests (serial)" "false" "-m 'performance'"
step3_exit=$?

echo ""
echo "${GREEN}✅ All test phases completed!${NC}"
echo ""
echo "${BLUE}Summary:${NC}"
echo "${CYAN}• Non-Delta tests:${NC} Run in parallel with 8 cores (loadfile distribution)"
echo "${CYAN}• Delta tests:${NC} Run serially with isolated Spark sessions"
echo "${CYAN}• Performance tests:${NC} Run serially for stable timing measurements"
echo ""
echo "${YELLOW}Note:${NC} --dist loadfile ensures each worker processes complete test files,"
echo "      preventing PySpark session conflicts in compatibility tests."

# Exit with error if any step failed
if [ $step1_exit -ne 0 ] || [ $step2_exit -ne 0 ] || [ $step3_exit -ne 0 ]; then
    echo "${RED}❌ Some test phases failed${NC}"
    exit 1
else
    echo "${GREEN}🎉 All test phases passed!${NC}"
    exit 0
fi
