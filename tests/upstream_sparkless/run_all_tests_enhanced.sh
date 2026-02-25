#!/bin/bash
# Enhanced test runner with real-time progress monitoring and better error handling

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

# Function to show a simple progress indicator
show_progress() {
    local passed=$1
    local failed=$2
    local skipped=$3
    local errors=$4
    local total=$5
    
    printf "\r${WHITE}                                                                    ${NC}\r"
    printf "${GREEN}✅ $passed${NC} ${RED}❌ $failed${NC} ${YELLOW}⏭️ $skipped${NC} ${PURPLE}⚠️ $errors${NC} ${BLUE}📊 $total${NC}"
}

# Function to run tests with real-time progress monitoring
run_tests_with_progress() {
    local test_pattern=$1
    local test_name=$2
    local parallel=$3
    local markers=$4
    
    echo ""
    echo "${CYAN}📋 ${test_name}${NC}"
    echo "${BLUE}================================${NC}"
    
    # Create temporary file for output
    local output_file=$(mktemp)
    
    # Start pytest in background
    if [ "$parallel" = "true" ]; then
        python3 -m pytest $test_pattern -v -n 8 --dist loadfile $markers --tb=short > $output_file 2>&1 &
    else
        python3 -m pytest $test_pattern -v $markers --tb=short > $output_file 2>&1 &
    fi
    
    local test_pid=$!
    
    # Initialize counters
    local passed=0
    local failed=0
    local skipped=0
    local errors=0
    local total=0
    local last_update=0
    
    echo "${YELLOW}🔍 Running tests...${NC}"
    
    # Monitor progress
    while kill -0 $test_pid 2>/dev/null; do
        # Count current results
        local new_passed=$(grep -c "PASSED" "$output_file" 2>/dev/null || echo "0")
        local new_failed=$(grep -c "FAILED" "$output_file" 2>/dev/null || echo "0")
        local new_skipped=$(grep -c "SKIPPED" "$output_file" 2>/dev/null || echo "0")
        local new_errors=$(grep -c "ERROR" "$output_file" 2>/dev/null || echo "0")
        
        # Update display if counts changed
        if [ "$new_passed" != "$passed" ] || [ "$new_failed" != "$failed" ] || [ "$new_skipped" != "$skipped" ] || [ "$new_errors" != "$errors" ]; then
            passed=$new_passed
            failed=$new_failed
            skipped=$new_skipped
            errors=$new_errors
            total=$((passed + failed + skipped + errors))
            last_update=$(date +%s)
            
            show_progress $passed $failed $skipped $errors $total
        fi
        
        sleep 0.5
    done
    
    # Wait for completion
    wait $test_pid
    local exit_code=$?
    
    # Get final results
    passed=$(grep -c "PASSED" "$output_file" 2>/dev/null || echo "0")
    failed=$(grep -c "FAILED" "$output_file" 2>/dev/null || echo "0")
    skipped=$(grep -c "SKIPPED" "$output_file" 2>/dev/null || echo "0")
    errors=$(grep -c "ERROR" "$output_file" 2>/dev/null || echo "0")
    total=$((passed + failed + skipped + errors))
    
    # Clear progress line and show final results
    printf "\r${WHITE}                                                                    ${NC}\r"
    
    echo "${GREEN}✅ Passed: $passed${NC}"
    echo "${RED}❌ Failed: $failed${NC}"
    echo "${YELLOW}⏭️ Skipped: $skipped${NC}"
    echo "${PURPLE}⚠️ Errors: $errors${NC}"
    echo "${BLUE}📊 Total: $total${NC}"
    
    # Show summary
    if [ $failed -eq 0 ] && [ $errors -eq 0 ]; then
        echo "${GREEN}🎉 All tests passed!${NC}"
    else
        echo "${RED}⚠️ Some tests failed or had errors${NC}"
        
        # Show recent failures if any
        if [ $failed -gt 0 ] || [ $errors -gt 0 ]; then
            echo "${YELLOW}📋 Recent failures:${NC}"
            tail -20 "$output_file" | grep -E "(FAILED|ERROR)" | head -3 | sed 's/^/  /'
        fi
    fi
    
    # Clean up
    rm -f "$output_file"
    
    return $exit_code
}

# Main execution
echo "${WHITE}🧪 Running Mock Spark Test Suite${NC}"
echo "${BLUE}================================${NC}"
echo ""

# Track overall results
overall_passed=0
overall_failed=0
overall_skipped=0
overall_errors=0

# Step 1: Non-Delta tests
echo "${YELLOW}Starting Step 1: Non-Delta tests...${NC}"
run_tests_with_progress "tests/" "Step 1: Non-Delta tests (parallel, 8 cores)" "true" "-m 'not delta and not performance'"
step1_exit=$?

# Step 2: Delta tests  
echo "${YELLOW}Starting Step 2: Delta tests...${NC}"
run_tests_with_progress "tests/" "Step 2: Delta tests (serial)" "false" "-m 'delta'"
step2_exit=$?

# Step 3: Performance tests
echo "${YELLOW}Starting Step 3: Performance tests...${NC}"
run_tests_with_progress "tests/" "Step 3: Performance tests (serial)" "false" "-m 'performance'"
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