# Compatibility Testing Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    COMPATIBILITY TESTING SYSTEM                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PySpark       │    │   Sparkless     │    │   Test Runner   │
│   (Generator)   │    │   (Runtime)      │    │   (pytest)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Expected Output │    │ Actual Output    │    │ Comparison      │
│ Generator       │    │ (Sparkless)     │    │ Engine          │
│                 │    │                  │    │                 │
│ • Runs PySpark  │    │ • Runs Sparkless│    │ • Loads expected│
│ • Captures      │    │ • Produces       │    │ • Compares      │
│   outputs       │    │   results        │    │ • Reports       │
│ • Saves JSON    │    │                  │    │   differences   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       │                       │
┌─────────────────┐              │                       │
│ JSON Files      │              │                       │
│ • test_name.json│              │                       │
│ • Schema info   │              │                       │
│ • Data rows     │              │                       │
│ • Metadata      │              │                       │
└─────────────────┘              │                       │
         │                       │                       │
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
                    ┌─────────────────┐
                    │ Test Results    │
                    │ • Pass/Fail     │
                    │ • Error Details │
                    │ • Coverage      │
                    └─────────────────┘
```

## Component Details

### 1. Expected Output Generator (`generate_expected_outputs.py`)

**Purpose**: Runs PySpark code to generate "golden" reference outputs

**Key Features**:
- Creates PySpark session with specific version
- Executes test operations against PySpark
- Captures schema and data information
- Saves results as JSON files
- Handles multiple PySpark versions

**Input**: Test case definitions (functions, data, operations)
**Output**: JSON files with expected outputs

### 2. Output Loader (`output_loader.py`)

**Purpose**: Loads and caches expected outputs during test execution

**Key Features**:
- LRU cache for performance
- Schema validation
- Version-specific loading
- Error handling for missing files

**Input**: Category, test name, PySpark version
**Output**: Expected output dictionary

### 3. Comparison Engine (`comparison_utils.py`)

**Purpose**: Compares Sparkless results with expected outputs

**Key Features**:
- Schema comparison (field names, types, counts)
- Data comparison (values, nulls, precision)
- Row count validation
- Detailed error reporting
- Tolerance-based numerical comparison

**Input**: Sparkless DataFrame, expected output dictionary
**Output**: Comparison result with pass/fail status

### 4. Test Files (`test_*_compatibility.py`)

**Purpose**: Define compatibility tests using the expected output system

**Key Features**:
- Load expected outputs
- Execute Sparkless operations
- Assert equivalence
- Provide clear test descriptions

**Input**: Test data, Sparkless operations
**Output**: Test results (pass/fail)

## Data Flow

### Generation Phase (Offline)

```
1. Developer defines test cases in generator
2. Generator creates PySpark session
3. Generator executes operations against PySpark
4. Generator captures schema and data
5. Generator saves JSON files to expected_outputs/
```

### Testing Phase (Runtime)

```
1. Test loads expected output from JSON
2. Test creates Sparkless session
3. Test executes same operations against Sparkless
4. Test compares Sparkless result with expected output
5. Test reports pass/fail with detailed error messages
```

## File Structure

```
tests/
├── tools/                          # Infrastructure
│   ├── generate_expected_outputs.py    # PySpark → JSON
│   ├── output_loader.py               # JSON → Dictionary
│   └── comparison_utils.py            # Compare results
├── expected_outputs/               # Generated data
│   ├── functions/                  # Function tests
│   │   ├── string_upper.json
│   │   ├── math_sqrt.json
│   │   └── ...
│   ├── dataframe_operations/       # DataFrame tests
│   ├── joins/                      # Join tests
│   └── ...
├── compatibility/                  # Test files
│   ├── test_functions_compatibility.py
│   ├── test_joins_compatibility.py
│   └── ...
└── generate_expected_outputs.sh   # Generation script
```

## Benefits

### 1. **No Runtime PySpark Dependency**
- Tests run without PySpark installed
- Faster test execution
- Easier CI/CD setup

### 2. **Deterministic Results**
- Expected outputs are pre-generated and versioned
- Consistent test results across environments
- Easy to debug differences

### 3. **Version Control**
- Expected outputs stored in JSON files
- Can track changes over time
- Easy to compare across PySpark versions

### 4. **Performance**
- No PySpark session startup during tests
- Cached expected outputs
- Parallel test execution

### 5. **Maintainability**
- Clear separation of concerns
- Easy to add new test categories
- Reusable comparison logic

## Usage Patterns

### Adding New Test Category

1. **Add generator method**:
   ```python
   def _generate_new_category_outputs(self):
       test_cases = {
           "test_name": lambda df: df.select(F.function(df.column)),
       }
       for test_name, test_func in test_cases.items():
           self._generate_single_output("new_category", test_name, test_func)
   ```

2. **Create test file**:
   ```python
   class TestNewCategoryCompatibility:
       def test_function(self, spark):
           expected = load_expected_output("new_category", "test_name")
           df = spark.createDataFrame(expected["input_data"])
           result = df.select(F.function(df.column))
           assert_dataframes_equal(result, expected)
   ```

3. **Generate expected outputs**:
   ```bash
   python tests/tools/generate_expected_outputs.py --category new_category
   ```

4. **Run tests**:
   ```bash
   pytest tests/compatibility/test_new_category_compatibility.py
   ```

### Debugging Test Failures

1. **Check expected output file exists**:
   ```bash
   ls tests/expected_outputs/category/test_name.json
   ```

2. **Regenerate if missing**:
   ```bash
   python tests/tools/generate_expected_outputs.py --category category
   ```

3. **Compare schemas**:
   - Check field names match
   - Check field types match
   - Check field counts match

4. **Compare data**:
   - Check row counts match
   - Check values match (with tolerance)
   - Check null handling

## Error Handling

### Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `FileNotFoundError` | Missing expected output | Run generator |
| `Schema mismatch` | Different column names | Update Sparkless naming |
| `Data mismatch` | Different calculations | Fix Sparkless logic |
| `Row count mismatch` | Different filtering | Fix Sparkless operations |

### Debugging Tools

1. **Verbose test output**:
   ```bash
   pytest tests/compatibility/test_file.py -v -s
   ```

2. **Single test execution**:
   ```bash
   pytest tests/compatibility/test_file.py::TestClass::test_method -v
   ```

3. **Expected output inspection**:
   ```python
   expected = load_expected_output("category", "test_name")
   print(json.dumps(expected, indent=2))
   ```

## Future Enhancements

### 1. **Automated Generation**
- CI/CD integration for expected output generation
- Automatic updates when PySpark versions change

### 2. **Enhanced Comparison**
- Custom comparison functions for complex types
- Visual diff tools for large datasets

### 3. **Performance Testing**
- Benchmark Sparkless against PySpark performance
- Memory usage comparison

### 4. **Coverage Analysis**
- Track which Sparkless features are tested
- Identify gaps in compatibility testing

This architecture provides a robust, maintainable foundation for compatibility testing that can scale with the project's growth.
