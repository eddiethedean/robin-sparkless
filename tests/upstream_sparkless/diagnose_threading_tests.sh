#!/bin/bash
# Quick diagnostic script to test threading-related code

echo "Testing threading implementation..."
echo "=================================="

# Test 1: Import test
echo ""
echo "Test 1: Can we import the storage module?"
python3 -c "from sparkless.backend.duckdb.storage import DuckDBStorageManager; print('✅ Import successful')" || echo "❌ Import failed"

# Test 2: Basic instantiation
echo ""
echo "Test 2: Can we create a storage manager?"
python3 -c "
from sparkless.backend.duckdb.storage import DuckDBStorageManager
import time
storage = DuckDBStorageManager()
print('✅ Storage manager created')
" || echo "❌ Storage manager creation failed"

# Test 3: Schema creation (should be fast)
echo ""
echo "Test 3: Can we create a schema (timeout: 5s)?"
timeout 5 python3 -c "
from sparkless.backend.duckdb.storage import DuckDBStorageManager
storage = DuckDBStorageManager()
storage.create_schema('test_schema')
print('✅ Schema creation successful')
" && echo "✅ Schema creation completed" || echo "❌ Schema creation timed out or failed"

# Test 4: Schema exists check (should be fast)
echo ""
echo "Test 4: Can we check if schema exists (timeout: 5s)?"
timeout 5 python3 -c "
from sparkless.backend.duckdb.storage import DuckDBStorageManager
storage = DuckDBStorageManager()
storage.create_schema('test_schema2')
exists = storage.schema_exists('test_schema2')
print(f'✅ Schema exists check: {exists}')
" && echo "✅ Schema exists check completed" || echo "❌ Schema exists check timed out or failed"

# Test 5: Thread-local connection (should be fast)
echo ""
echo "Test 5: Can we get thread-local connection (timeout: 5s)?"
timeout 5 python3 -c "
from sparkless.backend.duckdb.storage import _get_thread_connection, DuckDBStorageManager
storage = DuckDBStorageManager()
conn = _get_thread_connection(storage.engine)
print('✅ Thread-local connection obtained')
" && echo "✅ Thread-local connection completed" || echo "❌ Thread-local connection timed out or failed"

echo ""
echo "=================================="
echo "Diagnostic complete"




