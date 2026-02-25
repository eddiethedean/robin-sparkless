#!/bin/bash
# Run pytest with timeout (works on macOS and Linux)

TIMEOUT_SECONDS=$1
shift

# Check if timeout command exists (Linux)
if command -v timeout &> /dev/null; then
    timeout $TIMEOUT_SECONDS "$@"
    exit $?
fi

# Check if gtimeout exists (macOS with coreutils)
if command -v gtimeout &> /dev/null; then
    gtimeout $TIMEOUT_SECONDS "$@"
    exit $?
fi

# Fallback: Use Python-based timeout
python3 - "$TIMEOUT_SECONDS" "$@" << 'PYTHON_SCRIPT'
import subprocess
import sys
import time

timeout_seconds = int(sys.argv[1])
command = sys.argv[2:]

# Start the process
process = subprocess.Popen(command, stdout=sys.stdout, stderr=sys.stderr)

start_time = time.time()

# Wait for process with timeout
while process.poll() is None:
    elapsed = time.time() - start_time
    if elapsed > timeout_seconds:
        print(f"\n⚠️  Process exceeded {timeout_seconds}s timeout, terminating...", file=sys.stderr, flush=True)
        process.terminate()
        time.sleep(2)
        if process.poll() is None:
            process.kill()
        sys.exit(124)  # Timeout exit code
    time.sleep(0.1)

sys.exit(process.returncode)
PYTHON_SCRIPT

