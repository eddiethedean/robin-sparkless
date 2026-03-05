"""
Test wrapper with actual process-level timeout.

This module provides a way to run tests with real timeouts that can't be bypassed.
"""

import subprocess
import sys
import time


def run_test_with_timeout(test_path, timeout_seconds=30):
    """Run a test with a real timeout that kills the process."""
    print(f"Running {test_path} with {timeout_seconds}s timeout...", flush=True)

    # Run pytest in a subprocess
    cmd = [sys.executable, "-m", "pytest", test_path, "-v", "-s", "--tb=short"]

    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        )

        start_time = time.time()
        output_lines = []

        # Read output line by line with timeout
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                print(
                    f"\nTIMEOUT: Test exceeded {timeout_seconds}s, killing process...",
                    flush=True,
                )
                process.kill()
                process.wait()
                return {
                    "status": "timeout",
                    "output": "".join(output_lines),
                    "elapsed": elapsed,
                }

            # Check if process is still running
            if process.poll() is not None:
                # Process finished
                remaining_output, _ = process.communicate()
                if remaining_output:
                    output_lines.append(remaining_output)
                break

            # Try to read a line (non-blocking)
            try:
                line = process.stdout.readline()
                if line:
                    print(line, end="", flush=True)
                    output_lines.append(line)
                elif process.poll() is not None:
                    break
            except Exception:
                pass

            time.sleep(0.1)  # Small delay to avoid busy waiting

        return {
            "status": "completed",
            "returncode": process.returncode,
            "output": "".join(output_lines),
            "elapsed": time.time() - start_time,
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_with_timeout.py <test_path> [timeout_seconds]")
        sys.exit(1)

    test_path = sys.argv[1]
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 30

    result = run_test_with_timeout(test_path, timeout)

    if result["status"] == "timeout":
        print(f"\n❌ Test timed out after {result['elapsed']:.1f}s")
        sys.exit(1)
    elif result["status"] == "error":
        print(f"\n❌ Error running test: {result['error']}")
        sys.exit(1)
    elif result["status"] == "completed":
        if result["returncode"] == 0:
            print(f"\n✅ Test passed in {result['elapsed']:.1f}s")
            sys.exit(0)
        else:
            print(
                f"\n❌ Test failed with return code {result['returncode']} in {result['elapsed']:.1f}s"
            )
            sys.exit(result["returncode"])
