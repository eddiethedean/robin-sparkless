#!/usr/bin/env python3
"""
Validate compatibility testing setup without Docker.

This script checks that all required files exist and are properly configured.
"""

import sys
from pathlib import Path


def check_file_exists(path: Path, description: str) -> bool:
    """Check if a file exists."""
    if path.exists():
        print(f"✓ {description}: {path}")
        return True
    else:
        print(f"✗ {description}: {path} (NOT FOUND)")
        return False


def check_docker_available() -> bool:
    """Check if Docker is available."""
    try:
        import subprocess

        result = subprocess.run(
            ["docker", "--version"], capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            print(f"✓ Docker: {result.stdout.strip()}")
            return True
        else:
            print("✗ Docker: Not available")
            return False
    except FileNotFoundError:
        print("✗ Docker: Not installed")
        return False
    except Exception as e:
        print(f"✗ Docker: Error checking - {e}")
        return False


def main():
    """Validate setup."""
    project_root = Path(__file__).parent.parent.parent
    compat_dir = project_root / "tests" / "compatibility_matrix"

    print("=" * 70)
    print("Sparkless Compatibility Testing Setup Validation")
    print("=" * 70)
    print()

    all_good = True

    # Check required files
    print("Checking required files...")
    print()

    files_to_check = [
        (compat_dir / "Dockerfile.template", "Dockerfile template"),
        (compat_dir / "test_runner.sh", "Test runner script"),
        (compat_dir / "run_matrix_tests.py", "Matrix test orchestrator"),
        (compat_dir / "README.md", "README documentation"),
        (compat_dir / "QUICK_START.md", "Quick start guide"),
        (project_root / "run_compatibility_tests.sh", "Convenience wrapper"),
        (project_root / "COMPATIBILITY_TESTING_SETUP.md", "Setup documentation"),
    ]

    for path, description in files_to_check:
        if not check_file_exists(path, description):
            all_good = False

    print()

    # Check Docker
    print("Checking Docker...")
    print()
    docker_available = check_docker_available()
    if not docker_available:
        all_good = False
        print()
        print("Note: Docker is required to run compatibility tests.")
        print(
            "Install Docker Desktop from: https://www.docker.com/products/docker-desktop"
        )

    print()
    print("=" * 70)

    if all_good:
        print("✓ All checks passed! Ready to run compatibility tests.")
        print()
        print("To run tests:")
        print("  ./run_compatibility_tests.sh")
        return 0
    else:
        print("✗ Some checks failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
