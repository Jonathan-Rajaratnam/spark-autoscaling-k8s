#!/usr/bin/env bash
# run_tests.sh — Run the full test suite locally.
#
# Requires:
#   - Java 17 (installed via 'brew install openjdk@17')
#   - Python venv at .venv/ (created via 'python3 -m venv .venv && pip install -r requirements.txt')
#
# Usage:
#   ./run_tests.sh            # Run all tests
#   ./run_tests.sh -k unit    # Run only unit tests
#   ./run_tests.sh -k integration
#   ./run_tests.sh -k performance

set -euo pipefail

JAVA17_HOME="/opt/homebrew/Cellar/openjdk@17/17.0.18/libexec/openjdk.jdk/Contents/Home"
VENV_PYTHON="$(pwd)/.venv/bin/python3"
PYTEST="$(pwd)/.venv/bin/pytest"

if [ ! -d "$JAVA17_HOME" ]; then
  echo "ERROR: Java 17 not found at $JAVA17_HOME"
  echo "Install it with: brew install openjdk@17"
  exit 1
fi

if [ ! -f "$VENV_PYTHON" ]; then
  echo "ERROR: Python venv not found. Run:"
  echo "  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

export JAVA_HOME="$JAVA17_HOME"
export PYSPARK_PYTHON="$VENV_PYTHON"
export PYSPARK_DRIVER_PYTHON="$VENV_PYTHON"

echo "Running test suite with Java 17 and Python $(${VENV_PYTHON} --version)"
"$PYTEST" tests/ -v \
  --html=tests/report.html \
  --self-contained-html \
  "$@"

echo ""
echo "HTML report saved to: tests/report.html"
