#!/bin/bash
set -euo pipefail

# DCAG Live Integration Test Runner
# Usage: ANTHROPIC_API_KEY=sk-... ./scripts/run_live_test.sh

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "Error: ANTHROPIC_API_KEY environment variable required"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

cd "$REPO_DIR"

# Create venv if needed
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi

source .venv/bin/activate
pip install -e ".[dev]" --quiet

# Run live tests
echo "Running DCAG live integration tests..."
DCAG_LIVE_TEST=1 pytest tests/integration/ -v --tb=short

echo "Done."
