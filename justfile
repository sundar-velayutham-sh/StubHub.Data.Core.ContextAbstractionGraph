# DCAG development commands

# Install package with dev dependencies
setup:
    pip install -e ".[dev]"

# Run all tests
test:
    pytest

# Run tests with coverage report
test-cov:
    pytest --cov=dcag --cov-report=term-missing

# Lint source and tests
lint:
    ruff check src/ tests/

# Format source and tests
fmt:
    ruff format src/ tests/

# Start API server (development)
api:
    uvicorn dcag.api:app --reload --host 0.0.0.0 --port ${DCAG_API_PORT:-8321}

# Run conformance tests only
test-conformance:
    pytest tests/test_conformance_*.py

# Run e2e tests only
test-e2e:
    pytest tests/test_e2e_*.py

# Pre-push check: lint + test
check: lint test
