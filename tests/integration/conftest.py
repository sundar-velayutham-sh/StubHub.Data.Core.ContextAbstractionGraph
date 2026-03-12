"""Fixtures and markers for live integration tests."""
import os
import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "live: requires ANTHROPIC_API_KEY and real LLM calls")


def pytest_collection_modifyitems(config, items):
    run_live = (
        config.getoption("--run-live", default=False)
        or os.environ.get("DCAG_LIVE_TEST") == "1"
    )
    if not run_live:
        skip_live = pytest.mark.skip(reason="Live tests require --run-live or DCAG_LIVE_TEST=1")
        for item in items:
            if "live" in item.keywords:
                item.add_marker(skip_live)


def pytest_addoption(parser):
    parser.addoption("--run-live", action="store_true", default=False, help="Run live integration tests")
