"""Tests for decision trace persistence."""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from dcag._decisions import DecisionStore


@pytest.fixture
def store(tmp_path):
    """Create a DecisionStore with a temp directory."""
    return DecisionStore(tmp_path / "decisions")


class TestDecisionStore:
    """Tests for writing, loading, and searching decision traces."""

    def test_write_creates_file(self, store):
        """write() creates a JSON file in entity subdirectory."""
        store.write(
            run_id="dcag-abc123",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY", "keys": ["SALE_DATE"]},
            confidence="high",
        )
        entity_dir = store._base_dir / "DW.RPT.TRANSACTION"
        assert entity_dir.exists()
        files = list(entity_dir.glob("*.json"))
        assert len(files) == 1

    def test_write_content_structure(self, store):
        """Written JSON has expected structure."""
        store.write(
            run_id="dcag-abc123",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY"},
            confidence="high",
        )
        files = list((store._base_dir / "DW.RPT.TRANSACTION").glob("*.json"))
        data = json.loads(files[0].read_text())
        assert data["workflow"] == "table-optimizer"
        assert data["run_id"] == "dcag-abc123"
        assert data["entity"] == "DW.RPT.TRANSACTION"
        assert data["facts"]["strategy"] == "CLUSTER_BY"
        assert data["confidence"] == "high"
        assert "decided_at" in data

    def test_load_returns_decision(self, store):
        """load() reads back a written decision."""
        store.write(
            run_id="dcag-abc123",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY"},
            confidence="high",
        )
        decisions = store.load("DW.RPT.TRANSACTION")
        assert len(decisions) == 1
        assert decisions[0]["run_id"] == "dcag-abc123"

    def test_load_empty_entity(self, store):
        """load() returns empty list for unknown entity."""
        decisions = store.load("NONEXISTENT.TABLE")
        assert decisions == []

    def test_search_by_entity(self, store):
        """search_by_entity finds decisions for a given entity."""
        store.write(
            run_id="dcag-111",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY"},
            confidence="high",
        )
        store.write(
            run_id="dcag-222",
            workflow_id="add-column",
            entity="DW.RPT.TRANSACTION",
            facts={"column": "PCID"},
            confidence="medium",
        )
        store.write(
            run_id="dcag-333",
            workflow_id="table-optimizer",
            entity="DW.CORE.VENUE_DIM",
            facts={"strategy": "SKIP"},
            confidence="high",
        )
        results = store.search_by_entity("DW.RPT.TRANSACTION")
        assert len(results) == 2
        run_ids = {r["run_id"] for r in results}
        assert run_ids == {"dcag-111", "dcag-222"}

    def test_multiple_writes_same_entity(self, store):
        """Multiple writes to same entity create separate files."""
        for i in range(3):
            store.write(
                run_id=f"dcag-{i}",
                workflow_id="opt",
                entity="DW.RPT.T",
                facts={"i": i},
                confidence="high",
            )
        decisions = store.load("DW.RPT.T")
        assert len(decisions) == 3

    def test_write_with_valid_until(self, store):
        """valid_until field is persisted."""
        store.write(
            run_id="dcag-abc",
            workflow_id="opt",
            entity="DW.RPT.T",
            facts={},
            confidence="high",
            valid_until="2026-06-12",
        )
        decisions = store.load("DW.RPT.T")
        assert decisions[0]["valid_until"] == "2026-06-12"

    def test_base_dir_created_on_write(self, tmp_path):
        """Base directory is created if it doesn't exist."""
        deep_path = tmp_path / "a" / "b" / "decisions"
        store = DecisionStore(deep_path)
        store.write(
            run_id="dcag-x",
            workflow_id="w",
            entity="E",
            facts={},
            confidence="low",
        )
        assert deep_path.exists()
