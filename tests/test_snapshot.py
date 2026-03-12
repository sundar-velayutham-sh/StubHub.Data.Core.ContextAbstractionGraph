"""Tests for context assembly snapshots."""
from dcag._snapshot import ContextSnapshot


class TestContextSnapshot:
    def test_frozen(self):
        snap = ContextSnapshot(
            step_id="resolve_model",
            persona="analytics_engineer",
            knowledge=("dbt_project_structure", "repo_registry"),
            tools=("dbt_mcp.get_node_details_dev", "github_cli.search_code"),
            prior_outputs=(),
            instruction="Find the dbt model...",
            estimated_tokens=1500,
        )
        assert snap.step_id == "resolve_model"
        assert len(snap.knowledge) == 2
        assert snap.estimated_tokens == 1500

    def test_immutable(self):
        snap = ContextSnapshot(
            step_id="test", persona="ae", knowledge=(), tools=(),
            prior_outputs=(), instruction="", estimated_tokens=0,
        )
        import pytest
        with pytest.raises(AttributeError):
            snap.step_id = "modified"  # type: ignore

    def test_serializable(self):
        """Snapshots must be JSON-serializable for observability events."""
        import json
        from dataclasses import asdict
        snap = ContextSnapshot(
            step_id="test", persona="ae",
            knowledge=("k1", "k2"), tools=("t1",),
            prior_outputs=("step_a",), instruction="do thing",
            estimated_tokens=500,
        )
        data = asdict(snap)
        json_str = json.dumps(data)
        assert "test" in json_str

    def test_shift_suggestion_fields(self):
        """Shift suggestion #1: workflow_inputs and fallback_mode fields."""
        snap = ContextSnapshot(
            step_id="test", persona="ae", knowledge=(), tools=(),
            prior_outputs=(), instruction="", estimated_tokens=0,
            workflow_inputs={"model_name": "stg_test", "column_name": "col"},
            fallback_mode="snowflake_only",
        )
        assert snap.workflow_inputs["model_name"] == "stg_test"
        assert snap.fallback_mode == "snowflake_only"
