"""Conformance tests for add-dbt-tests workflow.

Validates context assembly per step WITHOUT LLM, using .test.yml spec.
"""
from pathlib import Path

import pytest
import yaml

from dcag import DCAGEngine
from dcag.types import (
    DelegateRequest,
    ReasonRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"


def load_conformance(workflow_id: str) -> dict:
    path = CONTENT_DIR / "workflows" / f"{workflow_id}.test.yml"
    with open(path) as f:
        return yaml.safe_load(f)["conformance"]


class TestAddDbtTestsConformance:
    """Validate that add-dbt-tests assembles correct context per step."""

    WORKFLOW_ID = "add-dbt-tests"
    INPUTS = {"model_name": "dim_venue"}

    @pytest.fixture
    def conformance(self):
        return load_conformance(self.WORKFLOW_ID)

    @pytest.fixture
    def engine(self):
        return DCAGEngine(content_dir=CONTENT_DIR)

    def test_all_steps_match_expected_types(self, engine, conformance):
        """Walk the workflow and verify each step returns the expected request type."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)
        type_map = {
            "ReasonRequest": ReasonRequest,
            "DelegateRequest": DelegateRequest,
        }

        for step_id, spec in conformance["steps"].items():
            request = run.next_step()
            assert request is not None, f"Workflow ended before step '{step_id}'"
            assert request.step_id == step_id, f"Expected step '{step_id}', got '{request.step_id}'"

            expected_type = type_map[spec["type"]]
            assert isinstance(request, expected_type), (
                f"Step '{step_id}': expected {spec['type']}, got {type(request).__name__}"
            )

            # Validate ReasonRequest specifics
            if isinstance(request, ReasonRequest):
                if "persona" in spec:
                    assert request.persona.id == spec["persona"], (
                        f"Step '{step_id}': expected persona '{spec['persona']}', got '{request.persona.id}'"
                    )
                if "tools_include" in spec:
                    tool_names = [t.name for t in request.tools]
                    for expected_tool in spec["tools_include"]:
                        assert expected_tool in tool_names, (
                            f"Step '{step_id}': missing tool '{expected_tool}'. Has: {tool_names}"
                        )
                if "tools_count" in spec:
                    assert len(request.tools) == spec["tools_count"], (
                        f"Step '{step_id}': expected {spec['tools_count']} tools, got {len(request.tools)}"
                    )
                if "has_instruction" in spec and spec["has_instruction"]:
                    assert request.instruction and len(request.instruction.strip()) > 0, (
                        f"Step '{step_id}': expected non-empty instruction"
                    )
                if "knowledge_includes" in spec:
                    for kid in spec["knowledge_includes"]:
                        assert kid in request.context.static, (
                            f"Step '{step_id}': missing knowledge '{kid}' in static context. Has: {list(request.context.static.keys())}"
                        )

            # Validate DelegateRequest specifics
            if isinstance(request, DelegateRequest):
                if "requires_approval" in spec:
                    assert request.requires_approval == spec["requires_approval"], (
                        f"Step '{step_id}': requires_approval mismatch"
                    )

            # Record dummy results to advance
            if isinstance(request, ReasonRequest):
                step_outputs = {
                    "resolve_model": {
                        "model_path": "models/marts/core/dim_venue.sql",
                        "schema_yml_path": "models/marts/core/schema.yml",
                        "source_table_fqn": "DW.CORE.VENUE_DIM",
                        "existing_tests": [
                            {"column": "venue_id", "tests": ["not_null", "unique"]}
                        ],
                        "columns_without_tests": ["city", "state", "country"],
                    },
                    "get_column_metadata": {
                        "column_metadata": [
                            {"name": "VENUE_ID", "sf_type": "NUMBER(38,0)", "nullable": False, "null_pct": 0.0, "cardinality": 15000, "row_count": 15000}
                        ],
                    },
                    "infer_tests": {
                        "new_tests": [
                            {"column": "city", "test_type": "not_null", "rationale": "Non-nullable column"}
                        ],
                        "total_tests_to_add": 8,
                        "columns_affected": ["city", "country", "venue_type", "created_at", "is_active"],
                    },
                    "update_schema_yml": {
                        "modified_yml": "version: 2\nmodels:\n  - name: dim_venue\n",
                        "tests_added_count": 8,
                        "columns_modified": ["city", "country", "venue_type", "created_at", "is_active"],
                    },
                    "validate": {
                        "parse_ok": True,
                        "tests_passed": 11,
                        "tests_failed": 0,
                        "errors": [],
                    },
                }
                run.record_result(step_id, StepSuccess(output=step_outputs.get(step_id, {"placeholder": True})))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"approved": True}))

    def test_conformance_covers_all_steps(self, engine, conformance):
        """Ensure conformance spec covers every step in the workflow."""
        from dcag._loaders import WorkflowLoader
        wf = WorkflowLoader(CONTENT_DIR / "workflows").load(self.WORKFLOW_ID)
        workflow_steps = {s.id for s in wf.steps}
        conformance_steps = set(conformance["steps"].keys())
        assert workflow_steps == conformance_steps, (
            f"Conformance spec mismatch. "
            f"In workflow but not conformance: {workflow_steps - conformance_steps}. "
            f"In conformance but not workflow: {conformance_steps - workflow_steps}"
        )
