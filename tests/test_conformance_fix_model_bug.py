"""Conformance tests for fix-model-bug workflow.

Validates context assembly per step WITHOUT LLM, using .test.yml spec.
Tests the cast_error path through conditional branching.
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


class TestFixModelBugConformance:
    """Validate that fix-model-bug assembles correct context per step.

    Walks the cast_error branch to validate conformance for the branching path.
    """

    WORKFLOW_ID = "fix-model-bug"
    INPUTS = {"model_name": "stg_ticket_listing", "error_message": "Numeric value not recognized"}

    @pytest.fixture
    def conformance(self):
        return load_conformance(self.WORKFLOW_ID)

    @pytest.fixture
    def engine(self):
        return DCAGEngine(content_dir=CONTENT_DIR)

    def test_all_steps_on_cast_path_match_spec(self, engine, conformance):
        """Walk the cast_error branch and verify each step matches conformance spec."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)
        type_map = {
            "ReasonRequest": ReasonRequest,
            "DelegateRequest": DelegateRequest,
        }

        # Steps that execute on cast_error path
        cast_path = [
            "parse_error",
            "read_model_sql",
            "classify_bug_type",
            "fix_cast_error",
            "validate_fix",
            "create_pr",
        ]

        step_outputs = {
            "parse_error": {
                "error_type": "runtime",
                "failing_expression": "CAST(TICKET_PRICE AS NUMBER(18,2))",
                "source_table": "DW.STG.STG_TICKET_LISTING",
                "model_name": "stg_ticket_listing",
            },
            "read_model_sql": {
                "model_path": "models/staging/ticket/stg_ticket_listing.sql",
                "model_sql": "SELECT ...",
                "cte_structure": ["source", "final"],
                "source_refs": [],
                "materialization": "view",
            },
            "classify_bug_type": {
                "bug_type": "cast_error",
                "confidence": "high",
                "rationale": "Cast failure on TICKET_PRICE",
                "failing_component": "CAST(TICKET_PRICE AS NUMBER(18,2))",
            },
            "fix_cast_error": {
                "fixed_sql": "WITH source AS ... TRY_CAST ...",
                "fix_description": "Replaced CAST with TRY_CAST",
                "problematic_values": ["N/A", ""],
            },
            "validate_fix": {
                "compile_ok": True,
                "tests_passed": True,
                "preview_rows": [],
                "fix_verified": True,
            },
        }

        for step_id in cast_path:
            request = run.next_step()
            assert request is not None, f"Workflow ended before step '{step_id}'"
            assert request.step_id == step_id, f"Expected step '{step_id}', got '{request.step_id}'"

            spec = conformance["steps"][step_id]
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

            # Record results to advance
            if isinstance(request, ReasonRequest):
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
