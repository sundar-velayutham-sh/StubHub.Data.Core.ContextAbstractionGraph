"""Conformance tests for create-etl-pipeline workflow.

Validates context assembly per step WITHOUT LLM, using .test.yml spec.
Tests the new_source path through conditional branching.
"""
import json
import yaml
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.types import (
    ReasonRequest,
    DelegateRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"
CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-new-source"


def load_conformance(workflow_id: str) -> dict:
    path = CONTENT_DIR / "workflows" / f"{workflow_id}.test.yml"
    with open(path) as f:
        return yaml.safe_load(f)["conformance"]


def load_cassette(step_id: str) -> dict:
    path = CASSETTE_DIR / f"{step_id}.json"
    with open(path) as f:
        return json.load(f)


class TestCreateEtlPipelineConformance:
    """Validate that create-etl-pipeline assembles correct context per step.

    Walks the new_source branch to validate conformance for the primary path.
    """

    WORKFLOW_ID = "create-etl-pipeline"
    INPUTS = {"request_text": "Build a pipeline for fivetran_database.tiktok_ads.campaign_report"}

    @pytest.fixture
    def conformance(self):
        return load_conformance(self.WORKFLOW_ID)

    @pytest.fixture
    def engine(self):
        return DCAGEngine(content_dir=CONTENT_DIR)

    def test_all_steps_on_new_source_path_match_spec(self, engine, conformance):
        """Walk the new_source branch and verify each step matches conformance spec."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)
        type_map = {
            "ReasonRequest": ReasonRequest,
            "DelegateRequest": DelegateRequest,
        }

        # Steps that execute on new_source path
        # generate_models runs twice (2 models in design_pipeline)
        new_source_path = [
            "setup_environment",
            "classify_intent",
            "discover_source_schema",
            "profile_source_data",
            "discover_reference_patterns",
            "design_pipeline",
            "confirm_plan",
            "generate_models",
            "generate_models",
            "validate_pipeline",
            "recommend_tests",
            "show_results",
            "create_pr",
            "recommend_orchestration",
        ]

        # Cassette outputs for each reason step
        reason_steps = [
            "setup_environment",
            "classify_intent",
            "discover_source_schema",
            "profile_source_data",
            "discover_reference_patterns",
            "design_pipeline",
            "generate_models",
            "validate_pipeline",
            "recommend_tests",
            "recommend_orchestration",
        ]
        step_outputs = {}
        for step_id in reason_steps:
            step_outputs[step_id] = load_cassette(step_id)["output"]

        for step_id in new_source_path:
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
                if step_id == "confirm_plan":
                    run.record_result(step_id, StepSuccess(output={"user_decision": "approve", "feedback": ""}))
                elif step_id == "show_results":
                    run.record_result(step_id, StepSuccess(output={"user_decision": "approve", "edit_request": "", "edit_count": 0}))
                elif step_id == "create_pr":
                    run.record_result(step_id, StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer/pull/123", "pr_number": 123}))

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
