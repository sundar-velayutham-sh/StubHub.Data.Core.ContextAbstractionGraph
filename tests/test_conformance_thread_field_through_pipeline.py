"""Conformance tests for thread-field-through-pipeline workflow.

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


class TestThreadFieldConformance:
    """Validate that thread-field-through-pipeline assembles correct context per step."""

    WORKFLOW_ID = "thread-field-through-pipeline"
    INPUTS = {"column_name": "VENUE_CAPACITY", "source_model": "stg_venues"}

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

        # Dummy outputs to advance through steps
        step_outputs = {
            "resolve_source_column": {
                "model_name": "stg_venues",
                "model_path": "models/staging/ticketing/stg_venues.sql",
                "table_fqn": "DW.STAGING.STG_VENUES",
                "column_info": {
                    "name": "VENUE_CAPACITY",
                    "sf_type": "NUMBER(10,0)",
                    "nullable": True,
                    "sample_values": [20000, 45000, 5500],
                },
            },
            "trace_pipeline_lineage": {
                "models_in_chain": [
                    {"model_name": "stg_venues", "model_path": "models/staging/ticketing/stg_venues.sql", "layer": "staging", "existing_columns": ["VENUE_ID", "VENUE_NAME"], "materialization": "view"},
                    {"model_name": "int_venues_enriched", "model_path": "models/intermediate/ticketing/int_venues_enriched.sql", "layer": "intermediate", "existing_columns": ["VENUE_ID", "VENUE_NAME"], "materialization": "table"},
                    {"model_name": "fct_event_sales", "model_path": "models/marts/core/fct_event_sales.sql", "layer": "fact", "existing_columns": ["EVENT_SALE_ID", "VENUE_ID"], "materialization": "incremental"},
                ],
                "chain_length": 3,
            },
            "modify_each_model": {
                "model_name": "stg_venues",
                "modified_sql": "SELECT venue_id, venue_name, venue_capacity FROM source",
                "changes_description": "Added venue_capacity",
            },
            "update_each_schema": {
                "model_name": "stg_venues",
                "schema_yml_content": "version: 2\nmodels:\n  - name: stg_venues",
                "tests_added": [],
            },
            "validate_pipeline": {
                "compile_ok": True,
                "tests_ok": True,
                "errors": [],
            },
        }

        visited_steps = []

        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            step_id = request.step_id

            # Check conformance spec for this logical step
            if step_id in conformance["steps"]:
                spec = conformance["steps"][step_id]
                expected_type = type_map[spec["type"]]
                assert isinstance(request, expected_type), (
                    f"Step '{step_id}': expected {spec['type']}, got {type(request).__name__}"
                )

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
                    if "has_instruction" in spec and spec["has_instruction"]:
                        assert request.instruction and len(request.instruction.strip()) > 0, (
                            f"Step '{step_id}': expected non-empty instruction"
                        )
                    if "knowledge_includes" in spec:
                        for kid in spec["knowledge_includes"]:
                            assert kid in request.context.static, (
                                f"Step '{step_id}': missing knowledge '{kid}' in static context. Has: {list(request.context.static.keys())}"
                            )

                if isinstance(request, DelegateRequest):
                    if "requires_approval" in spec:
                        assert request.requires_approval == spec["requires_approval"], (
                            f"Step '{step_id}': requires_approval mismatch"
                        )

            visited_steps.append(step_id)

            # Record dummy results to advance
            if isinstance(request, ReasonRequest):
                output = step_outputs.get(step_id, {"placeholder": True})
                run.record_result(step_id, StepSuccess(output=output))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"approved": True}))

        # Verify all conformance steps were visited
        for step_id in conformance["steps"]:
            assert step_id in visited_steps, (
                f"Conformance step '{step_id}' was never visited"
            )

    def test_conformance_covers_all_steps(self, engine, conformance):
        """Ensure conformance spec covers every logical step in the workflow."""
        from dcag._loaders import WorkflowLoader
        wf = WorkflowLoader(CONTENT_DIR / "workflows").load(self.WORKFLOW_ID)
        workflow_steps = {s.id for s in wf.steps}
        conformance_steps = set(conformance["steps"].keys())
        assert workflow_steps == conformance_steps, (
            f"Conformance spec mismatch. "
            f"In workflow but not conformance: {workflow_steps - conformance_steps}. "
            f"In conformance but not workflow: {conformance_steps - workflow_steps}"
        )
