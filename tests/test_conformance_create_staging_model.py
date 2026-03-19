"""Conformance tests for create-staging-model workflow.

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


class TestCreateStagingModelConformance:
    """Validate that create-staging-model assembles correct context per step."""

    WORKFLOW_ID = "create-staging-model"
    INPUTS = {"table_name": "PAYMENT_TRANSACTION", "source_system": "payment", "database": "DW", "schema": "RAW"}

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

        step_outputs = {
            "discover_source_table": {
                "table_fqn": "DW.RAW.PAYMENT_TRANSACTION",
                "columns": [
                    {"name": "PAYMENT_ID", "type": "NUMBER(38,0)", "nullable": False, "ordinal_position": 1},
                    {"name": "AMOUNT", "type": "NUMBER(18,2)", "nullable": False, "ordinal_position": 2},
                ],
                "row_count": 25000000,
                "size_bytes": 3200000000,
                "table_type": "BASE TABLE",
                "created_at": "2024-06-15T10:00:00Z",
            },
            "check_existing_models": {
                "model_exists": False,
                "existing_model_path": None,
                "sources_yml_exists": True,
                "source_entry_exists": False,
            },
            "choose_materialization": {
                "materialization": "incremental",
                "unique_key": "payment_id",
                "incremental_strategy": "merge",
                "rationale": "25M rows with UPDATED_AT column",
            },
            "generate_model_sql": {
                "model_sql": "{{ config(materialized='incremental') }}...",
                "model_filename": "stg_payment_transaction.sql",
                "column_count": 10,
                "config_block": "materialized='incremental'",
            },
            "generate_schema_yml": {
                "schema_yml_content": "version: 2...",
                "tests_added": [{"column": "payment_id", "tests": ["not_null", "unique"]}],
                "column_count": 10,
            },
            "add_to_sources_yml": {
                "sources_yml_content": "version: 2...",
                "is_new_source": False,
                "source_name": "payment",
            },
            "validate": {
                "compile_ok": True,
                "parse_ok": True,
                "errors": [],
            },
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
