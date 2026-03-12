"""Conformance tests — validate context assembly per step without LLM.

Reads .test.yml alongside the workflow YAML, walks the workflow using the
real assembler + loaders, and asserts each step's ContextSnapshot matches
the conformance contract.
"""
import yaml
from pathlib import Path
import pytest

from dcag import DCAGEngine
from dcag.types import (
    ReasonRequest,
    ExecuteScriptRequest,
    DelegateRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"


def load_conformance(workflow_id: str) -> dict:
    """Load the .test.yml conformance spec for a workflow."""
    path = CONTENT_DIR / "workflows" / f"{workflow_id}.test.yml"
    with open(path) as f:
        return yaml.safe_load(f)["conformance"]


class TestAddColumnConformance:
    """Validate that add-column-to-model assembles correct context per step."""

    WORKFLOW_ID = "add-column-to-model"
    INPUTS = {"model_name": "stg_pricing_analytics_events", "column_name": "pcid"}

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
            "ExecuteScriptRequest": ExecuteScriptRequest,
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
                if "tools_exclude" in spec:
                    tool_names = [t.name for t in request.tools]
                    for excluded_tool in spec["tools_exclude"]:
                        assert excluded_tool not in tool_names, (
                            f"Step '{step_id}': should NOT have tool '{excluded_tool}'"
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

            # Record a dummy result to advance the workflow
            if isinstance(request, ExecuteScriptRequest):
                run.record_result(step_id, StepSuccess(output={
                    "dbt_project_path": "/tmp/test",
                    "dbt_available": True,
                    "dbt_mcp_available": True,
                    "fallback_mode": "full",
                }))
            elif isinstance(request, ReasonRequest):
                # Provide outputs that satisfy downstream dynamic refs
                step_outputs = {
                    "resolve_model": {
                        "model_path": "models/staging/ecomm/stg_test.sql",
                        "sources_yml_path": "models/staging/ecomm/sources.yml",
                        "source_ref": "{{ source('ecomm', 'test') }}",
                        "existing_columns": ["COL_A"],
                        "source_table_fqn": "ECOMM.APP.TEST",
                    },
                    "discover_column": {
                        "column_info": {"name": "PCID", "source_type": "NUMBER", "sf_type": "NUMBER", "nullable": False},
                    },
                    "determine_logic": {
                        "intent_level": "passthrough", "column_expression": "PCID", "join_required": False,
                    },
                    "check_downstream_impact": {
                        "downstream_impact": {"affected_models": [], "select_star_risk": False, "impact_level": "safe"},
                    },
                    "modify_staging_sql": {
                        "modified_sql": "SELECT COL_A, PCID FROM src", "changes_made": ["added PCID"],
                    },
                    "update_schema_yml": {
                        "modified_yml": "- name: pcid", "changes_made": ["added pcid"],
                    },
                    "validate": {
                        "tests_passed": True, "compile_ok": True, "parse_ok": True,
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
