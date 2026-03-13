"""Conformance tests for triage-ae-alert workflow.

Validates context assembly per step WITHOUT LLM, using .test.yml spec.
Tests the code_error path through conditional branching.
"""
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


def load_conformance(workflow_id: str) -> dict:
    path = CONTENT_DIR / "workflows" / f"{workflow_id}.test.yml"
    with open(path) as f:
        return yaml.safe_load(f)["conformance"]


class TestTriageAeAlertConformance:
    """Validate that triage-ae-alert assembles correct context per step.

    Walks the code_error branch to validate conformance for the branching path.
    """

    WORKFLOW_ID = "triage-ae-alert"
    INPUTS = {"alert_text": "cs_chatbot_conversation_agg failed: SQL compilation error: invalid identifier 'CONVERSATIONID'"}

    @pytest.fixture
    def conformance(self):
        return load_conformance(self.WORKFLOW_ID)

    @pytest.fixture
    def engine(self):
        return DCAGEngine(content_dir=CONTENT_DIR)

    def test_all_steps_on_code_error_path_match_spec(self, engine, conformance):
        """Walk the code_error branch and verify each step matches conformance spec."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)
        type_map = {
            "ReasonRequest": ReasonRequest,
            "DelegateRequest": DelegateRequest,
        }

        # Steps that execute on code_error path
        code_error_path = [
            "parse_alert",
            "check_failure_history",
            "check_cascade",
            "get_model_context",
            "classify_alert",
            "diagnose_code_error",
            "determine_resolution",
            "generate_triage_report",
            "post_to_thread",
        ]

        step_outputs = {
            "parse_alert": {
                "model_name": "cs_chatbot_conversation_agg",
                "dag_name": "transform_cx__daily",
                "task_name": "cs_chatbot_conversation_agg",
                "error_message": "SQL compilation error: invalid identifier 'CONVERSATIONID'",
                "error_code": "000904",
                "priority": "In-hours",
            },
            "check_failure_history": {
                "is_recurring": True,
                "failure_count_7d": 10,
                "first_failure": "2026-03-03",
                "last_success": "2026-03-02",
                "pattern": "recurring",
            },
            "check_cascade": {
                "is_cascade": False,
                "cascade_count": 0,
                "root_model": "cs_chatbot_conversation_agg",
                "affected_models": [],
            },
            "get_model_context": {
                "model_sql": "WITH conversation_details AS ...",
                "owner": "@corinne.smallwood",
                "tags": ["cs", "semidaily"],
                "materialization": "table",
                "upstream_models": ["dw_nlp.llm_function_call_data"],
                "downstream_models": ["cs_chatbot_metrics"],
                "recent_commits": [],
                "model_path": "models/mart/ops/cs/cs_chatbot_conversation_agg.sql",
            },
            "classify_alert": {
                "classification": "invalid_identifier",
                "confidence": "high",
                "rationale": "Error contains invalid identifier pattern",
            },
            "diagnose_code_error": {
                "root_cause": "Column CONVERSATIONID temporarily unavailable",
                "proposed_fix": "Add source freshness test",
                "diagnostic_queries_run": ["INFORMATION_SCHEMA.COLUMNS check"],
            },
            "determine_resolution": {
                "resolution_type": "acknowledge_transient",
                "suggested_actions": ["Verify column exists now"],
                "who_to_tag": "@corinne.smallwood",
                "urgency": "low",
            },
            "generate_triage_report": {
                "thread_summary": "TRIAGE: cs_chatbot_conversation_agg — invalid_identifier",
                "investigation_report": "# Full report...",
            },
        }

        for step_id in code_error_path:
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
