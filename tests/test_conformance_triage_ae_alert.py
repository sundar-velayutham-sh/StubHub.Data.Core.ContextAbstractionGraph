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

    Walks the code_error branch (invalid_identifier classification) to
    validate conformance for the branching path.
    """

    WORKFLOW_ID = "triage-ae-alert"
    INPUTS = {
        "alert_text": "FIRING: dbt model stg_ticket_listing failed — invalid identifier 'TICKET_PRICE'",
        "channel_id": "C0123456789",
        "thread_ts": "1710000000.000000",
    }

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
                "model_name": "stg_ticket_listing",
                "dag_name": "core_data_dbt",
                "task_name": "run_stg_ticket_listing",
                "error_message": "SQL compilation error: invalid identifier 'TICKET_PRICE'",
                "error_code": "002003",
                "priority": "critical",
                "alert_url": "https://incident.io/alerts/12345",
            },
            "check_failure_history": {
                "is_recurring": False,
                "failure_count_7d": 1,
                "first_failure": "2026-03-12T08:00:00Z",
                "last_success": "2026-03-12T04:00:00Z",
                "pattern": "new",
            },
            "check_cascade": {
                "is_cascade": False,
                "cascade_count": 1,
                "root_model": "stg_ticket_listing",
                "affected_models": ["stg_ticket_listing"],
            },
            "get_model_context": {
                "model_sql": "SELECT TICKET_PRICE FROM {{ source('raw', 'ticket_listing') }}",
                "owner": "data-eng",
                "tags": ["core", "daily"],
                "materialization": "view",
                "upstream_models": [],
                "downstream_models": ["int_ticket_sales"],
                "recent_commits": [{"sha": "abc123", "message": "rename column", "date": "2026-03-12"}],
                "model_path": "models/staging/ticket/stg_ticket_listing.sql",
            },
            "classify_alert": {
                "classification": "invalid_identifier",
                "confidence": "high",
                "rationale": "Error message contains 'invalid identifier'",
            },
            "diagnose_code_error": {
                "root_cause": "Column TICKET_PRICE was renamed to LISTING_PRICE in upstream source",
                "proposed_fix": "Update column reference from TICKET_PRICE to LISTING_PRICE",
                "diagnostic_queries_run": [
                    "SELECT COLUMN_NAME FROM DW.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TICKET_LISTING'"
                ],
            },
            "determine_resolution": {
                "resolution_type": "fix_directly",
                "suggested_actions": ["Create PR to rename TICKET_PRICE to LISTING_PRICE", "Run dbt test after merge"],
                "who_to_tag": "data-eng",
                "urgency": "in-hours",
            },
            "generate_triage_report": {
                "thread_summary": "TRIAGE: stg_ticket_listing — invalid_identifier\nRoot cause: Column renamed upstream",
                "investigation_report": "# Investigation Report\n\n## Error Details\n...",
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
                run.record_result(step_id, StepSuccess(output={"posted": True}))

    def test_data_issue_branch_path(self, engine, conformance):
        """Walk the data_issue branch (duplicate_row classification)."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)

        step_outputs = {
            "parse_alert": {
                "model_name": "fct_order",
                "dag_name": "core_data_dbt",
                "task_name": "run_fct_order",
                "error_message": "Duplicate row detected in merge",
                "error_code": None,
                "priority": "critical",
                "alert_url": "https://incident.io/alerts/12346",
            },
            "check_failure_history": {
                "is_recurring": False,
                "failure_count_7d": 1,
                "first_failure": "2026-03-12T08:00:00Z",
                "last_success": "2026-03-12T04:00:00Z",
                "pattern": "new",
            },
            "check_cascade": {
                "is_cascade": False,
                "cascade_count": 1,
                "root_model": "fct_order",
                "affected_models": ["fct_order"],
            },
            "get_model_context": {
                "model_sql": "SELECT * FROM {{ ref('int_order') }}",
                "owner": "analytics",
                "tags": ["core"],
                "materialization": "incremental",
                "upstream_models": ["int_order"],
                "downstream_models": ["rpt_daily_orders"],
                "recent_commits": [],
                "model_path": "models/marts/fct_order.sql",
            },
            "classify_alert": {
                "classification": "duplicate_row",
                "confidence": "high",
                "rationale": "Error message contains 'Duplicate row'",
            },
            "diagnose_data_issue": {
                "root_cause": "Upstream backfill introduced duplicate order_ids",
                "diagnostic_queries_run": ["SELECT order_id, COUNT(*) FROM fct_order GROUP BY 1 HAVING COUNT(*) > 1"],
                "affected_row_count": 42,
            },
            "determine_resolution": {
                "resolution_type": "escalate_to_owner",
                "suggested_actions": ["Tag analytics team", "Deduplicate upstream"],
                "who_to_tag": "analytics",
                "urgency": "in-hours",
            },
            "generate_triage_report": {
                "thread_summary": "TRIAGE: fct_order — duplicate_row",
                "investigation_report": "# Investigation\n...",
            },
        }

        # Data issue path
        data_issue_path = [
            "parse_alert",
            "check_failure_history",
            "check_cascade",
            "get_model_context",
            "classify_alert",
            "diagnose_data_issue",
            "determine_resolution",
            "generate_triage_report",
            "post_to_thread",
        ]

        for step_id in data_issue_path:
            request = run.next_step()
            assert request is not None, f"Workflow ended before step '{step_id}'"
            assert request.step_id == step_id, f"Expected step '{step_id}', got '{request.step_id}'"

            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=step_outputs.get(step_id, {"placeholder": True})))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"posted": True}))

    def test_infrastructure_branch_path(self, engine, conformance):
        """Walk the infrastructure branch (internal_error classification)."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)

        step_outputs = {
            "parse_alert": {
                "model_name": "fct_event_sales",
                "dag_name": "core_data_dbt",
                "task_name": "run_fct_event_sales",
                "error_message": "Processing aborted due to internal error",
                "error_code": "300005",
                "priority": "warning",
                "alert_url": "https://incident.io/alerts/12347",
            },
            "check_failure_history": {
                "is_recurring": True,
                "failure_count_7d": 4,
                "first_failure": "2026-03-06T08:00:00Z",
                "last_success": "2026-03-12T04:00:00Z",
                "pattern": "intermittent",
            },
            "check_cascade": {
                "is_cascade": True,
                "cascade_count": 5,
                "root_model": "fct_event_sales",
                "affected_models": ["fct_event_sales", "rpt_daily_sales", "rpt_venue_perf", "dim_event_agg", "rpt_weekly"],
            },
            "get_model_context": {
                "model_sql": "SELECT * FROM {{ ref('int_event_sales') }}",
                "owner": "data-eng",
                "tags": ["core", "daily"],
                "materialization": "incremental",
                "upstream_models": ["int_event_sales"],
                "downstream_models": ["rpt_daily_sales"],
                "recent_commits": [],
                "model_path": "models/marts/fct_event_sales.sql",
            },
            "classify_alert": {
                "classification": "internal_error",
                "confidence": "high",
                "rationale": "Error code 300005 is a known Snowflake internal error",
            },
            "diagnose_infrastructure": {
                "root_cause": "Snowflake transient internal error 300005",
                "is_transient": True,
                "diagnostic_queries_run": ["SELECT * FROM QUERY_HISTORY WHERE ..."],
            },
            "determine_resolution": {
                "resolution_type": "acknowledge_transient",
                "suggested_actions": ["Acknowledge in thread", "Monitor next run"],
                "who_to_tag": "data-eng",
                "urgency": "low",
            },
            "generate_triage_report": {
                "thread_summary": "TRIAGE: fct_event_sales — internal_error (transient)",
                "investigation_report": "# Investigation\n...",
            },
        }

        infra_path = [
            "parse_alert",
            "check_failure_history",
            "check_cascade",
            "get_model_context",
            "classify_alert",
            "diagnose_infrastructure",
            "determine_resolution",
            "generate_triage_report",
            "post_to_thread",
        ]

        for step_id in infra_path:
            request = run.next_step()
            assert request is not None, f"Workflow ended before step '{step_id}'"
            assert request.step_id == step_id, f"Expected step '{step_id}', got '{request.step_id}'"

            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=step_outputs.get(step_id, {"placeholder": True})))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"posted": True}))

    def test_known_issue_branch_path(self, engine, conformance):
        """Walk the known_issue branch (recurring_transient classification)."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)

        step_outputs = {
            "parse_alert": {
                "model_name": "rpt_marketshare",
                "dag_name": "analytics_dbt",
                "task_name": "run_rpt_marketshare",
                "error_message": "Got 3 results, configured to fail if != 0",
                "error_code": None,
                "priority": "warning",
                "alert_url": "https://incident.io/alerts/12348",
            },
            "check_failure_history": {
                "is_recurring": True,
                "failure_count_7d": 5,
                "first_failure": "2026-03-06T02:00:00Z",
                "last_success": "2026-03-11T02:00:00Z",
                "pattern": "recurring",
            },
            "check_cascade": {
                "is_cascade": False,
                "cascade_count": 1,
                "root_model": "rpt_marketshare",
                "affected_models": ["rpt_marketshare"],
            },
            "get_model_context": {
                "model_sql": "SELECT * FROM {{ ref('int_marketshare') }}",
                "owner": "analytics",
                "tags": ["analytics"],
                "materialization": "table",
                "upstream_models": ["int_marketshare"],
                "downstream_models": [],
                "recent_commits": [],
                "model_path": "models/marts/rpt_marketshare.sql",
            },
            "classify_alert": {
                "classification": "recurring_transient",
                "confidence": "high",
                "rationale": "Same test failure 5 times in 7 days, self-resolves on rerun",
            },
            "diagnose_known_issue": {
                "root_cause": "Flaky test on rpt_marketshare — timing-dependent row count",
                "is_self_resolved": True,
                "should_suppress": True,
                "recommendation": "Disable test or increase threshold",
            },
            "determine_resolution": {
                "resolution_type": "recommend_suppression",
                "suggested_actions": ["Discuss suppression with analytics team", "Disable test temporarily"],
                "who_to_tag": "analytics",
                "urgency": "low",
            },
            "generate_triage_report": {
                "thread_summary": "TRIAGE: rpt_marketshare — recurring_transient",
                "investigation_report": "# Investigation\n...",
            },
        }

        known_issue_path = [
            "parse_alert",
            "check_failure_history",
            "check_cascade",
            "get_model_context",
            "classify_alert",
            "diagnose_known_issue",
            "determine_resolution",
            "generate_triage_report",
            "post_to_thread",
        ]

        for step_id in known_issue_path:
            request = run.next_step()
            assert request is not None, f"Workflow ended before step '{step_id}'"
            assert request.step_id == step_id, f"Expected step '{step_id}', got '{request.step_id}'"

            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=step_outputs.get(step_id, {"placeholder": True})))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"posted": True}))

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
