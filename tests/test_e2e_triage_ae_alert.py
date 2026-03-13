"""
End-to-end test for the triage-ae-alert workflow.

Tests the full 9-step workflow with CONDITIONAL BRANCHING at classify_alert.
Four test classes exercise different branch paths:
  - TestTriageCodeError: classify -> diagnose_code_error -> determine_resolution
  - TestTriageDataIssue: classify -> diagnose_data_issue -> determine_resolution
  - TestTriageInfrastructure: classify -> diagnose_infrastructure -> determine_resolution
  - TestTriageKnownIssue: classify -> diagnose_known_issue -> determine_resolution
"""
import json
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.types import (
    ReasonRequest,
    DelegateRequest,
    StepSuccess,
)


CONTENT_DIR = Path(__file__).parent.parent / "content"

# Steps for code_error path (skips diagnose_data_issue, diagnose_infrastructure, diagnose_known_issue)
CODE_ERROR_STEPS = [
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

# Steps for data_issue path (skips diagnose_code_error, diagnose_infrastructure, diagnose_known_issue)
DATA_ISSUE_STEPS = [
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

# Steps for infrastructure path (skips diagnose_code_error, diagnose_data_issue, diagnose_known_issue)
INFRASTRUCTURE_STEPS = [
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

# Steps for known_issue path (skips diagnose_code_error, diagnose_data_issue, diagnose_infrastructure)
KNOWN_ISSUE_STEPS = [
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


def load_cassettes(cassette_dir: Path, reason_steps: list[str]) -> dict[str, dict]:
    """Load cassettes for the steps that will actually execute."""
    cassettes = {}
    for step_id in reason_steps:
        path = cassette_dir / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(
    cassette_dir: Path,
    inputs: dict,
    reason_steps: list[str],
) -> tuple:
    """Drive the workflow with cassette responses, handling branching."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes(cassette_dir, reason_steps)

    run = engine.start("triage-ae-alert", inputs)
    assert run.status == "running"

    steps_executed = []
    reason_outputs = {}

    while run.status == "running":
        request = run.next_step()
        if request is None:
            break

        steps_executed.append(request.step_id)

        if isinstance(request, ReasonRequest):
            cassette = cassettes[request.step_id]
            reason_outputs[request.step_id] = cassette["output"]
            run.record_result(
                request.step_id,
                StepSuccess(output=cassette["output"]),
            )

        elif isinstance(request, DelegateRequest):
            if request.step_id == "post_to_thread":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"posted": True, "thread_ts": "1710230400.000100"}),
                )

    return run, steps_executed, reason_outputs


class TestTriageCodeError:
    """Code error branch: parse -> history -> cascade -> context -> classify -> diagnose_code_error -> resolve -> report -> post."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "triage-ae-alert-code-error"
    INPUTS = {"alert_text": "cs_chatbot_conversation_agg failed in transform_cx__daily: SQL compilation error: invalid identifier 'CONVERSATIONID'"}
    REASON_STEPS = [s for s in CODE_ERROR_STEPS if s != "post_to_thread"]

    def test_workflow_takes_code_error_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == CODE_ERROR_STEPS

    def test_code_error_path_has_9_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 9

    def test_classify_returns_invalid_identifier(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_alert"]["classification"] == "invalid_identifier"

    def test_resolution_type_is_acknowledge_transient(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["determine_resolution"]["resolution_type"] == "acknowledge_transient"

    def test_report_has_thread_summary_and_investigation(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        report = reason_outputs["generate_triage_report"]
        assert "thread_summary" in report
        assert "investigation_report" in report
        assert "cs_chatbot_conversation_agg" in report["thread_summary"]

    def test_skips_other_diagnose_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "diagnose_data_issue" not in steps_executed
        assert "diagnose_infrastructure" not in steps_executed
        assert "diagnose_known_issue" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "triage-ae-alert"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "diagnose_code_error" in step_ids
        assert "diagnose_data_issue" not in step_ids
        assert "diagnose_infrastructure" not in step_ids
        assert "diagnose_known_issue" not in step_ids

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("triage-ae-alert", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "parse_alert"
        assert request.persona.id == "analytics_engineer"
        assert len(request.tools) > 0


class TestTriageDataIssue:
    """Data issue branch: parse -> history -> cascade -> context -> classify -> diagnose_data_issue -> resolve -> report -> post."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "triage-ae-alert-data-issue"
    INPUTS = {"alert_text": "gpm_repeat_buyer_dim failed in transform_pricing__daily: Duplicate row detected during DML action (100090)"}
    REASON_STEPS = [s for s in DATA_ISSUE_STEPS if s != "post_to_thread"]

    def test_workflow_takes_data_issue_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == DATA_ISSUE_STEPS

    def test_data_issue_path_has_9_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 9

    def test_classify_returns_duplicate_row(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_alert"]["classification"] == "duplicate_row"

    def test_resolution_type_is_escalate_to_owner(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["determine_resolution"]["resolution_type"] == "escalate_to_owner"

    def test_report_has_thread_summary_and_investigation(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        report = reason_outputs["generate_triage_report"]
        assert "thread_summary" in report
        assert "investigation_report" in report
        assert "gpm_repeat_buyer_dim" in report["thread_summary"]

    def test_cascade_detected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["check_cascade"]["is_cascade"] is True
        assert reason_outputs["check_cascade"]["cascade_count"] == 2

    def test_affected_row_count_present(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["diagnose_data_issue"]["affected_row_count"] == 436168

    def test_skips_other_diagnose_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "diagnose_code_error" not in steps_executed
        assert "diagnose_infrastructure" not in steps_executed
        assert "diagnose_known_issue" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "diagnose_data_issue" in step_ids
        assert "diagnose_code_error" not in step_ids


class TestTriageInfrastructure:
    """Infrastructure branch: parse -> history -> cascade -> context -> classify -> diagnose_infrastructure -> resolve -> report -> post."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "triage-ae-alert-infrastructure"
    INPUTS = {"alert_text": "seller_event_day_listing_agg failed in transform_core__hourly: SQL execution internal error: Processing aborted due to error 300005"}
    REASON_STEPS = [s for s in INFRASTRUCTURE_STEPS if s != "post_to_thread"]

    def test_workflow_takes_infrastructure_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == INFRASTRUCTURE_STEPS

    def test_infrastructure_path_has_9_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 9

    def test_classify_returns_internal_error(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_alert"]["classification"] == "internal_error"

    def test_resolution_type_is_acknowledge_transient(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["determine_resolution"]["resolution_type"] == "acknowledge_transient"

    def test_is_transient_flag(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["diagnose_infrastructure"]["is_transient"] is True

    def test_report_has_thread_summary_and_investigation(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        report = reason_outputs["generate_triage_report"]
        assert "thread_summary" in report
        assert "investigation_report" in report
        assert "seller_event_day_listing_agg" in report["thread_summary"]

    def test_skips_other_diagnose_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "diagnose_code_error" not in steps_executed
        assert "diagnose_data_issue" not in steps_executed
        assert "diagnose_known_issue" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "diagnose_infrastructure" in step_ids
        assert "diagnose_code_error" not in step_ids


class TestTriageKnownIssue:
    """Known issue branch: parse -> history -> cascade -> context -> classify -> diagnose_known_issue -> resolve -> report -> post."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "triage-ae-alert-known-issue"
    INPUTS = {"alert_text": "apex_participation_matches_participation_fact_enriched_12 failed in monitor__severe_tests__hourly: Got 436168 results, configured to fail if != 0"}
    REASON_STEPS = [s for s in KNOWN_ISSUE_STEPS if s != "post_to_thread"]

    def test_workflow_takes_known_issue_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == KNOWN_ISSUE_STEPS

    def test_known_issue_path_has_9_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 9

    def test_classify_returns_recurring_transient(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_alert"]["classification"] == "recurring_transient"

    def test_resolution_type_is_recommend_suppression(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["determine_resolution"]["resolution_type"] == "recommend_suppression"

    def test_should_suppress_flag(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["diagnose_known_issue"]["should_suppress"] is True

    def test_report_has_thread_summary_and_investigation(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        report = reason_outputs["generate_triage_report"]
        assert "thread_summary" in report
        assert "investigation_report" in report
        assert "recurring_transient" in report["thread_summary"]

    def test_skips_other_diagnose_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "diagnose_code_error" not in steps_executed
        assert "diagnose_data_issue" not in steps_executed
        assert "diagnose_infrastructure" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "diagnose_known_issue" in step_ids
        assert "diagnose_code_error" not in step_ids
        assert "diagnose_infrastructure" not in step_ids
