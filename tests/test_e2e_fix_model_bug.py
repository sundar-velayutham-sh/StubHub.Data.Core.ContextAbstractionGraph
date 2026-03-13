"""
End-to-end test for the fix-model-bug workflow.

Tests the full 8-step workflow with CONDITIONAL BRANCHING.
Two test classes exercise different branch paths:
  - TestFixModelBugCast: cast_error branch (classify -> fix_cast_error -> validate)
  - TestFixModelBugJoin: join_error branch (classify -> fix_join_error -> validate)
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

# Steps for cast_error path (skips fix_join_error and fix_logic_error)
CAST_PATH_STEPS = [
    "parse_error",
    "read_model_sql",
    "classify_bug_type",
    "fix_cast_error",
    "validate_fix",
    "create_pr",
]

# Steps for join_error path (skips fix_cast_error and fix_logic_error)
JOIN_PATH_STEPS = [
    "parse_error",
    "read_model_sql",
    "classify_bug_type",
    "fix_join_error",
    "validate_fix",
    "create_pr",
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

    run = engine.start("fix-model-bug", inputs)
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
            if request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/99"}),
                )

    return run, steps_executed, reason_outputs


class TestFixModelBugCast:
    """Cast error branch: parse_error -> read_model_sql -> classify -> fix_cast_error -> validate -> create_pr."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "fix-model-bug-cast"
    INPUTS = {"model_name": "stg_ticket_listing", "error_message": "Numeric value 'N/A' is not recognized"}
    REASON_STEPS = [s for s in CAST_PATH_STEPS if s != "create_pr"]

    def test_workflow_takes_cast_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == CAST_PATH_STEPS

    def test_cast_path_has_6_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 6

    def test_classify_returns_cast_error(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_bug_type"]["bug_type"] == "cast_error"

    def test_fix_uses_try_cast(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        fixed_sql = reason_outputs["fix_cast_error"]["fixed_sql"]
        assert "TRY_CAST" in fixed_sql
        assert "NULLIF" in fixed_sql

    def test_problematic_values_found(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        values = reason_outputs["fix_cast_error"]["problematic_values"]
        assert len(values) > 0
        assert "N/A" in values

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["validate_fix"]["compile_ok"] is True
        assert reason_outputs["validate_fix"]["tests_passed"] is True
        assert reason_outputs["validate_fix"]["fix_verified"] is True

    def test_skips_join_and_logic_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "fix_join_error" not in steps_executed
        assert "fix_logic_error" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "fix-model-bug"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "fix_cast_error" in step_ids
        assert "fix_join_error" not in step_ids

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("fix-model-bug", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "parse_error"
        assert request.persona.id == "analytics_engineer"
        assert len(request.tools) > 0


class TestFixModelBugJoin:
    """Join error branch: parse_error -> read_model_sql -> classify -> fix_join_error -> validate -> create_pr."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "fix-model-bug-join"
    INPUTS = {"model_name": "rpt_event_summary", "error_message": "Row count exceeded threshold (10x expected)"}
    REASON_STEPS = [s for s in JOIN_PATH_STEPS if s != "create_pr"]

    def test_workflow_takes_join_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == JOIN_PATH_STEPS

    def test_join_path_has_6_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 6

    def test_classify_returns_join_error(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_bug_type"]["bug_type"] == "join_error"

    def test_fix_adds_is_current_filter(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        fixed_sql = reason_outputs["fix_join_error"]["fixed_sql"]
        assert "IS_CURRENT" in fixed_sql

    def test_join_analysis_present(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        analysis = reason_outputs["fix_join_error"]["join_analysis"]
        assert analysis["root_cause"] is not None
        assert analysis["duplicates_per_key"] > 1

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["validate_fix"]["compile_ok"] is True
        assert reason_outputs["validate_fix"]["tests_passed"] is True
        assert reason_outputs["validate_fix"]["fix_verified"] is True

    def test_skips_cast_and_logic_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "fix_cast_error" not in steps_executed
        assert "fix_logic_error" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "fix_join_error" in step_ids
        assert "fix_cast_error" not in step_ids
