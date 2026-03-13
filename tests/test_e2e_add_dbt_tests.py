"""
End-to-end test for the add-dbt-tests workflow.

Tests the full 5-step workflow with cassette responses, verifying
the engine walks all steps and produces correct test additions.
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

# All 5 steps in execution order
EXPECTED_STEPS = [
    "resolve_model",
    "get_column_metadata",
    "infer_tests",
    "update_schema_yml",
    "validate",
]

# All 5 steps are REASON (no DELEGATE in this workflow)
REASON_STEPS = EXPECTED_STEPS


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 5 cassettes for the add-dbt-tests test."""
    cassettes = {}
    for step_id in REASON_STEPS:
        path = cassette_dir / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(cassette_dir: Path, inputs: dict) -> tuple:
    """Drive the full workflow with cassette responses."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes(cassette_dir)

    run = engine.start("add-dbt-tests", inputs)
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
            run.record_result(
                request.step_id,
                StepSuccess(output={"approved": True}),
            )

    return run, steps_executed, reason_outputs


class TestAddDbtTests:
    """Add tests to dim_venue model with partial coverage."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "add-dbt-tests"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "add-dbt-tests"
    INPUTS = {"model_name": "dim_venue"}

    def test_workflow_completes_5_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 5
        assert steps_executed == EXPECTED_STEPS

    def test_existing_tests_detected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        existing = reason_outputs["resolve_model"]["existing_tests"]
        assert len(existing) == 2
        venue_id_tests = next(t for t in existing if t["column"] == "venue_id")
        assert "not_null" in venue_id_tests["tests"]
        assert "unique" in venue_id_tests["tests"]

    def test_columns_without_tests_identified(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        untested = reason_outputs["resolve_model"]["columns_without_tests"]
        assert len(untested) == 8
        assert "city" in untested
        assert "is_active" in untested

    def test_column_metadata_collected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        metadata = reason_outputs["get_column_metadata"]["column_metadata"]
        assert len(metadata) == 10
        country = next(c for c in metadata if c["name"] == "COUNTRY")
        assert country["cardinality"] == 12
        assert country["nullable"] is False

    def test_new_tests_inferred(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        new_tests = reason_outputs["infer_tests"]["new_tests"]
        assert reason_outputs["infer_tests"]["total_tests_to_add"] == 8
        # Verify not_null tests
        not_null_cols = [t["column"] for t in new_tests if t["test_type"] == "not_null"]
        assert "city" in not_null_cols
        assert "country" in not_null_cols
        assert "is_active" in not_null_cols

    def test_accepted_values_for_low_cardinality(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        new_tests = reason_outputs["infer_tests"]["new_tests"]
        av_tests = [t for t in new_tests if t["test_type"] == "accepted_values"]
        av_cols = [t["column"] for t in av_tests]
        assert "country" in av_cols
        assert "venue_type" in av_cols
        assert "is_active" in av_cols

    def test_nullable_columns_skipped(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        new_tests = reason_outputs["infer_tests"]["new_tests"]
        tested_cols = [t["column"] for t in new_tests]
        # Nullable columns should NOT get not_null
        not_null_cols = [t["column"] for t in new_tests if t["test_type"] == "not_null"]
        assert "state" not in not_null_cols
        assert "capacity" not in not_null_cols
        assert "updated_at" not in not_null_cols

    def test_schema_yml_updated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["update_schema_yml"]
        assert output["tests_added_count"] == 8
        assert "dim_venue" in output["modified_yml"]
        assert "accepted_values" in output["modified_yml"]

    def test_existing_tests_preserved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        yml = reason_outputs["update_schema_yml"]["modified_yml"]
        # venue_id should still have unique test
        assert "unique" in yml
        # venue_name should still have not_null
        assert "venue_name" in yml

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["validate"]["parse_ok"] is True
        assert reason_outputs["validate"]["tests_passed"] == 11
        assert reason_outputs["validate"]["tests_failed"] == 0

    def test_report_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        with open(self.GOLDEN_DIR / "test_coverage_output.json") as f:
            golden = json.load(f)
        infer = reason_outputs["infer_tests"]
        assert infer["total_tests_to_add"] == golden["new_tests_count"]
        av_cols = [t["column"] for t in infer["new_tests"] if t["test_type"] == "accepted_values"]
        assert set(av_cols) == set(golden["columns_with_accepted_values"])

    def test_trace_has_all_5_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "add-dbt-tests"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 5

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-dbt-tests", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_model"
        assert request.persona.id == "analytics_engineer"
        assert len(request.persona.heuristics) > 0
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_infer_tests_has_no_tools(self):
        """Verify infer_tests is pure reasoning with no tools."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes(self.CASSETTE_DIR)
        run = engine.start("add-dbt-tests", self.INPUTS)

        # Walk to infer_tests (step index 2)
        for step_id in EXPECTED_STEPS[:2]:
            request = run.next_step()
            run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "infer_tests"
        assert len(request.tools) == 0
