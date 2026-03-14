"""
End-to-end test for the create-etl-pipeline workflow.

Tests the full 18-step workflow with 4 ENTRY POINT BRANCHES at classify_intent.
Each test class exercises a different entry point:
  - TestNewSource: classify -> discover_source_schema -> profile -> design -> generate -> validate -> orchestration
  - TestSimilarTo: classify -> trace_reference_pipeline -> profile -> design -> generate -> validate -> orchestration
  - TestSqlToPipeline: classify -> parse_sql_sources -> profile -> design -> generate -> validate -> orchestration
  - TestExtendExisting: classify -> analyze_target_pipeline -> profile -> design -> generate -> validate -> orchestration
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

# Steps for new_source path (discovery via discover_source_schema)
# generate_models runs twice (loop over design_pipeline.models which has 2 models)
NEW_SOURCE_STEPS = [
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

# Steps for similar_to path (discovery via trace_reference_pipeline)
SIMILAR_TO_STEPS = [
    "setup_environment",
    "classify_intent",
    "trace_reference_pipeline",
    "profile_source_data",
    "discover_reference_patterns",
    "design_pipeline",
    "confirm_plan",
    "generate_models",
    "validate_pipeline",
    "recommend_tests",
    "show_results",
    "create_pr",
    "recommend_orchestration",
]

# Steps for sql_to_pipeline path (discovery via parse_sql_sources)
SQL_TO_PIPELINE_STEPS = [
    "setup_environment",
    "classify_intent",
    "parse_sql_sources",
    "profile_source_data",
    "discover_reference_patterns",
    "design_pipeline",
    "confirm_plan",
    "generate_models",
    "validate_pipeline",
    "recommend_tests",
    "show_results",
    "create_pr",
    "recommend_orchestration",
]

# Steps for extend_existing path (discovery via analyze_target_pipeline)
EXTEND_EXISTING_STEPS = [
    "setup_environment",
    "classify_intent",
    "analyze_target_pipeline",
    "profile_source_data",
    "discover_reference_patterns",
    "design_pipeline",
    "confirm_plan",
    "generate_models",
    "validate_pipeline",
    "recommend_tests",
    "show_results",
    "create_pr",
    "recommend_orchestration",
]


def load_cassettes(cassette_dir: Path, reason_steps: list[str]) -> dict[str, dict]:
    """Load cassettes for the steps that will actually execute."""
    cassettes = {}
    for step_id in reason_steps:
        path = cassette_dir / f"{step_id}.json"
        if path.exists():
            with open(path) as f:
                cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(
    cassette_dir: Path,
    inputs: dict,
    expected_steps: list[str],
) -> tuple:
    """Drive the workflow with cassette responses, handling branching and delegation."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    reason_steps = [s for s in expected_steps if s not in ("confirm_plan", "show_results", "create_pr")]
    cassettes = load_cassettes(cassette_dir, reason_steps)

    run = engine.start("create-etl-pipeline", inputs)
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
            if request.step_id == "confirm_plan":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"user_decision": "approve", "feedback": ""}),
                )
            elif request.step_id == "show_results":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"user_decision": "approve", "edit_request": "", "edit_count": 0}),
                )
            elif request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer/pull/123", "pr_number": 123}),
                )

    return run, steps_executed, reason_outputs


class TestNewSource:
    """New source path: setup -> classify -> discover_source -> profile -> reference -> design -> confirm -> generate -> validate -> tests -> show -> PR -> orchestration."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-new-source"
    INPUTS = {"request_text": "Build a pipeline for fivetran_database.tiktok_ads.campaign_report"}

    def test_workflow_takes_new_source_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert run.status == "completed"
        assert steps_executed == NEW_SOURCE_STEPS

    def test_new_source_path_step_count(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert len(steps_executed) == 14  # generate_models loops twice (2 models)

    def test_classify_returns_new_source(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "new_source"

    def test_source_discovered_correctly(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        sources = reason_outputs["discover_source_schema"]["source_tables"]
        assert len(sources) == 1
        assert "TIKTOK" in sources[0]["table_fqn"]

    def test_profiling_no_warnings(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["profile_source_data"]["warnings"] == []

    def test_pipeline_pattern_is_multi_source_union(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["design_pipeline"]["pipeline_pattern"] == "multi_source_union"

    def test_design_has_2_models(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        models = reason_outputs["design_pipeline"]["models"]
        assert len(models) == 2
        assert models[0]["is_new"] is True
        assert models[1]["is_new"] is False  # modification to existing

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["validate_pipeline"]["compiles"] is True
        assert reason_outputs["validate_pipeline"]["errors"] == []

    def test_tests_recommended(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        tests = reason_outputs["recommend_tests"]["recommended_tests"]
        assert len(tests) >= 4
        test_names = [t["test_name"] for t in tests]
        assert "unique" in test_names
        assert "not_null" in test_names

    def test_orchestration_uses_existing_dag(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        orch = reason_outputs["recommend_orchestration"]
        assert orch["dag_name"] == "transform_acquisition__daily"
        assert orch["selector_match"] is True

    def test_skips_other_discovery_branches(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert "trace_reference_pipeline" not in steps_executed
        assert "parse_sql_sources" not in steps_executed
        assert "analyze_target_pipeline" not in steps_executed

    def test_trace_records_all_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "create-etl-pipeline"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "discover_source_schema" in step_ids
        assert "trace_reference_pipeline" not in step_ids

    def test_generated_sql_has_tiktok_source(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        sql = reason_outputs["generate_models"]["sql_content"]
        assert "tiktok_ads" in sql
        assert "campaign_report" in sql

    def test_change_points_provided(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        cps = reason_outputs["generate_models"]["change_points"]
        assert len(cps) >= 2
        assert any("spend" in cp["section"].lower() for cp in cps)
