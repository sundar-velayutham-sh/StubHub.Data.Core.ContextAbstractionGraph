"""
End-to-end test for the create-etl-pipeline workflow.

Tests the 14-step guardrails-model workflow with 4 ENTRY POINT BRANCHES at classify_intent.
Each test class exercises a different entry point:
  - TestNewSource: classify -> discover_source_schema -> profile -> gather_reference -> build -> validate -> orchestration
  - TestSimilarTo: classify -> trace_reference_pipeline -> profile -> gather_reference -> build -> validate -> orchestration
  - TestSqlToPipeline: classify -> parse_sql_sources -> profile -> gather_reference -> build -> validate -> orchestration
  - TestExtendExisting: classify -> analyze_target_pipeline -> profile -> gather_reference -> build -> validate -> orchestration
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
NEW_SOURCE_STEPS = [
    "setup_environment",
    "classify_intent",
    "discover_source_schema",
    "profile_source_data",
    "gather_reference_context",
    "build_pipeline",
    "validate_output",
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
    "gather_reference_context",
    "build_pipeline",
    "validate_output",
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
    "gather_reference_context",
    "build_pipeline",
    "validate_output",
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
    "gather_reference_context",
    "build_pipeline",
    "validate_output",
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
    reason_steps = [s for s in expected_steps if s not in ("show_results", "create_pr")]
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
            if request.step_id == "show_results":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"user_decision": "approve", "revision_request": "", "revision_count": 0}),
                )
            elif request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer/pull/123", "pr_number": 123}),
                )

    return run, steps_executed, reason_outputs


class TestNewSource:
    """New source path: setup -> classify -> discover_source -> profile -> gather_reference -> build -> validate -> show -> PR -> orchestration."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-new-source"
    INPUTS = {"request_text": "Build a pipeline for fivetran_database.tiktok_ads.campaign_report"}

    def test_workflow_takes_new_source_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert run.status == "completed"
        assert steps_executed == NEW_SOURCE_STEPS

    def test_new_source_path_step_count(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert len(steps_executed) == 10

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
        assert reason_outputs["build_pipeline"]["design"]["pattern"] == "multi_source_union"

    def test_design_has_2_models(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        models = reason_outputs["build_pipeline"]["models"]
        assert len(models) == 2
        assert models[0]["is_new"] is True
        assert models[1]["is_new"] is False  # modification to existing

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["validate_output"]["overall_status"] == "all_pass"

    def test_tests_recommended(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        # Tests are now embedded in build_pipeline's schema_yml_content
        models = reason_outputs["build_pipeline"]["models"]
        schema_yml = models[0]["schema_yml_content"]
        assert "unique" in schema_yml
        assert "not_null" in schema_yml

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


class TestSimilarTo:
    """Similar-to path: setup -> classify -> trace_reference -> profile -> gather_reference -> build -> validate -> show -> PR -> orchestration."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-similar-to"
    INPUTS = {"request_text": "Build something like campaign_day_agg but for affiliate traffic"}

    def test_workflow_takes_similar_to_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert run.status == "completed"
        assert "trace_reference_pipeline" in steps_executed
        assert "discover_source_schema" not in steps_executed

    def test_similar_to_path_step_count(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert len(steps_executed) == 10

    def test_classify_returns_similar_to(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "similar_to"

    def test_reference_model_hint_extracted(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert reason_outputs["classify_intent"]["reference_model_hint"] == "campaign_day_agg"

    def test_pipeline_pattern_matches_reference(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert reason_outputs["build_pipeline"]["design"]["pattern"] == "hourly_rollup"

    def test_profiling_warns_on_nulls(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        warnings = reason_outputs["profile_source_data"]["warnings"]
        assert len(warnings) == 1
        assert warnings[0]["warning_type"] == "high_nulls"

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert reason_outputs["validate_output"]["overall_status"] == "all_pass"

    def test_skips_other_discovery_branches(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert "discover_source_schema" not in steps_executed
        assert "parse_sql_sources" not in steps_executed
        assert "analyze_target_pipeline" not in steps_executed

    def test_trace_records_all_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "create-etl-pipeline"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "trace_reference_pipeline" in step_ids
        assert "discover_source_schema" not in step_ids


class TestSqlToPipeline:
    """SQL-to-pipeline path: setup -> classify -> parse_sql -> profile -> gather_reference -> build -> validate -> show -> PR -> orchestration."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-sql-to-pipeline"
    INPUTS = {
        "request_text": "Make this a proper pipeline",
        "sql_text": "SELECT campaign_id, SUM(spend) AS total_spend FROM fivetran_database.tiktok_ads.campaign_report GROUP BY 1",
    }

    def test_workflow_takes_sql_to_pipeline_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert run.status == "completed"
        assert "parse_sql_sources" in steps_executed
        assert "discover_source_schema" not in steps_executed

    def test_sql_to_pipeline_step_count(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert len(steps_executed) == 10

    def test_classify_returns_sql_to_pipeline(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "sql_to_pipeline"

    def test_sql_text_passed_through(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert "SUM(spend)" in reason_outputs["classify_intent"]["sql_text"]

    def test_pipeline_pattern_is_standard(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert reason_outputs["build_pipeline"]["design"]["pattern"] == "standard"

    def test_design_has_1_model(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        models = reason_outputs["build_pipeline"]["models"]
        assert len(models) == 1
        assert models[0]["is_new"] is True

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert reason_outputs["validate_output"]["overall_status"] == "all_pass"

    def test_skips_other_discovery_branches(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert "discover_source_schema" not in steps_executed
        assert "trace_reference_pipeline" not in steps_executed
        assert "analyze_target_pipeline" not in steps_executed

    def test_trace_records_all_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "create-etl-pipeline"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "parse_sql_sources" in step_ids
        assert "discover_source_schema" not in step_ids


class TestExtendExisting:
    """Extend-existing path: setup -> classify -> analyze_target -> profile -> gather_reference -> build -> validate -> show -> PR -> orchestration."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-extend-existing"
    INPUTS = {"request_text": "Add TikTok as a new channel to marketing_spend_day_country_agg"}

    def test_workflow_takes_extend_existing_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert run.status == "completed"
        assert "analyze_target_pipeline" in steps_executed
        assert "discover_source_schema" not in steps_executed

    def test_extend_existing_step_count(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert len(steps_executed) == 10

    def test_classify_returns_extend_existing(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "extend_existing"

    def test_reference_model_hint_extracted(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert reason_outputs["classify_intent"]["reference_model_hint"] == "marketing_spend_day_country_agg"

    def test_design_has_modification(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        models = reason_outputs["build_pipeline"]["models"]
        has_new = any(m["is_new"] for m in models)
        has_modification = any(not m["is_new"] for m in models)
        assert has_new
        assert has_modification

    def test_design_has_2_models(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        models = reason_outputs["build_pipeline"]["models"]
        assert len(models) == 2

    def test_pipeline_pattern_is_multi_source_union(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert reason_outputs["build_pipeline"]["design"]["pattern"] == "multi_source_union"

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert reason_outputs["validate_output"]["overall_status"] == "all_pass"

    def test_skips_other_discovery_branches(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert "discover_source_schema" not in steps_executed
        assert "trace_reference_pipeline" not in steps_executed
        assert "parse_sql_sources" not in steps_executed

    def test_trace_records_all_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "create-etl-pipeline"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "analyze_target_pipeline" in step_ids
        assert "discover_source_schema" not in step_ids
