"""
End-to-end test for the create-staging-model workflow.

Tests the full 8-step linear workflow with cassette responses, verifying
the engine walks all steps and produces a correct staging model package
(SQL + schema.yml + sources.yml).
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

# All 8 steps in execution order
EXPECTED_STEPS = [
    "discover_source_table",
    "check_existing_models",
    "choose_materialization",
    "generate_model_sql",
    "generate_schema_yml",
    "add_to_sources_yml",
    "validate",
    "create_pr",
]

# 7 REASON steps that need cassettes
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("create_pr",)]


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 7 cassettes for the create-staging-model test."""
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

    run = engine.start("create-staging-model", inputs)
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
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/67"}),
                )

    return run, steps_executed, reason_outputs


class TestCreateStagingModel:
    """Create staging model for PAYMENT_TRANSACTION from RAW schema."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-staging-model"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "create-staging-model"
    INPUTS = {"table_name": "PAYMENT_TRANSACTION", "source_system": "payment", "database": "DW", "schema": "RAW"}

    def test_workflow_completes_8_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 8
        assert steps_executed == EXPECTED_STEPS

    def test_table_fqn_resolved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["discover_source_table"]["table_fqn"] == "DW.RAW.PAYMENT_TRANSACTION"

    def test_columns_discovered(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        columns = reason_outputs["discover_source_table"]["columns"]
        assert len(columns) == 10
        assert columns[0]["name"] == "PAYMENT_ID"

    def test_no_existing_model(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["check_existing_models"]["model_exists"] is False

    def test_materialization_is_incremental(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        mat = reason_outputs["choose_materialization"]
        assert mat["materialization"] == "incremental"
        assert mat["unique_key"] == "payment_id"
        assert mat["incremental_strategy"] == "merge"

    def test_model_sql_generated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["generate_model_sql"]
        assert output["model_filename"] == "stg_payment_transaction.sql"
        assert output["column_count"] == 10
        assert "incremental" in output["model_sql"]
        assert "source('payment', 'PAYMENT_TRANSACTION')" in output["model_sql"]
        assert "is_incremental()" in output["model_sql"]

    def test_model_sql_has_snake_case_columns(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        sql = reason_outputs["generate_model_sql"]["model_sql"]
        assert "payment_id" in sql
        assert "payment_method" in sql
        assert "processor_response_code" in sql

    def test_model_sql_has_trim(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        sql = reason_outputs["generate_model_sql"]["model_sql"]
        assert "TRIM(" in sql

    def test_schema_yml_generated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["generate_schema_yml"]
        assert output["column_count"] == 10
        assert "stg_payment_transaction" in output["schema_yml_content"]
        assert "payment_id" in output["schema_yml_content"]

    def test_pk_has_not_null_and_unique(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_schema_yml"]["tests_added"]
        pk_tests = next(t for t in tests_added if t["column"] == "payment_id")
        assert "not_null" in pk_tests["tests"]
        assert "unique" in pk_tests["tests"]

    def test_enum_columns_have_accepted_values(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_schema_yml"]["tests_added"]
        status_tests = next(t for t in tests_added if t["column"] == "status")
        assert "accepted_values" in status_tests["tests"]
        currency_tests = next(t for t in tests_added if t["column"] == "currency")
        assert "accepted_values" in currency_tests["tests"]

    def test_sources_yml_updated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["add_to_sources_yml"]
        assert output["is_new_source"] is False
        assert output["source_name"] == "payment"
        assert "PAYMENT_TRANSACTION" in output["sources_yml_content"]

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["validate"]["compile_ok"] is True
        assert reason_outputs["validate"]["parse_ok"] is True
        assert reason_outputs["validate"]["errors"] == []

    def test_report_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        with open(self.GOLDEN_DIR / "staging_model_output.json") as f:
            golden = json.load(f)
        assert reason_outputs["discover_source_table"]["table_fqn"] == golden["table_fqn"]
        assert reason_outputs["generate_model_sql"]["model_filename"] == golden["model_filename"]
        assert reason_outputs["choose_materialization"]["materialization"] == golden["materialization"]
        assert reason_outputs["generate_model_sql"]["column_count"] == golden["column_count"]
        assert reason_outputs["validate"]["compile_ok"] == golden["compile_ok"]

    def test_trace_has_all_8_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "create-staging-model"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 8

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("create-staging-model", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "discover_source_table"
        assert request.persona.id == "analytics_engineer"
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_cache_as_declared_on_discover_step(self):
        """Verify the discover_source_table step declares cache_as."""
        from dcag._loaders import WorkflowLoader
        wf = WorkflowLoader(CONTENT_DIR / "workflows").load("create-staging-model")
        discover_step = next(s for s in wf.steps if s.id == "discover_source_table")
        assert discover_step.cache_as == "source_metadata"
