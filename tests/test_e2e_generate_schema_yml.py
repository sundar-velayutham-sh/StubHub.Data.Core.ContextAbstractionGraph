"""
End-to-end test for the generate-schema-yml workflow.

Tests the full 6-step workflow with cassette responses, verifying
the engine walks all steps and produces a correct schema.yml.
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

# All 6 steps in execution order
EXPECTED_STEPS = [
    "resolve_model",
    "parse_columns",
    "describe_columns",
    "generate_yml",
    "validate",
    "create_pr",
]

# 5 REASON steps that need cassettes
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("create_pr",)]


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 5 cassettes for the generate-schema-yml test."""
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

    run = engine.start("generate-schema-yml", inputs)
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
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/55"}),
                )

    return run, steps_executed, reason_outputs


class TestGenerateSchemaYml:
    """Generate schema.yml for fct_ticket_sales model."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "generate-schema-yml"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "generate-schema-yml"
    INPUTS = {"model_name": "fct_ticket_sales"}

    def test_workflow_completes_6_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 6
        assert steps_executed == EXPECTED_STEPS

    def test_model_path_resolved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["resolve_model"]["model_path"] == "models/marts/core/fct_ticket_sales.sql"

    def test_source_table_fqn_resolved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["resolve_model"]["source_table_fqn"] == "DW.RPT.TICKET_SALES"

    def test_columns_parsed(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        columns = reason_outputs["parse_columns"]["columns"]
        assert len(columns) == 10
        assert columns[0]["name"] == "TICKET_SALE_ID"
        assert reason_outputs["parse_columns"]["has_star_select"] is False

    def test_column_metadata_collected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        metadata = reason_outputs["describe_columns"]["column_metadata"]
        assert len(metadata) == 10
        pk = metadata[0]
        assert pk["name"] == "TICKET_SALE_ID"
        assert pk["nullable"] is False
        assert pk["is_pk"] is True

    def test_schema_yml_generated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["generate_yml"]
        assert output["column_count"] == 10
        assert "fct_ticket_sales" in output["schema_yml_content"]
        assert "ticket_sale_id" in output["schema_yml_content"]

    def test_pk_has_not_null_and_unique(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_yml"]["tests_added"]
        pk_tests = next(t for t in tests_added if t["column"] == "ticket_sale_id")
        assert "not_null" in pk_tests["tests"]
        assert "unique" in pk_tests["tests"]

    def test_nullable_columns_skip_not_null(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_yml"]["tests_added"]
        test_columns = [t["column"] for t in tests_added]
        # section_name and updated_at are nullable — should not have not_null
        assert "section_name" not in test_columns
        assert "updated_at" not in test_columns

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["validate"]["parse_ok"] is True
        assert reason_outputs["validate"]["compile_ok"] is True
        assert reason_outputs["validate"]["errors"] == []

    def test_report_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        with open(self.GOLDEN_DIR / "schema_yml_output.json") as f:
            golden = json.load(f)
        output = reason_outputs["generate_yml"]
        assert output["column_count"] == golden["column_count"]
        pk_tests = next(t for t in output["tests_added"] if t["column"] == golden["pk_column"])
        assert set(pk_tests["tests"]) == set(golden["pk_tests"])

    def test_trace_has_all_6_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "generate-schema-yml"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 6

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("generate-schema-yml", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_model"
        assert request.persona.id == "analytics_engineer"
        assert len(request.persona.heuristics) > 0
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_delegate_create_pr_has_approval(self):
        """Verify create_pr is a delegate with approval gate."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes(self.CASSETTE_DIR)
        run = engine.start("generate-schema-yml", self.INPUTS)

        # Walk through REASON steps until we hit create_pr
        for step_id in EXPECTED_STEPS[:5]:
            request = run.next_step()
            assert request.step_id == step_id
            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        # Step 5 should be create_pr (DelegateRequest)
        request = run.next_step()
        assert isinstance(request, DelegateRequest)
        assert request.step_id == "create_pr"
        assert request.requires_approval is True
