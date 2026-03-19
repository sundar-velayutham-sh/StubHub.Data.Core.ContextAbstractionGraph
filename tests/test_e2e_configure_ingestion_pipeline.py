"""
End-to-end test for the configure-ingestion-pipeline workflow.

Tests the full 7-step workflow with cassette responses, verifying
the engine walks all steps and produces a correct ingestion config.

Test scenario: Configure ingestion for PARTNER_COMMISSION from ECOMM SQL Server.
"""
import json
from pathlib import Path

from dcag import DCAGEngine
from dcag.types import (
    DelegateRequest,
    ReasonRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"
CASSETTE_DIR = Path(__file__).parent / "cassettes" / "configure-ingestion-pipeline"
GOLDEN_DIR = Path(__file__).parent / "goldens" / "configure-ingestion-pipeline"

# All 7 steps in execution order
EXPECTED_STEPS = [
    "discover_source_schema",
    "design_staging_table",
    "generate_ingestion_config",
    "configure_load_frequency",
    "validate_connectivity",
    "show_plan",
    "create_pr",
]

# 5 REASON steps that need cassettes
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("show_plan", "create_pr")]

INPUTS = {"table_name": "PARTNER_COMMISSION", "source_database": "ECOMM"}


def load_cassettes() -> dict[str, dict]:
    """Load all 5 cassettes for the configure-ingestion-pipeline test."""
    cassettes = {}
    for step_id in REASON_STEPS:
        path = CASSETTE_DIR / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(inputs: dict = None) -> tuple:
    """Drive the full workflow with cassette responses."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes()

    run = engine.start("configure-ingestion-pipeline", inputs or INPUTS)
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
            if request.step_id == "show_plan":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"approved": True, "user_feedback": None}),
                )
            elif request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/55"}),
                )

    return run, steps_executed, reason_outputs


class TestConfigureIngestionPipeline:
    """Configure ingestion for PARTNER_COMMISSION from ECOMM."""

    def test_workflow_completes_7_steps(self):
        run, steps_executed, _ = run_workflow()
        assert run.status == "completed"
        assert len(steps_executed) == 7
        assert steps_executed == EXPECTED_STEPS

    def test_source_schema_discovered(self):
        _, _, reason_outputs = run_workflow()
        schema = reason_outputs["discover_source_schema"]
        assert schema["source_table_fqn"] == "ECOMM.DBO.PARTNER_COMMISSION"
        assert len(schema["columns"]) == 10
        assert schema["primary_key"] == ["CommissionId"]
        assert schema["row_count"] == 8500000
        assert schema["update_frequency"] == "HOURLY"

    def test_staging_table_is_transient(self):
        _, _, reason_outputs = run_workflow()
        sql = reason_outputs["design_staging_table"]["create_table_sql"]
        assert "TRANSIENT" in sql

    def test_staging_table_has_metadata_columns(self):
        _, _, reason_outputs = run_workflow()
        sql = reason_outputs["design_staging_table"]["create_table_sql"]
        assert "_LOADED_AT" in sql
        assert "_SOURCE" in sql

    def test_staging_table_fqn_follows_convention(self):
        _, _, reason_outputs = run_workflow()
        fqn = reason_outputs["design_staging_table"]["staging_table_fqn"]
        assert fqn == "DW.STAGING.STG_PARTNER_COMMISSION"

    def test_column_mapping_correct_count(self):
        _, _, reason_outputs = run_workflow()
        mapping = reason_outputs["design_staging_table"]["column_mapping"]
        assert len(mapping) == 10

    def test_type_mapping_applied(self):
        _, _, reason_outputs = run_workflow()
        mapping = reason_outputs["design_staging_table"]["column_mapping"]
        bigint_col = next(m for m in mapping if m["source"] == "CommissionId")
        assert bigint_col["target_type"] == "NUMBER(38,0)"
        datetime_col = next(m for m in mapping if m["source"] == "CreatedDate")
        assert datetime_col["target_type"] == "TIMESTAMP_NTZ"

    def test_snake_case_naming(self):
        _, _, reason_outputs = run_workflow()
        mapping = reason_outputs["design_staging_table"]["column_mapping"]
        camel_col = next(m for m in mapping if m["source"] == "CommissionId")
        assert camel_col["target"] == "COMMISSION_ID"

    def test_ingestion_config_is_sql_server(self):
        _, _, reason_outputs = run_workflow()
        config = reason_outputs["generate_ingestion_config"]
        assert config["database_class"] == "sql_server"
        assert "SqlServerTable(" in config["config_snippet"]

    def test_config_has_incremental_column(self):
        _, _, reason_outputs = run_workflow()
        snippet = reason_outputs["generate_ingestion_config"]["config_snippet"]
        assert "MODIFIED_DATE" in snippet

    def test_load_frequency_is_hourly(self):
        _, _, reason_outputs = run_workflow()
        freq = reason_outputs["configure_load_frequency"]
        assert freq["cron_schedule"] == "0 * * * *"
        assert freq["sla_tier"] == "tier_2"
        assert freq["incremental_strategy"] == "UPSERT"

    def test_connectivity_validated(self):
        _, _, reason_outputs = run_workflow()
        assert reason_outputs["validate_connectivity"]["connectivity_ok"] is True
        assert reason_outputs["validate_connectivity"]["row_count_verified"] is True
        assert reason_outputs["validate_connectivity"]["errors"] == []

    def test_no_pii_detected(self):
        _, _, reason_outputs = run_workflow()
        assert reason_outputs["design_staging_table"]["pii_columns"] == []

    def test_golden_match(self):
        _, _, reason_outputs = run_workflow()
        with open(GOLDEN_DIR / "ingestion_config_result.json") as f:
            golden = json.load(f)
        assert reason_outputs["discover_source_schema"]["source_table_fqn"] == golden["source_table"]
        assert reason_outputs["design_staging_table"]["staging_table_fqn"] == golden["staging_table"]
        assert reason_outputs["generate_ingestion_config"]["database_class"] == golden["database_class"]
        assert reason_outputs["configure_load_frequency"]["cron_schedule"] == golden["cron_schedule"]
        assert reason_outputs["configure_load_frequency"]["incremental_strategy"] == golden["incremental_strategy"]

    def test_trace_has_all_7_steps(self):
        run, _, _ = run_workflow()
        trace = run.get_trace()
        assert trace["workflow_id"] == "configure-ingestion-pipeline"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 7

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("configure-ingestion-pipeline", INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "discover_source_schema"
        assert request.persona.id == "data_engineer"
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_show_plan_requires_approval(self):
        """Verify show_plan is a delegate with approval gate."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes()
        run = engine.start("configure-ingestion-pipeline", INPUTS)

        # Walk through 5 REASON steps
        for step_id in REASON_STEPS:
            request = run.next_step()
            assert request.step_id == step_id
            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        # Step 6 should be show_plan (DelegateRequest)
        request = run.next_step()
        assert isinstance(request, DelegateRequest)
        assert request.step_id == "show_plan"
        assert request.requires_approval is True

    def test_design_staging_has_no_tools(self):
        """Verify design_staging_table is a pure reasoning step with no tools."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes()
        run = engine.start("configure-ingestion-pipeline", INPUTS)

        # Walk to discover_source_schema
        request = run.next_step()
        run.record_result(request.step_id, StepSuccess(output=cassettes[request.step_id]["output"]))

        # design_staging_table should have 0 tools
        request = run.next_step()
        assert request.step_id == "design_staging_table"
        assert isinstance(request, ReasonRequest)
        assert len(request.tools) == 0
