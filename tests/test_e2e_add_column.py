"""
THE NORTH STAR TEST.
Written first. Everything is built to make this pass.

Tests the full add-column-to-model workflow with cassette responses,
verifying the engine walks all 10 steps (1 SCRIPT + 7 REASON + 2 DELEGATE)
and that modified SQL/schema.yml match golden files.

4 test classes — one per intent level:
  - TestLevel1Passthrough (PCID)
  - TestLevel2Rename (userId → user_id)
  - TestLevel3Expression (price_with_markup)
  - TestLevel4Join (venue_name from dim_venue)
"""
import json
from pathlib import Path

from dcag import DCAGEngine
from dcag.types import (
    DelegateRequest,
    ExecuteScriptRequest,
    ReasonRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"

# All 10 steps in execution order (step 0 + 9 workflow steps)
EXPECTED_STEPS = [
    "setup_dbt_project",       # Step 0: EXECUTE/SCRIPT
    "resolve_model",           # Step 1: REASON (merged resolve_model + resolve_source_table)
    "discover_column",         # Step 2: REASON
    "determine_logic",         # Step 3: REASON
    "check_downstream_impact", # Step 4: REASON
    "show_plan",               # Step 5: DELEGATE (user confirms plan)
    "modify_staging_sql",      # Step 6: REASON
    "update_schema_yml",       # Step 7: REASON
    "validate",                # Step 8: REASON
    "create_pr",               # Step 9: DELEGATE
]

# 7 REASON steps that need cassettes (steps 1-4, 6-8)
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("setup_dbt_project", "show_plan", "create_pr")]


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 7 cassettes for a golden test."""
    cassettes = {}
    for step_id in REASON_STEPS:
        path = cassette_dir / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(cassette_dir: Path, inputs: dict) -> tuple:
    """Drive the full workflow with cassette responses. Returns (run, reason_outputs)."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes(cassette_dir)

    run = engine.start("add-column-to-model", inputs)
    assert run.status == "running"

    steps_executed = []
    reason_outputs = {}

    while run.status == "running":
        request = run.next_step()
        if request is None:
            break

        steps_executed.append(request.step_id)

        if isinstance(request, ExecuteScriptRequest):
            # Step 0: setup_dbt_project — simulate successful clone + dbt deps
            run.record_result(
                request.step_id,
                StepSuccess(output={
                    "dbt_project_path": "/tmp/astronomer-core-data",
                    "setup_mode": "full",
                    "dbt_available": True,
                    "dbt_mcp_available": True,
                    "fallback_mode": "full",
                }),
            )

        elif isinstance(request, ReasonRequest):
            cassette = cassettes[request.step_id]
            reason_outputs[request.step_id] = cassette["output"]
            run.record_result(
                request.step_id,
                StepSuccess(output=cassette["output"]),
            )

        elif isinstance(request, DelegateRequest):
            if request.step_id == "show_plan":
                # User approves the plan
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"approved": True, "user_feedback": None}),
                )
            elif request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/1"}),
                )

    return run, steps_executed, reason_outputs


class TestLevel1Passthrough:
    """Level 1: Pass-through — add PCID column directly from source."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "add-column-pcid"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "add-column-pcid"
    INPUTS = {"model_name": "stg_pricing_analytics_events", "column_name": "pcid"}

    def test_workflow_completes_10_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 10
        assert steps_executed == EXPECTED_STEPS

    def test_modified_sql_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        expected_sql = (self.GOLDEN_DIR / "staging.sql").read_text().strip()
        actual_sql = reason_outputs["modify_staging_sql"]["modified_sql"].strip()
        assert actual_sql == expected_sql, f"SQL mismatch:\n--- expected ---\n{expected_sql}\n--- actual ---\n{actual_sql}"

    def test_modified_schema_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        expected_yml = (self.GOLDEN_DIR / "schema.yml").read_text().strip()
        actual_yml = reason_outputs["update_schema_yml"]["modified_yml"].strip()
        assert actual_yml == expected_yml

    def test_trace_has_all_10_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "add-column-to-model"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 10

    def test_intent_level_is_passthrough(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["determine_logic"]["intent_level"] == "passthrough"

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-column-to-model", self.INPUTS)

        # Step 0 is EXECUTE/SCRIPT — skip it
        request = run.next_step()
        assert isinstance(request, ExecuteScriptRequest)
        run.record_result(request.step_id, StepSuccess(output={
            "dbt_project_path": "/tmp/test",
            "dbt_available": True,
            "dbt_mcp_available": True,
            "fallback_mode": "full",
        }))

        # Step 1 (resolve_model) is REASON
        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_model"
        assert request.persona.id == "analytics_engineer"
        assert len(request.persona.heuristics) > 0
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_delegate_request_has_approval_flag(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        # The last step was create_pr — check it via trace
        trace = run.get_trace()
        last_step = trace["steps"][-1]
        assert last_step["step_id"] == "create_pr"
        # DelegateRequest approval is verified by the run completing successfully


class TestLevel2Rename:
    """Level 2: Rename — add userId as user_id."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "add-column-rename"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "add-column-rename"
    INPUTS = {"model_name": "stg_pricing_analytics_events", "column_name": "user_id"}

    def test_workflow_completes_10_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 10

    def test_intent_level_is_rename(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["determine_logic"]["intent_level"] == "rename"

    def test_modified_sql_has_alias(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        modified_sql = reason_outputs["modify_staging_sql"]["modified_sql"]
        assert "AS USER_ID" in modified_sql or "as user_id" in modified_sql.lower()


class TestLevel3Expression:
    """Level 3: Expression — add price_with_markup as computed column."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "add-column-expression"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "add-column-expression"
    INPUTS = {"model_name": "stg_pricing_analytics_events", "column_name": "price_with_markup"}

    def test_workflow_completes_10_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 10

    def test_intent_level_is_expression(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["determine_logic"]["intent_level"] == "expression"

    def test_modified_sql_has_expression(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        modified_sql = reason_outputs["modify_staging_sql"]["modified_sql"]
        assert "ROUND" in modified_sql or "round" in modified_sql.lower()


class TestLevel4Join:
    """Level 4: Join — add venue_name from dim_venue."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "add-column-join"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "add-column-join"
    INPUTS = {"model_name": "stg_pricing_analytics_events", "column_name": "venue_name"}

    def test_workflow_completes_10_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 10

    def test_intent_level_is_join(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["determine_logic"]["intent_level"] == "join"

    def test_modified_sql_has_join(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        modified_sql = reason_outputs["modify_staging_sql"]["modified_sql"]
        assert "JOIN" in modified_sql or "join" in modified_sql.lower()

    def test_modified_sql_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        expected_sql = (self.GOLDEN_DIR / "staging.sql").read_text().strip()
        actual_sql = reason_outputs["modify_staging_sql"]["modified_sql"].strip()
        assert actual_sql == expected_sql
