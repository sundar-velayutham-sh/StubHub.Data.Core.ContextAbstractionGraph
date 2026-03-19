"""
End-to-end test for the thread-field-through-pipeline workflow.

Tests the full 7-step workflow with cassette responses, including
LOOP steps that iterate over a 3-model pipeline chain.

Test scenario: Thread VENUE_CAPACITY through stg_venues -> int_venues_enriched -> fct_event_sales.
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
CASSETTE_DIR = Path(__file__).parent / "cassettes" / "thread-field-through-pipeline"
GOLDEN_DIR = Path(__file__).parent / "goldens" / "thread-field-through-pipeline"

# All 7 logical steps in execution order.
# Loop steps (modify_each_model, update_each_schema) execute 3 times each,
# so actual step count is 3 + 3*2 + 2 = 11 step executions.
EXPECTED_LOGICAL_STEPS = [
    "resolve_source_column",
    "trace_pipeline_lineage",
    "show_plan",
    "modify_each_model",
    "update_each_schema",
    "validate_pipeline",
    "create_pr",
]

# Non-loop REASON steps that need simple cassettes
SIMPLE_REASON_STEPS = ["resolve_source_column", "trace_pipeline_lineage", "validate_pipeline"]

# Loop steps with multi-output cassettes
LOOP_STEPS = ["modify_each_model", "update_each_schema"]

INPUTS = {"column_name": "VENUE_CAPACITY", "source_model": "stg_venues"}

CHAIN_LENGTH = 3


def load_cassettes() -> dict[str, dict]:
    """Load all cassettes for the thread-field-through-pipeline test."""
    cassettes = {}
    for step_id in SIMPLE_REASON_STEPS + LOOP_STEPS:
        path = CASSETTE_DIR / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow() -> tuple:
    """Drive the full workflow with cassette responses."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes()

    run = engine.start("thread-field-through-pipeline", INPUTS)
    assert run.status == "running"

    steps_executed = []
    reason_outputs = {}
    loop_iteration_count = {"modify_each_model": 0, "update_each_schema": 0}

    while run.status == "running":
        request = run.next_step()
        if request is None:
            break

        steps_executed.append(request.step_id)

        if isinstance(request, ReasonRequest):
            if request.step_id in LOOP_STEPS:
                # Loop step -- feed the iteration-specific output
                idx = loop_iteration_count[request.step_id]
                cassette = cassettes[request.step_id]
                iteration_output = cassette["loop_outputs"][idx]["output"]
                reason_outputs.setdefault(request.step_id, []).append(iteration_output)
                run.record_result(
                    request.step_id,
                    StepSuccess(output=iteration_output),
                )
                loop_iteration_count[request.step_id] += 1
            else:
                # Simple reason step
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
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/99"}),
                )

    return run, steps_executed, reason_outputs


class TestThreadFieldThroughPipeline:
    """Thread VENUE_CAPACITY through a 3-model pipeline."""

    def test_workflow_completes(self):
        run, steps_executed, _ = run_workflow()
        assert run.status == "completed"

    def test_total_step_executions(self):
        """7 logical steps, but loop steps execute 3x each = 11 total."""
        _, steps_executed, _ = run_workflow()
        # resolve_source_column, trace_pipeline_lineage, show_plan,
        # modify_each_model x3, update_each_schema x3,
        # validate_pipeline, create_pr
        assert len(steps_executed) == 11

    def test_loop_steps_execute_3_times_each(self):
        _, steps_executed, _ = run_workflow()
        assert steps_executed.count("modify_each_model") == CHAIN_LENGTH
        assert steps_executed.count("update_each_schema") == CHAIN_LENGTH

    def test_column_info_resolved(self):
        _, _, reason_outputs = run_workflow()
        col = reason_outputs["resolve_source_column"]["column_info"]
        assert col["name"] == "VENUE_CAPACITY"
        assert col["sf_type"] == "NUMBER(10,0)"

    def test_pipeline_chain_has_3_models(self):
        _, _, reason_outputs = run_workflow()
        chain = reason_outputs["trace_pipeline_lineage"]["models_in_chain"]
        assert len(chain) == CHAIN_LENGTH
        assert chain[0]["model_name"] == "stg_venues"
        assert chain[1]["model_name"] == "int_venues_enriched"
        assert chain[2]["model_name"] == "fct_event_sales"

    def test_all_3_models_modified(self):
        _, _, reason_outputs = run_workflow()
        modifications = reason_outputs["modify_each_model"]
        assert len(modifications) == CHAIN_LENGTH
        model_names = [m["model_name"] for m in modifications]
        assert model_names == ["stg_venues", "int_venues_enriched", "fct_event_sales"]

    def test_modified_sql_contains_column(self):
        _, _, reason_outputs = run_workflow()
        for mod in reason_outputs["modify_each_model"]:
            assert "venue_capacity" in mod["modified_sql"].lower()

    def test_all_3_schemas_updated(self):
        _, _, reason_outputs = run_workflow()
        schemas = reason_outputs["update_each_schema"]
        assert len(schemas) == CHAIN_LENGTH
        model_names = [s["model_name"] for s in schemas]
        assert model_names == ["stg_venues", "int_venues_enriched", "fct_event_sales"]

    def test_schema_yml_contains_column(self):
        _, _, reason_outputs = run_workflow()
        for schema in reason_outputs["update_each_schema"]:
            assert "venue_capacity" in schema["schema_yml_content"].lower()

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow()
        assert reason_outputs["validate_pipeline"]["compile_ok"] is True
        assert reason_outputs["validate_pipeline"]["tests_ok"] is True
        assert reason_outputs["validate_pipeline"]["errors"] == []

    def test_golden_match(self):
        _, _, reason_outputs = run_workflow()
        with open(GOLDEN_DIR / "pipeline_threading_result.json") as f:
            golden = json.load(f)
        assert reason_outputs["trace_pipeline_lineage"]["chain_length"] == golden["chain_length"]
        model_names = [m["model_name"] for m in reason_outputs["modify_each_model"]]
        assert model_names == golden["models_modified"]

    def test_trace_has_all_step_executions(self):
        run, _, _ = run_workflow()
        trace = run.get_trace()
        assert trace["workflow_id"] == "thread-field-through-pipeline"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 11

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("thread-field-through-pipeline", INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_source_column"
        assert request.persona.id == "analytics_engineer"
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_show_plan_requires_approval(self):
        """Verify show_plan is a delegate with approval gate."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes()
        run = engine.start("thread-field-through-pipeline", INPUTS)

        # Walk through first 2 REASON steps
        for step_id in ["resolve_source_column", "trace_pipeline_lineage"]:
            request = run.next_step()
            assert request.step_id == step_id
            run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        # Step 3 should be show_plan (DelegateRequest)
        request = run.next_step()
        assert isinstance(request, DelegateRequest)
        assert request.step_id == "show_plan"
        assert request.requires_approval is True
