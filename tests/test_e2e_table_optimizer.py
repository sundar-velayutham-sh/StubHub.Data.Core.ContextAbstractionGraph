"""
End-to-end test for the table-optimizer workflow.

Tests the full 9-step workflow with cassette responses, verifying
the engine walks all steps and produces a correct optimization report.

2 test classes:
  - TestTableOptimizer: daily-loaded table → CLUSTER_BY
  - TestTableOptimizerHourlySkip: sub-hourly table → SKIP
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

# All 9 steps in execution order
EXPECTED_STEPS = [
    "identify_table",
    "detect_load_frequency",
    "analyze_query_patterns",
    "assess_clustering",
    "check_partitioning",
    "analyze_materialization",
    "show_recommendations",
    "generate_report",
    "apply_changes",
]

# 7 REASON steps that need cassettes
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("show_recommendations", "apply_changes")]


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 7 cassettes for the table-optimizer test."""
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

    run = engine.start("table-optimizer", inputs)
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
            if request.step_id == "show_recommendations":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"approved": True, "user_feedback": None}),
                )
            elif request.step_id == "apply_changes":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/42"}),
                )

    return run, steps_executed, reason_outputs


class TestTableOptimizer:
    """Daily-loaded table → CLUSTER_BY recommendation."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "table-optimizer"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "table-optimizer"
    INPUTS = {"table_name": "TRANSACTION", "database": "DW", "schema": "RPT"}

    def test_workflow_completes_9_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 9
        assert steps_executed == EXPECTED_STEPS

    def test_strategy_is_cluster_by(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["generate_report"]["strategy"] == "CLUSTER_BY"

    def test_load_frequency_is_daily(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        freq = reason_outputs["detect_load_frequency"]
        assert freq["load_frequency"] == "DAILY"
        assert freq["skip_clustering"] is False
        assert "daily" in freq["tags"]

    def test_report_has_implementation_sql(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        sql = reason_outputs["generate_report"]["implementation_sql"]
        assert "ALTER TABLE" in sql
        assert "CLUSTER BY" in sql
        assert "EVENT_DATE" in sql

    def test_report_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        with open(self.GOLDEN_DIR / "optimization_report.json") as f:
            golden = json.load(f)
        report = reason_outputs["generate_report"]
        assert report["strategy"] == golden["strategy"]
        assert report["implementation_sql"] == golden["implementation_sql"]

    def test_table_fqn_is_correct(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["identify_table"]["table_fqn"] == "DW.RPT.TRANSACTION"

    def test_query_patterns_detected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        patterns = reason_outputs["analyze_query_patterns"]
        assert patterns["query_count"] > 0
        assert len(patterns["filter_columns"]) > 0
        top_col = patterns["filter_columns"][0]
        assert top_col["name"] == "EVENT_DATE"
        assert top_col["frequency_pct"] > 80

    def test_clustering_recommendation(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        clustering = reason_outputs["assess_clustering"]
        assert clustering["current_clustering"] is None
        assert "EVENT_DATE" in clustering["optimal_clustering"]
        assert clustering["clustering_depth"] > 2

    def test_materialization_is_dbt_managed(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        mat = reason_outputs["analyze_materialization"]
        assert mat["is_dbt_managed"] is True
        assert mat["current_materialization"] == "incremental"

    def test_report_has_caveats(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        caveats = reason_outputs["generate_report"]["caveats"]
        assert len(caveats) > 0

    def test_trace_has_all_9_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "table-optimizer"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 9

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("table-optimizer", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "identify_table"
        assert request.persona.id == "data_engineer"
        assert len(request.persona.heuristics) > 0
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_delegate_show_recommendations_has_approval(self):
        """Verify show_recommendations is a delegate with approval gate."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes(self.CASSETTE_DIR)
        run = engine.start("table-optimizer", self.INPUTS)

        # Walk through REASON steps until we hit show_recommendations
        for step_id in EXPECTED_STEPS[:6]:
            request = run.next_step()
            assert request.step_id == step_id
            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        # Step 6 should be show_recommendations (DelegateRequest)
        request = run.next_step()
        assert isinstance(request, DelegateRequest)
        assert request.step_id == "show_recommendations"
        assert request.requires_approval is True


class TestTableOptimizerHourlySkip:
    """Sub-hourly-loaded table → SKIP recommendation."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "table-optimizer-hourly"
    INPUTS = {"table_name": "TRANSACTION_COUPON_FACT", "database": "DW", "schema": "RPT"}

    def test_workflow_completes_9_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 9
        assert steps_executed == EXPECTED_STEPS

    def test_load_frequency_is_sub_hourly(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        freq = reason_outputs["detect_load_frequency"]
        assert freq["load_frequency"] == "SUB_HOURLY"
        assert freq["avg_runs_per_day"] > 12
        assert freq["skip_clustering"] is True
        assert "sub_hourly" in freq["tags"]

    def test_strategy_is_skip(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["generate_report"]["strategy"] == "SKIP"

    def test_skip_rationale_mentions_load_frequency(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        report = reason_outputs["generate_report"]["report"]
        assert "sub-hourly" in report["recommendation"].lower() or "sub_hourly" in str(report).lower()

    def test_no_alter_table_in_sql(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        sql = reason_outputs["generate_report"]["implementation_sql"]
        assert "ALTER TABLE" not in sql
        assert "CLUSTER BY" not in sql

    def test_caveats_mention_load_frequency(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        caveats = reason_outputs["generate_report"]["caveats"]
        caveats_text = " ".join(caveats).lower()
        assert "hourly" in caveats_text or "load" in caveats_text

    def test_alternative_recommendations_present(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        report = reason_outputs["generate_report"]["report"]
        assert "alternative_recommendations" in report
        assert len(report["alternative_recommendations"]) > 0

    def test_trace_has_all_9_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 9
