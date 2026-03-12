"""Tests for the Shift integration driver.

Validates prompt assembly, delegate routing, capability detection,
and observability event emission — all without real LLM calls.
"""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dcag import DCAGEngine
from dcag.types import (
    Budget,
    ContextBundle,
    DelegateRequest,
    ExecuteScriptRequest,
    PersonaBundle,
    ReasonRequest,
    StepSuccess,
    ToolDirective,
)
from dcag.drivers.shift import ShiftDriver


CONTENT_DIR = Path(__file__).parent.parent / "content"


@pytest.fixture
def persona():
    return PersonaBundle(
        id="analytics_engineer",
        name="Analytics Engineer",
        description="Builds dbt models and ensures data quality.",
        domain_knowledge=["dbt models follow ref() for dependencies"],
        heuristics=["Always add not_null tests for PKs"],
        anti_patterns=["Don't SELECT *"],
        quality_standards={"naming": "snake_case"},
    )


@pytest.fixture
def tools():
    return [
        ToolDirective(
            name="snowflake_mcp.execute_query",
            instruction="Run SQL against Snowflake",
            usage_pattern="SELECT ... FROM ... WHERE ...",
        ),
        ToolDirective(
            name="github_cli.read_file",
            instruction="Read a file from the repo",
        ),
    ]


@pytest.fixture
def reason_request(persona, tools):
    return ReasonRequest(
        step_id="resolve_model",
        persona=persona,
        instruction="Find the dbt model file for the given model name.",
        context=ContextBundle(
            static={"dbt_project_structure": {"models_dir": "models/"}},
            dynamic={"source_table_fqn": "DW.CORE.EVENTS"},
            domain_knowledge=["dbt models follow ref()"],
            estimated_tokens=500,
        ),
        tools=tools,
        output_schema={"type": "object", "required": ["model_path"]},
        quality_criteria=["Model path is a real file path"],
        budget=Budget(max_llm_turns=5),
    )


@pytest.fixture
def driver():
    return ShiftDriver()


class TestPromptAssembly:
    """Verify the Shift driver assembles prompts in the correct format."""

    def test_tool_block_comes_first(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        tool_pos = prompt.find("[TOOLS")
        persona_pos = prompt.find("[PERSONA]")
        task_pos = prompt.find("[TASK]")
        assert tool_pos < persona_pos < task_pos, (
            "Tool block must come before persona, which comes before task"
        )

    def test_tool_block_lists_all_tools(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "snowflake_mcp.execute_query" in prompt
        assert "github_cli.read_file" in prompt

    def test_tool_block_includes_usage_pattern(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "SELECT ... FROM ... WHERE ..." in prompt

    def test_persona_section_present(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "[PERSONA]" in prompt
        assert "Analytics Engineer" in prompt
        assert "Builds dbt models" in prompt

    def test_domain_knowledge_in_persona(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "dbt models follow ref()" in prompt

    def test_heuristics_in_prompt(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "Always add not_null tests for PKs" in prompt

    def test_anti_patterns_in_prompt(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "Don't SELECT *" in prompt

    def test_task_section_has_instruction(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        task_start = prompt.find("[TASK]")
        context_start = prompt.find("[CONTEXT]")
        task_section = prompt[task_start:context_start]
        assert "Find the dbt model file" in task_section

    def test_context_section_has_static_and_dynamic(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "Static knowledge:" in prompt or "static" in prompt.lower()
        assert "dbt_project_structure" in prompt
        assert "source_table_fqn" in prompt

    def test_output_section_has_schema(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "[OUTPUT]" in prompt
        assert "model_path" in prompt

    def test_quality_criteria_in_prompt(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "Model path is a real file path" in prompt

    def test_budget_in_prompt(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        assert "[BUDGET]" in prompt
        assert "5" in prompt  # max_llm_turns

    def test_no_tools_prompt(self, driver, persona):
        request = ReasonRequest(
            step_id="determine_logic",
            persona=persona,
            instruction="Classify the column intent.",
            context=ContextBundle(static={}, dynamic={}, domain_knowledge=[], estimated_tokens=100),
            tools=[],
            output_schema=None,
            quality_criteria=[],
            budget=Budget(),
        )
        prompt = driver.assemble_prompt(request)
        assert "[TOOLS" in prompt
        assert "No tools available" in prompt or "ONLY" in prompt


class TestDelegateRouting:
    """Verify delegate requests route to correct Shift capabilities."""

    def test_show_plan_routing(self, driver):
        request = DelegateRequest(
            step_id="show_plan",
            capability="shift.show_plan",
            inputs={"model_path": "models/stg_x.sql", "workflow_inputs": {"model_name": "stg_x"}},
            requires_approval=True,
        )
        action = driver.route_delegate(request)
        assert action["capability"] == "show_plan"
        assert action["requires_approval"] is True
        assert "model_path" in action["inputs"]

    def test_create_pr_routing(self, driver):
        request = DelegateRequest(
            step_id="create_pr",
            capability="shift.create_pr",
            inputs={
                "modified_sql": "SELECT 1",
                "modified_yml": "- name: col",
                "workflow_inputs": {"model_name": "stg_x"},
            },
            requires_approval=True,
        )
        action = driver.route_delegate(request)
        assert action["capability"] == "create_pr"
        assert action["requires_approval"] is True

    def test_unknown_delegate_raises(self, driver):
        request = DelegateRequest(
            step_id="unknown",
            capability="shift.unknown_cap",
            inputs={},
            requires_approval=False,
        )
        with pytest.raises(ValueError, match="Unknown delegate capability"):
            driver.route_delegate(request)


class TestCapabilityDetection:
    """Verify the driver interprets step 0 capabilities correctly."""

    def test_full_capabilities(self, driver):
        caps = driver.parse_capabilities({
            "dbt_project_path": "/tmp/astro",
            "setup_mode": "full",
            "dbt_available": True,
            "dbt_mcp_available": True,
            "fallback_mode": "full",
        })
        assert caps["dbt_available"] is True
        assert caps["dbt_mcp_available"] is True
        assert caps["fallback_mode"] == "full"

    def test_degraded_capabilities(self, driver):
        caps = driver.parse_capabilities({
            "dbt_project_path": "/tmp/astro",
            "setup_mode": "degraded",
            "dbt_available": True,
            "dbt_mcp_available": False,
            "fallback_mode": "snowflake_only",
        })
        assert caps["dbt_available"] is True
        assert caps["dbt_mcp_available"] is False
        assert caps["fallback_mode"] == "snowflake_only"

    def test_full_failure_capabilities(self, driver):
        caps = driver.parse_capabilities({
            "dbt_project_path": "",
            "dbt_available": False,
            "dbt_mcp_available": False,
            "fallback_mode": "snowflake_only",
        })
        assert caps["dbt_available"] is False
        assert caps["dbt_mcp_available"] is False


class TestTokenEstimation:
    """Verify prompt token estimation."""

    def test_prompt_has_positive_tokens(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        estimate = driver.estimate_prompt_tokens(prompt)
        assert estimate > 0

    def test_token_estimate_proportional_to_length(self, driver, reason_request):
        prompt = driver.assemble_prompt(reason_request)
        estimate = driver.estimate_prompt_tokens(prompt)
        # ~4 chars per token
        assert abs(estimate - len(prompt) // 4) < 10


class TestObservabilityEvents:
    """Verify the driver emits typed observability events."""

    def test_step_started_event(self, driver, reason_request):
        event = driver.emit_step_started(reason_request.step_id, "reason")
        assert event["type"] == "step_started"
        assert event["step_id"] == "resolve_model"
        assert event["mode"] == "reason"
        assert "timestamp" in event

    def test_context_assembled_event(self, driver, reason_request):
        event = driver.emit_context_assembled(reason_request)
        assert event["type"] == "context_assembled"
        assert event["step_id"] == "resolve_model"
        assert "snapshot" in event
        assert event["snapshot"]["persona"] == "analytics_engineer"
        assert "timestamp" in event

    def test_tool_resolved_event(self, driver, reason_request):
        event = driver.emit_tool_resolved(
            reason_request.step_id,
            requested=["snowflake_mcp.execute_query", "github_cli.read_file"],
            available=["snowflake_mcp.execute_query"],
        )
        assert event["type"] == "tool_resolved"
        assert len(event["requested"]) == 2
        assert len(event["available"]) == 1

    def test_result_recorded_event(self, driver):
        event = driver.emit_result_recorded("resolve_model", "completed", 1500)
        assert event["type"] == "result_recorded"
        assert event["duration_ms"] == 1500


class TestEndToEndWithEngine:
    """Integration test: Shift driver works with real DCAGEngine."""

    def test_driver_handles_full_workflow(self, driver):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-column-to-model", {
            "model_name": "stg_test",
            "column_name": "new_col",
        })

        steps_handled = []
        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            steps_handled.append(request.step_id)

            if isinstance(request, ExecuteScriptRequest):
                run.record_result(request.step_id, StepSuccess(output={
                    "dbt_project_path": "/tmp/test",
                    "dbt_available": True,
                    "dbt_mcp_available": True,
                    "fallback_mode": "full",
                }))
            elif isinstance(request, ReasonRequest):
                # Driver assembles prompt (verified separately)
                prompt = driver.assemble_prompt(request)
                assert len(prompt) > 0

                # Mock LLM response
                dummy_outputs = {
                    "resolve_model": {
                        "model_path": "models/stg_test.sql",
                        "sources_yml_path": "models/sources.yml",
                        "source_ref": "{{ source('raw', 'test') }}",
                        "existing_columns": ["COL_A"],
                        "source_table_fqn": "RAW.PUBLIC.TEST",
                    },
                    "discover_column": {
                        "column_info": {"name": "NEW_COL", "source_type": "VARCHAR", "sf_type": "VARCHAR", "nullable": True},
                    },
                    "determine_logic": {
                        "intent_level": "passthrough", "column_expression": "NEW_COL", "join_required": False,
                    },
                    "check_downstream_impact": {
                        "downstream_impact": {"affected_models": [], "select_star_risk": False, "impact_level": "safe"},
                    },
                    "modify_staging_sql": {"modified_sql": "SELECT COL_A, NEW_COL FROM src", "changes_made": ["added NEW_COL"]},
                    "update_schema_yml": {"modified_yml": "- name: new_col", "changes_made": ["added new_col"]},
                    "validate": {"tests_passed": True, "compile_ok": True, "parse_ok": True},
                }
                run.record_result(request.step_id, StepSuccess(output=dummy_outputs.get(request.step_id, {})))
            elif isinstance(request, DelegateRequest):
                action = driver.route_delegate(request)
                run.record_result(request.step_id, StepSuccess(output={"approved": True}))

        assert run.status == "completed"
        assert len(steps_handled) == 10

    def test_driver_handles_table_optimizer_workflow(self, driver):
        """Integration: ShiftDriver walks the full 9-step table-optimizer workflow."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassette_dir = Path(__file__).parent / "cassettes" / "table-optimizer"

        expected_steps = [
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
        reason_steps = [s for s in expected_steps if s not in ("show_recommendations", "apply_changes")]

        # Load cassettes for the 7 reason steps
        cassettes = {}
        for step_id in reason_steps:
            with open(cassette_dir / f"{step_id}.json") as f:
                cassettes[step_id] = json.load(f)

        run = engine.start("table-optimizer", {"table_name": "TRANSACTION"})
        assert run.status == "running"

        steps_handled = []
        prompts_assembled = []
        delegates_routed = []

        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            steps_handled.append(request.step_id)

            if isinstance(request, ReasonRequest):
                # Driver assembles prompt
                prompt = driver.assemble_prompt(request)
                assert len(prompt) > 0
                prompts_assembled.append(request.step_id)

                # Use cassette output
                cassette = cassettes[request.step_id]
                run.record_result(
                    request.step_id,
                    StepSuccess(output=cassette["output"]),
                )
            elif isinstance(request, DelegateRequest):
                # Driver routes delegate
                action = driver.route_delegate(request)
                delegates_routed.append(request.step_id)
                assert "capability" in action
                assert "requires_approval" in action

                # Dummy approval for delegate steps
                if request.step_id == "show_recommendations":
                    run.record_result(
                        request.step_id,
                        StepSuccess(output={"approved": True, "user_feedback": None}),
                    )
                elif request.step_id == "apply_changes":
                    run.record_result(
                        request.step_id,
                        StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/99"}),
                    )

        # Workflow completed all 9 steps
        assert run.status == "completed"
        assert len(steps_handled) == 9
        assert steps_handled == expected_steps

        # Driver assembled prompts for all 7 reason steps
        assert len(prompts_assembled) == 7
        assert set(prompts_assembled) == set(reason_steps)

        # Driver routed both delegate steps
        assert len(delegates_routed) == 2
        assert set(delegates_routed) == {"show_recommendations", "apply_changes"}
