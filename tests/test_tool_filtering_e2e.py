"""E2E test: add-column-to-model in degraded mode filters unavailable tools."""
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.types import (
    DelegateRequest,
    ExecuteScriptRequest,
    ReasonRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"


class TestDegradedModeToolFiltering:
    """Verify that after step 0 reports degraded capabilities,
    subsequent ReasonRequests only contain available tools."""

    def test_snowflake_only_filters_dbt_and_github_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-column-to-model", {
            "model_name": "test_model",
            "column_name": "TEST_COL",
        })

        reason_tools: dict[str, list[str]] = {}

        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            if isinstance(request, ExecuteScriptRequest):
                run.record_result(request.step_id, StepSuccess(output={
                    "dbt_project_path": None,
                    "dbt_available": False,
                    "dbt_mcp_available": False,
                    "github_available": False,
                    "fallback_mode": "snowflake_only",
                }))

            elif isinstance(request, ReasonRequest):
                reason_tools[request.step_id] = [t.name for t in request.tools]

                dummy_outputs = {
                    "resolve_model": {
                        "model_path": "models/test.sql",
                        "sources_yml_path": "models/sources.yml",
                        "source_ref": "{{ source('raw', 'test') }}",
                        "existing_columns": ["COL_A"],
                        "source_table_fqn": "RAW.PUBLIC.TEST",
                    },
                    "discover_column": {
                        "column_info": {"name": "TEST_COL", "source_type": "VARCHAR", "sf_type": "VARCHAR", "nullable": True},
                    },
                    "determine_logic": {
                        "intent_level": "passthrough", "column_expression": "TEST_COL", "join_required": False,
                    },
                    "check_downstream_impact": {
                        "downstream_impact": {"affected_models": [], "select_star_risk": False, "impact_level": "safe"},
                    },
                    "modify_staging_sql": {"modified_sql": "SELECT COL_A, TEST_COL FROM src", "changes_made": ["added"]},
                    "update_schema_yml": {"modified_yml": "- name: test_col", "changes_made": ["added"]},
                    "validate": {"tests_passed": True, "compile_ok": True, "parse_ok": True},
                }
                run.record_result(request.step_id, StepSuccess(output=dummy_outputs.get(request.step_id, {})))

            elif isinstance(request, DelegateRequest):
                run.record_result(request.step_id, StepSuccess(output={"approved": True}))

        assert run.status == "completed"

        # Key assertions: no dbt or github tools in any step
        for step_id, tools in reason_tools.items():
            for tool in tools:
                assert not tool.startswith("dbt_mcp."), (
                    f"Step '{step_id}' has dbt tool '{tool}' in degraded mode"
                )
                assert not tool.startswith("github_cli."), (
                    f"Step '{step_id}' has github tool '{tool}' in degraded mode"
                )

        # resolve_model should still have snowflake tool
        assert "snowflake_mcp.execute_query" in reason_tools["resolve_model"]

        # discover_column should still have both snowflake tools
        assert "snowflake_mcp.describe_table" in reason_tools["discover_column"]
        assert "snowflake_mcp.execute_query" in reason_tools["discover_column"]

        # determine_logic has no tools (pure reasoning)
        assert reason_tools["determine_logic"] == []

        # validate should have only snowflake
        assert "snowflake_mcp.execute_query" in reason_tools["validate"]
        assert len([t for t in reason_tools["validate"] if t.startswith("dbt_mcp.")]) == 0

    def test_full_mode_keeps_all_tools(self):
        """When step 0 reports full capabilities, all tools remain."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-column-to-model", {
            "model_name": "test_model",
            "column_name": "TEST_COL",
        })

        request = run.next_step()  # step 0: ExecuteScriptRequest
        assert isinstance(request, ExecuteScriptRequest)

        run.record_result(request.step_id, StepSuccess(output={
            "dbt_project_path": "/tmp/project",
            "dbt_available": True,
            "dbt_mcp_available": True,
            "github_available": True,
            "fallback_mode": "full",
        }))

        request = run.next_step()  # step 1: resolve_model
        assert isinstance(request, ReasonRequest)
        tool_names = [t.name for t in request.tools]

        # All 4 tools present
        assert "dbt_mcp.get_node_details_dev" in tool_names
        assert "github_cli.search_code" in tool_names
        assert "github_cli.read_file" in tool_names
        assert "snowflake_mcp.execute_query" in tool_names
