"""Tests for tool capability registry."""
from dcag._registry import ToolRegistry
from dcag.types import ToolDirective


class TestToolRegistry:
    def test_all_available_when_full_mode(self):
        reg = ToolRegistry()
        reg.update_capabilities({
            "dbt_available": True,
            "dbt_mcp_available": True,
        })
        tools = [
            ToolDirective("dbt_mcp.compile", "compile model"),
            ToolDirective("snowflake_mcp.execute_query", "run query"),
        ]
        available = reg.resolve_available(tools)
        assert len(available) == 2

    def test_dbt_filtered_when_unavailable(self):
        reg = ToolRegistry()
        reg.update_capabilities({
            "dbt_available": True,
            "dbt_mcp_available": False,  # dbt-MCP not provisioned
        })
        tools = [
            ToolDirective("dbt_mcp.compile", "compile model"),
            ToolDirective("dbt_mcp.get_lineage_dev", "get lineage"),
            ToolDirective("snowflake_mcp.execute_query", "run query"),
            ToolDirective("github_cli.search_code", "search code"),
        ]
        available = reg.resolve_available(tools)
        assert len(available) == 2  # only snowflake + github
        names = [t.name for t in available]
        assert "dbt_mcp.compile" not in names
        assert "snowflake_mcp.execute_query" in names

    def test_resolution_report(self):
        reg = ToolRegistry()
        reg.update_capabilities({"dbt_available": True, "dbt_mcp_available": False})
        tools = [
            ToolDirective("dbt_mcp.compile", "compile"),
            ToolDirective("snowflake_mcp.execute_query", "query"),
        ]
        report = reg.get_resolution_report(tools)
        assert "dbt_mcp.compile" in report["filtered_out"]
        assert "snowflake_mcp.execute_query" in report["available"]

    def test_unknown_tool_defaults_available(self):
        """Tools not in the requirements map are assumed always available."""
        reg = ToolRegistry()
        tools = [ToolDirective("custom_tool.do_thing", "custom")]
        available = reg.resolve_available(tools)
        assert len(available) == 1

    def test_empty_capabilities(self):
        """Before step 0 reports, unknown capabilities default to True."""
        reg = ToolRegistry()
        tools = [ToolDirective("snowflake_mcp.execute_query", "query")]
        available = reg.resolve_available(tools)
        assert len(available) == 1

    def test_github_filtered_when_unavailable(self):
        reg = ToolRegistry()
        reg.update_capabilities({
            "dbt_available": True,
            "dbt_mcp_available": True,
            "github_available": False,
        })
        tools = [
            ToolDirective("dbt_mcp.compile", "compile model"),
            ToolDirective("github_cli.search_code", "search code"),
            ToolDirective("github_cli.read_file", "read file"),
            ToolDirective("snowflake_mcp.execute_query", "run query"),
        ]
        available = reg.resolve_available(tools)
        names = [t.name for t in available]
        assert "github_cli.search_code" not in names
        assert "github_cli.read_file" not in names
        assert "dbt_mcp.compile" in names
        assert "snowflake_mcp.execute_query" in names

    def test_snowflake_only_mode(self):
        """When both dbt and github are unavailable, only snowflake tools remain."""
        reg = ToolRegistry()
        reg.update_capabilities({
            "dbt_available": False,
            "dbt_mcp_available": False,
            "github_available": False,
        })
        tools = [
            ToolDirective("dbt_mcp.compile", "compile"),
            ToolDirective("github_cli.read_file", "read"),
            ToolDirective("snowflake_mcp.execute_query", "query"),
            ToolDirective("snowflake_mcp.describe_table", "describe"),
        ]
        available = reg.resolve_available(tools)
        names = [t.name for t in available]
        assert names == ["snowflake_mcp.execute_query", "snowflake_mcp.describe_table"]
