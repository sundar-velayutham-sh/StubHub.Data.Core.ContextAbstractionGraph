"""Tests for context assembly."""
from pathlib import Path
import pytest
from dcag._context import ContextAssembler
from dcag._loaders import PersonaLoader, KnowledgeLoader, WorkflowLoader
from dcag._registry import ToolRegistry
from dcag.types import ReasonRequest, ContextBundle

CONTENT_DIR = Path(__file__).parent.parent / "content"


@pytest.fixture
def assembler():
    return ContextAssembler(
        persona_loader=PersonaLoader(CONTENT_DIR / "personas"),
        knowledge_loader=KnowledgeLoader(CONTENT_DIR / "knowledge"),
    )


@pytest.fixture
def workflow():
    return WorkflowLoader(CONTENT_DIR / "workflows").load("add-column-to-model")


class TestContextAssembler:
    def test_build_static(self, assembler):
        static = assembler.build_static(["sf_type_mapping", "naming_conventions"])
        assert "sf_type_mapping" in static
        assert "naming_conventions" in static

    def test_build_dynamic_with_select(self, assembler):
        refs = [{"from": "discover_column", "select": "column_info"}]
        prior = {"discover_column": {"column_info": {"name": "pcid"}, "extra": "ignored"}}
        dynamic = assembler.build_dynamic(refs, prior)
        assert dynamic["column_info"]["name"] == "pcid"
        assert "extra" not in dynamic

    def test_build_dynamic_whole_step(self, assembler):
        refs = [{"from": "determine_logic"}]
        prior = {"determine_logic": {"intent_level": "passthrough", "join_required": False}}
        dynamic = assembler.build_dynamic(refs, prior)
        assert "determine_logic" in dynamic

    def test_build_dynamic_missing_raises(self, assembler):
        refs = [{"from": "nonexistent", "select": "field"}]
        with pytest.raises(KeyError, match="nonexistent"):
            assembler.build_dynamic(refs, {})

    def test_assemble_reason_request(self, assembler, workflow):
        step = workflow.steps[1]  # resolve_model (first REASON step)
        persona = PersonaLoader(CONTENT_DIR / "personas").load("analytics_engineer")
        request = assembler.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_model"
        assert request.persona.id == "analytics_engineer"
        # Tools gated to this step
        assert len(request.tools) > 0
        # Static knowledge loaded
        assert "dbt_project_structure" in request.context.static
        # Tokens estimated
        assert request.context.estimated_tokens > 0


@pytest.fixture
def registry_degraded():
    """Registry in snowflake-only mode."""
    reg = ToolRegistry()
    reg.update_capabilities({
        "dbt_available": False,
        "dbt_mcp_available": False,
        "github_available": False,
    })
    return reg


@pytest.fixture
def assembler_with_registry(registry_degraded):
    return ContextAssembler(
        persona_loader=PersonaLoader(CONTENT_DIR / "personas"),
        knowledge_loader=KnowledgeLoader(CONTENT_DIR / "knowledge"),
        registry=registry_degraded,
    )


class TestToolFiltering:
    def test_degraded_mode_filters_dbt_and_github(self, assembler_with_registry, workflow):
        """In snowflake_only mode, only snowflake tools appear in ReasonRequest."""
        step = workflow.steps[1]  # resolve_model: has dbt + github + snowflake tools
        persona = PersonaLoader(CONTENT_DIR / "personas").load("analytics_engineer")
        request = assembler_with_registry.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        tool_names = [t.name for t in request.tools]
        assert "snowflake_mcp.execute_query" in tool_names
        assert "dbt_mcp.get_node_details_dev" not in tool_names
        assert "github_cli.search_code" not in tool_names
        assert "github_cli.read_file" not in tool_names

    def test_full_mode_keeps_all_tools(self, workflow):
        """With no registry (default), all tools pass through."""
        assembler = ContextAssembler(
            persona_loader=PersonaLoader(CONTENT_DIR / "personas"),
            knowledge_loader=KnowledgeLoader(CONTENT_DIR / "knowledge"),
        )
        step = workflow.steps[1]  # resolve_model
        persona = PersonaLoader(CONTENT_DIR / "personas").load("analytics_engineer")
        request = assembler.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        assert len(request.tools) == 4

    def test_token_estimate_uses_filtered_tools(self, assembler_with_registry, workflow):
        """Token estimate should reflect only available tools, not all declared tools."""
        assembler_full = ContextAssembler(
            persona_loader=PersonaLoader(CONTENT_DIR / "personas"),
            knowledge_loader=KnowledgeLoader(CONTENT_DIR / "knowledge"),
        )
        step = workflow.steps[1]
        persona = PersonaLoader(CONTENT_DIR / "personas").load("analytics_engineer")
        request_full = assembler_full.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        request_degraded = assembler_with_registry.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        assert request_degraded.context.estimated_tokens < request_full.context.estimated_tokens


class TestSchemaCache:
    """Tests for schema cache assembly."""

    def test_build_cache_returns_matching_entries(self, assembler):
        """build_cache returns only the requested cache keys."""
        cache = {"table_columns": {"col1": "VARCHAR"}, "storage_metrics": {"bytes": 1024}}
        result = assembler.build_cache(["table_columns"], cache)
        assert "table_columns" in result
        assert "storage_metrics" not in result

    def test_build_cache_empty_refs(self, assembler):
        """No cache refs returns empty dict."""
        cache = {"table_columns": {"col1": "VARCHAR"}}
        result = assembler.build_cache([], cache)
        assert result == {}

    def test_build_cache_missing_key_skipped(self, assembler):
        """Missing cache key is silently skipped (not an error)."""
        cache = {"table_columns": {"col1": "VARCHAR"}}
        result = assembler.build_cache(["table_columns", "nonexistent"], cache)
        assert "table_columns" in result
        assert "nonexistent" not in result

    def test_build_cache_empty_cache(self, assembler):
        """Empty cache with refs returns empty dict."""
        result = assembler.build_cache(["table_columns"], {})
        assert result == {}

    def test_build_cache_all_keys(self, assembler):
        """Multiple keys all found."""
        cache = {
            "table_columns": {"col1": "VARCHAR"},
            "storage_metrics": {"bytes": 1024},
            "row_count": 50000,
        }
        result = assembler.build_cache(["table_columns", "storage_metrics", "row_count"], cache)
        assert len(result) == 3
