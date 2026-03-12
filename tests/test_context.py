"""Tests for context assembly."""
from pathlib import Path
import pytest
from dcag._context import ContextAssembler
from dcag._loaders import PersonaLoader, KnowledgeLoader, WorkflowLoader
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
