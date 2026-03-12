"""Tests for YAML loaders."""
from pathlib import Path
import pytest
from dcag._loaders import PersonaLoader, KnowledgeLoader, WorkflowLoader
from dcag.types import PersonaBundle, WorkflowDef, ManifestEntry

CONTENT_DIR = Path(__file__).parent.parent / "content"


class TestPersonaLoader:
    def test_load(self):
        loader = PersonaLoader(CONTENT_DIR / "personas")
        p = loader.load("analytics_engineer")
        assert isinstance(p, PersonaBundle)
        assert p.id == "analytics_engineer"
        assert len(p.heuristics) == 4
        assert len(p.anti_patterns) == 4

    def test_not_found(self):
        loader = PersonaLoader(CONTENT_DIR / "personas")
        with pytest.raises(FileNotFoundError):
            loader.load("nonexistent")

    def test_merge(self):
        loader = PersonaLoader(CONTENT_DIR / "personas")
        base = loader.load("analytics_engineer")
        merged = loader.merge(
            base,
            step_heuristics=["step heuristic"],
            step_anti_patterns=["step anti-pattern"],
            step_knowledge=["step knowledge item"],
        )
        # Step items come FIRST (more specific)
        assert merged.heuristics[0] == "step heuristic"
        assert merged.anti_patterns[0] == "step anti-pattern"
        assert merged.domain_knowledge[-1] == "step knowledge item"
        # Originals still present
        assert len(merged.heuristics) == 5
        assert len(merged.anti_patterns) == 5


class TestKnowledgeLoader:
    def test_load(self):
        loader = KnowledgeLoader(CONTENT_DIR / "knowledge")
        k = loader.load("sf_type_mapping")
        assert k["id"] == "sf_type_mapping"
        assert "rules" in k

    def test_load_multiple(self):
        loader = KnowledgeLoader(CONTENT_DIR / "knowledge")
        result = loader.load_multiple(["sf_type_mapping", "naming_conventions"])
        assert len(result) == 2

    def test_not_found(self):
        loader = KnowledgeLoader(CONTENT_DIR / "knowledge")
        with pytest.raises(FileNotFoundError):
            loader.load("nonexistent")

    def test_estimate_tokens(self):
        loader = KnowledgeLoader(CONTENT_DIR / "knowledge")
        k = loader.load("sf_type_mapping")
        tokens = loader.estimate_tokens(k)
        assert 100 < tokens < 2000


class TestWorkflowLoader:
    def test_load(self):
        loader = WorkflowLoader(CONTENT_DIR / "workflows")
        wf = loader.load("add-column-to-model")
        assert isinstance(wf, WorkflowDef)
        assert wf.id == "add-column-to-model"
        assert wf.persona == "analytics_engineer"
        assert len(wf.steps) == 10

    def test_step_modes(self):
        loader = WorkflowLoader(CONTENT_DIR / "workflows")
        wf = loader.load("add-column-to-model")
        modes = [(s.id, s.mode) for s in wf.steps]
        assert modes == [
            ("setup_dbt_project", "execute"),       # Step 0
            ("resolve_model", "reason"),             # Step 1
            ("discover_column", "reason"),           # Step 2
            ("determine_logic", "reason"),           # Step 3
            ("check_downstream_impact", "reason"),   # Step 4
            ("show_plan", "execute"),                # Step 5
            ("modify_staging_sql", "reason"),        # Step 6
            ("update_schema_yml", "reason"),         # Step 7
            ("validate", "reason"),                  # Step 8
            ("create_pr", "execute"),                # Step 9
        ]

    def test_execute_types(self):
        loader = WorkflowLoader(CONTENT_DIR / "workflows")
        wf = loader.load("add-column-to-model")
        execute_steps = [(s.id, s.execute_type) for s in wf.steps if s.mode == "execute"]
        assert execute_steps == [
            ("setup_dbt_project", "script"),
            ("show_plan", "delegate"),
            ("create_pr", "delegate"),
        ]

    def test_reason_steps_have_tools_except_determine_logic(self):
        loader = WorkflowLoader(CONTENT_DIR / "workflows")
        wf = loader.load("add-column-to-model")
        for step in wf.steps:
            if step.mode == "reason" and step.id != "determine_logic":
                assert len(step.tools) > 0, f"{step.id} has no tools"
        # determine_logic has no tools — it classifies intent from prior step context
        determine = next(s for s in wf.steps if s.id == "determine_logic")
        assert len(determine.tools) == 0

    def test_load_manifest(self):
        loader = WorkflowLoader(CONTENT_DIR / "workflows")
        manifest = loader.load_manifest()
        assert len(manifest) >= 1
        assert isinstance(manifest[0], ManifestEntry)
        assert "add column" in manifest[0].keywords

    def test_not_found(self):
        loader = WorkflowLoader(CONTENT_DIR / "workflows")
        with pytest.raises(FileNotFoundError):
            loader.load("nonexistent")
