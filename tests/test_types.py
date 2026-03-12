"""Tests for DCAG type definitions."""
from dcag.types import (
    Budget,
    ContextBundle,
    DecisionLog,
    DelegateRequest,
    ExecuteTemplateRequest,
    ManifestEntry,
    PersonaBundle,
    ReasonRequest,
    StepDef,
    StepFailure,
    StepSkipped,
    StepSuccess,
    ToolDirective,
    Trace,
    TraceStep,
    WorkflowDef,
)


class TestToolDirective:
    def test_minimal(self):
        td = ToolDirective(name="sf.describe", instruction="inspect")
        assert td.usage_pattern is None

    def test_full(self):
        td = ToolDirective(name="sf.describe", instruction="inspect", usage_pattern="1. query\n2. check")
        assert "query" in td.usage_pattern


class TestBudget:
    def test_defaults(self):
        b = Budget()
        assert b.max_llm_turns == 5
        assert b.max_tokens == 10000
        assert b.max_retries == 2


class TestStepRequest:
    def test_reason_request(self):
        rr = ReasonRequest(
            step_id="test",
            persona=PersonaBundle(id="ae", name="AE", description="", domain_knowledge=[], heuristics=[], anti_patterns=[], quality_standards={}),
            instruction="do something",
            context=ContextBundle(static={}, dynamic={}, domain_knowledge=[], estimated_tokens=0),
            tools=[],
            output_schema=None,
            quality_criteria=[],
            budget=Budget(),
        )
        assert rr.step_id == "test"

    def test_execute_template_request(self):
        etr = ExecuteTemplateRequest(step_id="gen_sql", rendered_output="SELECT 1")
        assert etr.rendered_output == "SELECT 1"
        assert etr.artifacts == []

    def test_delegate_request(self):
        dr = DelegateRequest(step_id="pr", capability="shift.create_pr", inputs={"files": []}, requires_approval=True)
        assert dr.requires_approval is True


class TestStepOutcome:
    def test_success(self):
        s = StepSuccess(output={"key": "value"})
        assert s.artifacts == []

    def test_failure(self):
        f = StepFailure(error="timeout", retryable=True, retry_count=1)
        assert f.retryable is True

    def test_skipped(self):
        s = StepSkipped(reason="condition not met")
        assert s.reason == "condition not met"


class TestTrace:
    def test_create(self):
        t = Trace(
            run_id="run-001", workflow_id="add-column", status="completed",
            inputs={"model": "test"}, started_at="2026-03-12T10:00:00Z",
            completed_at="2026-03-12T10:01:00Z", steps=[], config_snapshot="sha256:abc",
        )
        assert t.status == "completed"


class TestWorkflowDef:
    def test_create(self):
        wd = WorkflowDef(id="test", name="Test", persona="ae", inputs={}, steps=[])
        assert wd.persona == "ae"


class TestManifestEntry:
    def test_create(self):
        me = ManifestEntry(id="add-column", name="Add Column", persona="ae", keywords=["add column"])
        assert "add column" in me.keywords
