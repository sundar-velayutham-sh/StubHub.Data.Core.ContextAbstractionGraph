"""DCAG type definitions — the typed contract between DCAG and its driver."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol


# ============================================================
# Protocols (extension points)
# ============================================================

class Validator(Protocol):
    """Structural validator for step outputs."""
    name: str
    def validate(self, output: Any, config: dict) -> bool: ...
    def describe_failure(self, output: Any, config: dict) -> str: ...


class ScriptRunner(Protocol):
    """Runner for execute/script steps."""
    def run(self, inputs: dict[str, Any]) -> dict[str, Any]: ...


# ============================================================
# Tool + Persona + Context
# ============================================================

@dataclass(frozen=True)
class ToolDirective:
    """A tool the step is allowed to use, with prescriptive guidance."""
    name: str
    instruction: str
    usage_pattern: str | None = None


@dataclass(frozen=True)
class PersonaBundle:
    """Merged persona defaults + step-level overrides."""
    id: str
    name: str
    description: str
    domain_knowledge: list[str]
    heuristics: list[str]
    anti_patterns: list[str]
    quality_standards: dict[str, str]


@dataclass(frozen=True)
class ContextBundle:
    """All context assembled for a step."""
    static: dict[str, Any]
    dynamic: dict[str, Any]
    domain_knowledge: list[str]
    estimated_tokens: int


@dataclass(frozen=True)
class Budget:
    """Resource limits for a reason step."""
    max_llm_turns: int = 5
    max_tokens: int = 10000
    max_time_ms: int = 30000
    max_retries: int = 2


# ============================================================
# Step Requests (returned by next_step() — driver pattern-matches)
# ============================================================

@dataclass(frozen=True)
class ReasonRequest:
    """Step needs LLM reasoning. Driver makes the LLM call."""
    step_id: str
    persona: PersonaBundle
    instruction: str
    context: ContextBundle
    tools: list[ToolDirective]
    output_schema: dict | None
    quality_criteria: list[str]
    budget: Budget


@dataclass(frozen=True)
class ExecuteTemplateRequest:
    """Step rendered a template. Driver just acknowledges."""
    step_id: str
    rendered_output: str
    artifacts: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExecuteScriptRequest:
    """Step needs a script/command executed. Driver runs and returns output."""
    step_id: str
    script: str
    inputs: dict[str, Any]
    fallback_on_failure: str | None = None


@dataclass(frozen=True)
class DelegateRequest:
    """Step needs driver-native capability (e.g., PR creation)."""
    step_id: str
    capability: str
    inputs: dict[str, Any]
    requires_approval: bool


StepRequest = ReasonRequest | ExecuteTemplateRequest | ExecuteScriptRequest | DelegateRequest


# ============================================================
# Step Outcomes (passed to record_result() by driver)
# ============================================================

@dataclass
class StepSuccess:
    """Step completed successfully."""
    output: dict | str
    artifacts: list[str] = field(default_factory=list)


@dataclass
class StepFailure:
    """Step failed."""
    error: str
    retryable: bool = False
    retry_count: int = 0


@dataclass
class StepSkipped:
    """Step was skipped."""
    reason: str


StepOutcome = StepSuccess | StepFailure | StepSkipped


# ============================================================
# Decision + Trace
# ============================================================

@dataclass(frozen=True)
class DecisionLog:
    """Structured rationale captured in trace."""
    decision: str
    rationale: str
    alternatives_considered: list[dict[str, str]]
    confidence: str
    informed_by: list[str]


@dataclass
class TraceStep:
    """One step's record in the execution trace."""
    step_id: str
    mode: str
    status: str
    started_at: str
    duration_ms: int
    output: dict | str | None = None
    decision_log: dict | None = None
    tool_calls: list[dict] | None = None
    token_usage: dict | None = None
    error: str | None = None


@dataclass
class Trace:
    """Full execution trace for a workflow run."""
    run_id: str
    workflow_id: str
    status: str
    inputs: dict
    started_at: str
    completed_at: str | None
    steps: list[TraceStep]
    config_snapshot: str


# ============================================================
# Workflow + Manifest (parsed from YAML)
# ============================================================

@dataclass(frozen=True)
class StepDef:
    """Parsed step definition from workflow YAML."""
    id: str
    mode: Literal["reason", "execute"]
    execute_type: Literal["template", "script", "delegate"] | None
    template: str | None
    script: str | None
    delegate: str | None
    tools: list[ToolDirective]
    instruction: str | None
    context_static: list[str]
    context_dynamic: list[dict]
    context_knowledge: list[str]
    heuristics: list[str]
    anti_patterns: list[str]
    quality_criteria: list[str]
    output_schema: dict | None
    validation: list[dict]
    requires_approval: bool
    budget: Budget | None
    transitions: list[dict] | None
    fallback_on_failure: str | None = None


@dataclass(frozen=True)
class WorkflowDef:
    """Parsed workflow definition from YAML."""
    id: str
    name: str
    persona: str
    inputs: dict[str, dict]
    steps: list[StepDef]


@dataclass(frozen=True)
class ManifestEntry:
    """Entry in the workflow registry for intent routing."""
    id: str
    name: str
    persona: str
    keywords: list[str]
    input_pattern: str | None = None
