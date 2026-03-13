"""DCAGEngine + WorkflowRun — the public API."""
from __future__ import annotations

import hashlib
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from dcag._context import ContextAssembler
from dcag._decisions import DecisionStore
from dcag._loaders import KnowledgeLoader, PersonaLoader, WorkflowLoader
from dcag._registry import ToolRegistry
from dcag._trace import TraceWriter
from dcag._validation import validate_structural
from dcag._walker import Walker
from dcag.types import (
    DelegateRequest,
    ExecuteScriptRequest,
    ExecuteTemplateRequest,
    ManifestEntry,
    PersonaBundle,
    ReasonRequest,
    StepDef,
    StepFailure,
    StepOutcome,
    StepRequest,
    StepSkipped,
    StepSuccess,
    WorkflowDef,
)


class DCAGEngine:
    """Entry point. Loads content, creates workflow runs."""

    def __init__(self, content_dir: str | Path):
        self._content_dir = Path(content_dir)
        self._persona_loader = PersonaLoader(self._content_dir / "personas")
        self._knowledge_loader = KnowledgeLoader(self._content_dir / "knowledge")
        self._workflow_loader = WorkflowLoader(self._content_dir / "workflows")

    def start(self, workflow_id: str, inputs: dict[str, Any],
              decisions_dir: str | Path | None = None) -> WorkflowRun:
        """Start a new workflow run."""
        workflow = self._workflow_loader.load(workflow_id)
        persona = self._persona_loader.load(workflow.persona)
        run_id = f"dcag-{uuid.uuid4().hex[:8]}"
        config_hash = self._hash_content()
        registry = ToolRegistry()
        assembler = ContextAssembler(self._persona_loader, self._knowledge_loader, registry)

        return WorkflowRun(
            run_id=run_id,
            workflow=workflow,
            persona=persona,
            inputs=inputs,
            assembler=assembler,
            config_hash=config_hash,
            registry=registry,
            decisions_dir=Path(decisions_dir) if decisions_dir else None,
        )

    def list_workflows(self) -> list[ManifestEntry]:
        """List available workflows from manifest."""
        return self._workflow_loader.load_manifest()

    def _hash_content(self) -> str:
        """Hash all content files for snapshot tracking."""
        h = hashlib.sha256()
        for path in sorted(self._content_dir.rglob("*.yml")):
            h.update(path.read_bytes())
        return f"sha256:{h.hexdigest()[:12]}"


class WorkflowRun:
    """A running workflow. Driver pulls steps via next_step() and records results."""

    def __init__(
        self,
        run_id: str,
        workflow: WorkflowDef,
        persona: PersonaBundle,
        inputs: dict[str, Any],
        assembler: ContextAssembler,
        config_hash: str,
        registry: ToolRegistry | None = None,
        decisions_dir: Path | None = None,
    ):
        self._run_id = run_id
        self._workflow = workflow
        self._persona = persona
        self._inputs = inputs
        self._assembler = assembler
        self._walker = Walker(workflow.steps)
        self._prior_outputs: dict[str, Any] = {}
        self._schema_cache: dict[str, Any] = {}
        self._decision_store = DecisionStore(decisions_dir) if decisions_dir else None
        self._status = "running"
        self._trace = TraceWriter(run_id, Path(tempfile.gettempdir()) / "dcag-runs")
        self._trace.record_start(workflow.id, inputs, config_hash)
        self._step_start_time: float = 0
        self._registry = registry or ToolRegistry()

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def status(self) -> str:
        return self._status

    def next_step(self) -> StepRequest | None:
        """Get the next step as a typed request. Returns None when complete."""
        if self._walker.is_complete():
            self._status = "completed"
            self._trace.record_end("completed")
            return None

        step = self._walker.current()
        self._step_start_time = time.monotonic()

        # Initialize loop items on first encounter of a loop step
        if step.loop and not self._walker.is_in_loop():
            over_path = step.loop.get("over", "")
            parts = over_path.split(".")
            if len(parts) >= 2:
                source_step = parts[0]
                field_path = ".".join(parts[1:])
                source = self._prior_outputs.get(source_step, {})
                # Traverse field path
                for part in field_path.split("."):
                    source = source.get(part, []) if isinstance(source, dict) else []
                if isinstance(source, list):
                    self._walker.set_loop_items(source)

        if step.mode == "reason":
            loop_var = None
            if self._walker.is_in_loop() and self._walker.loop_variable_name():
                loop_var = (self._walker.loop_variable_name(), self._walker.current_loop_item())
            return self._assembler.assemble_reason(
                step=step,
                persona=self._persona,
                prior_outputs=self._prior_outputs,
                workflow_inputs=self._inputs,
                schema_cache=self._schema_cache,
                loop_var=loop_var,
                decision_store=self._decision_store,
            )

        elif step.mode == "execute" and step.execute_type == "script":
            return ExecuteScriptRequest(
                step_id=step.id,
                script=step.script or "",
                inputs=self._inputs,
                fallback_on_failure=getattr(step, 'fallback_on_failure', None),
            )

        elif step.mode == "execute" and step.execute_type == "delegate":
            # Build delegate inputs from dynamic context
            delegate_inputs = {}
            for ref in step.context_dynamic:
                if isinstance(ref, dict):
                    step_id = ref["from"]
                    if step_id in self._prior_outputs:
                        if "select" in ref:
                            sel = ref["select"]
                            output = self._prior_outputs[step_id]
                            if isinstance(sel, list):
                                for field in sel:
                                    delegate_inputs[field] = output[field] if isinstance(output, dict) else output
                            else:
                                delegate_inputs[sel] = output[sel] if isinstance(output, dict) else output
                        else:
                            delegate_inputs[step_id] = self._prior_outputs[step_id]

            return DelegateRequest(
                step_id=step.id,
                capability=step.delegate or "",
                inputs={**delegate_inputs, **{"workflow_inputs": self._inputs}},
                requires_approval=step.requires_approval,
            )

        return None

    def record_result(self, step_id: str, outcome: StepOutcome) -> None:
        """Record a step's outcome and advance the walker."""
        duration_ms = int((time.monotonic() - self._step_start_time) * 1000)
        step = self._walker.current()

        if isinstance(outcome, StepSuccess):
            # Run structural validation if defined
            if step.validation and isinstance(outcome.output, dict):
                errors = validate_structural(outcome.output, step.validation)
                if errors:
                    self._trace.record_step(
                        step_id=step_id, mode=step.mode, status="failed",
                        duration_ms=duration_ms, output=outcome.output,
                        error="; ".join(errors),
                    )
                    self._status = "paused"
                    return

            self._prior_outputs[step_id] = outcome.output

            # Populate schema cache if step declares cache_as
            if step.cache_as and isinstance(outcome.output, dict):
                self._schema_cache[step.cache_as] = outcome.output

            self._trace.record_step(
                step_id=step_id, mode=step.mode, status="completed",
                duration_ms=duration_ms, output=outcome.output,
            )

            # Auto-populate ToolRegistry from any execute/script step
            # that reports capability fields
            if step.mode == "execute" and step.execute_type == "script" and isinstance(outcome.output, dict):
                capability_keys = {"dbt_available", "dbt_mcp_available", "github_available", "fallback_mode"}
                if capability_keys & outcome.output.keys():
                    self._registry.update_capabilities(outcome.output)

            self._walker.advance(step_output={"output": outcome.output})

            # Check if that was the last step
            if self._walker.is_complete():
                self._status = "completed"
                self._trace.record_end("completed")
                if self._decision_store:
                    self._persist_decisions()

        elif isinstance(outcome, StepFailure):
            self._trace.record_step(
                step_id=step_id, mode=step.mode, status="failed",
                duration_ms=duration_ms, output=None, error=outcome.error,
            )
            self._status = "failed" if not outcome.retryable else "paused"

        elif isinstance(outcome, StepSkipped):
            self._trace.record_step(
                step_id=step_id, mode=step.mode, status="skipped",
                duration_ms=duration_ms, output=None,
            )
            self._walker.advance()

    def _persist_decisions(self) -> None:
        """Extract decision facts from the last step output and persist."""
        if not self._decision_store:
            return

        # Look for decision_facts in the last completed step output
        last_output = None
        for step in reversed(self._workflow.steps):
            if step.id in self._prior_outputs:
                last_output = self._prior_outputs[step.id]
                break

        if not isinstance(last_output, dict):
            return

        # If last output has explicit decision fields, persist them
        entity = last_output.get("entity") or self._inputs.get("table_name", "")
        facts = last_output.get("decision_facts", last_output.get("facts", {}))
        confidence = last_output.get("confidence", "medium")

        if entity:
            self._decision_store.write(
                run_id=self._run_id,
                workflow_id=self._workflow.id,
                entity=str(entity),
                facts=facts if isinstance(facts, dict) else {"result": facts},
                confidence=str(confidence),
                valid_until=last_output.get("valid_until"),
            )

    def get_trace(self) -> dict:
        """Get the consolidated execution trace."""
        return self._trace.consolidate()
