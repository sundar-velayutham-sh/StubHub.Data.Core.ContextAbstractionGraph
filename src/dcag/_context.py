"""Context assembly — builds context bundles for workflow steps."""
from __future__ import annotations

import logging
from typing import Any

from dcag._decisions import DecisionStore
from dcag._loaders import KnowledgeLoader, PersonaLoader
from dcag._registry import ToolRegistry
from dcag._tokens import estimate_tokens
from dcag.types import (
    Budget,
    ContextBundle,
    PersonaBundle,
    ReasonRequest,
    StepDef,
)

logger = logging.getLogger(__name__)


class ContextAssembler:
    """Assembles context for each step: static + dynamic + persona merge."""

    def __init__(self, persona_loader: PersonaLoader, knowledge_loader: KnowledgeLoader, registry: ToolRegistry | None = None):
        self._personas = persona_loader
        self._knowledge = knowledge_loader
        self._registry = registry

    def build_static(self, static_refs: list[str]) -> dict[str, Any]:
        """Load static knowledge files."""
        return self._knowledge.load_multiple(static_refs) if static_refs else {}

    def build_dynamic(self, dynamic_refs: list[dict | str], prior_outputs: dict[str, Any]) -> dict[str, Any]:
        """Resolve dynamic context from prior step outputs."""
        result: dict[str, Any] = {}
        for ref in dynamic_refs:
            if isinstance(ref, dict):
                step_id = ref["from"]
                if step_id not in prior_outputs:
                    # Skip refs from steps that didn't execute (e.g. conditional branches)
                    logger.debug(
                        f"Dynamic ref '{step_id}' not in prior outputs (branch not taken), skipping. "
                        f"Available: {list(prior_outputs.keys())}"
                    )
                    continue
                output = prior_outputs[step_id]
                if "select" in ref:
                    sel = ref["select"]
                    if isinstance(sel, list):
                        for field in sel:
                            result[field] = output[field] if isinstance(output, dict) else output
                    else:
                        # Support dot notation: "column_info.sf_type"
                        parts = sel.split(".")
                        val = output
                        for part in parts:
                            val = val[part] if isinstance(val, dict) else val
                        result[parts[0] if len(parts) == 1 else sel] = val
                else:
                    result[step_id] = output
            elif isinstance(ref, str):
                if ref not in prior_outputs:
                    logger.debug(f"Dynamic ref '{ref}' not in prior outputs (branch not taken), skipping")
                    continue
                result[ref] = prior_outputs[ref]
        return result

    def build_decisions(
        self,
        decision_refs: list[dict],
        decision_store: DecisionStore | None,
        workflow_inputs: dict[str, Any],
    ) -> dict[str, Any]:
        """Load decision traces matching entity references."""
        if not decision_refs or not decision_store:
            return {}

        result: dict[str, Any] = {}
        for ref in decision_refs:
            entity = ref.get("entity", "")
            # Resolve template variables: {{inputs.table_name}} -> actual value
            if "{{" in entity:
                for key, val in workflow_inputs.items():
                    entity = entity.replace(f"{{{{inputs.{key}}}}}", str(val))
            decisions = decision_store.search_by_entity(entity)
            if decisions:
                result[f"decisions:{entity}"] = decisions
        return result

    def build_cache(self, cache_refs: list[str], schema_cache: dict[str, Any]) -> dict[str, Any]:
        """Load cached metadata entries by key."""
        result: dict[str, Any] = {}
        for ref in cache_refs:
            if ref in schema_cache:
                result[ref] = schema_cache[ref]
        return result

    def assemble_reason(
        self,
        step: StepDef,
        persona: PersonaBundle,
        prior_outputs: dict[str, Any],
        workflow_inputs: dict[str, Any],
        schema_cache: dict[str, Any] | None = None,
        loop_var: tuple[str, Any] | None = None,
        decision_store: DecisionStore | None = None,
    ) -> ReasonRequest:
        """Assemble a full ReasonRequest for a reason step."""
        # Merge knowledge refs into domain knowledge
        knowledge_items: list[str] = []
        for kid in step.context_knowledge:
            try:
                k = self._knowledge.load(kid)
                guidance = k.get("guidance", [])
                if isinstance(guidance, list):
                    knowledge_items.extend(guidance)
            except FileNotFoundError:
                logger.warning(f"Knowledge '{kid}' not found, skipping")

        # Merge persona with step overrides
        merged = self._personas.merge(
            persona,
            step_heuristics=step.heuristics,
            step_anti_patterns=step.anti_patterns,
            step_knowledge=knowledge_items,
        )

        static = self.build_static(step.context_static)
        dynamic = self.build_dynamic(step.context_dynamic, prior_outputs)

        cached = self.build_cache(step.context_cache, schema_cache or {})
        if cached:
            dynamic.update(cached)

        # Inject loop variable if walker provides one
        if loop_var is not None:
            var_name, var_value = loop_var
            dynamic[var_name] = var_value

        # Load decision traces
        decisions = self.build_decisions(step.context_decisions, decision_store, workflow_inputs)
        if decisions:
            dynamic.update(decisions)

        # Filter tools through registry if available
        available_tools = self._registry.resolve_available(step.tools) if self._registry else step.tools

        # Estimate tokens
        total_tokens = (
            estimate_tokens(static)
            + estimate_tokens(dynamic)
            + estimate_tokens(merged.domain_knowledge)
            + estimate_tokens(merged.heuristics)
            + estimate_tokens(merged.anti_patterns)
            + estimate_tokens([t.instruction + (t.usage_pattern or "") for t in available_tools])
        )

        budget = step.budget or Budget()
        if total_tokens > budget.max_tokens * 0.5:
            logger.warning(
                f"Step '{step.id}' context is ~{total_tokens} tokens "
                f"({total_tokens * 100 // budget.max_tokens}% of {budget.max_tokens} budget)"
            )

        return ReasonRequest(
            step_id=step.id,
            persona=merged,
            instruction=step.instruction or "",
            context=ContextBundle(
                static=static,
                dynamic=dynamic,
                domain_knowledge=merged.domain_knowledge,
                estimated_tokens=total_tokens,
            ),
            tools=available_tools,
            output_schema=step.output_schema,
            quality_criteria=step.quality_criteria,
            budget=budget,
        )
