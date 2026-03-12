"""Context assembly snapshots — frozen record of what was resolved per step."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContextSnapshot:
    """Frozen snapshot of resolved context for a step.

    This is the keystone contract between DCAG and any consumer.
    Consumed by observability events, validated by conformance tests,
    informed by the tool registry.
    """
    step_id: str
    persona: str                    # resolved persona ID
    knowledge: tuple[str, ...]      # resolved knowledge file IDs
    tools: tuple[str, ...]          # resolved available tools (after registry filtering)
    prior_outputs: tuple[str, ...]  # step IDs that contributed dynamic context
    instruction: str                # assembled instruction text (first 200 chars)
    estimated_tokens: int           # total context size
    workflow_inputs: dict[str, str] | None = None   # Shift suggestion #1
    fallback_mode: str | None = None                # Shift suggestion #1
