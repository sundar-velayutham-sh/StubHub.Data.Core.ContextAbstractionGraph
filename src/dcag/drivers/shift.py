"""Shift integration driver — translates DCAG requests into Shift's tool calls.

This is the bridge between DCAG's abstract workflow engine and Shift's
concrete LLM + MCP capabilities. It handles:
  - Prompt assembly (tool-gate-first format)
  - Delegate routing (show_plan, create_pr)
  - Capability parsing from step 0 output
  - Observability event emission
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from dcag._snapshot import ContextSnapshot
from dcag.types import DelegateRequest, ReasonRequest


# Supported delegate capabilities
_DELEGATE_CAPABILITIES = {"shift.show_plan", "shift.create_pr"}


class ShiftDriver:
    """Reference driver for Shift (StubHub's Slack AI assistant).

    Translates DCAG's typed requests into prompts and delegate actions.
    Does NOT make LLM calls — Shift does that.
    """

    def assemble_prompt(self, request: ReasonRequest) -> str:
        """Build the LLM prompt from a ReasonRequest.

        Format: TOOLS first, then PERSONA, TASK, CONTEXT, OUTPUT, BUDGET.
        """
        sections: list[str] = []

        # 1. Tool gate — ALWAYS first
        sections.append(self._build_tool_section(request))

        # 2. Persona
        sections.append(self._build_persona_section(request))

        # 3. Task
        sections.append(self._build_task_section(request))

        # 4. Context
        sections.append(self._build_context_section(request))

        # 5. Output schema + quality criteria
        sections.append(self._build_output_section(request))

        # 6. Budget
        sections.append(self._build_budget_section(request))

        return "\n\n".join(sections)

    def route_delegate(self, request: DelegateRequest) -> dict[str, Any]:
        """Route a DelegateRequest to the appropriate Shift capability."""
        if request.capability not in _DELEGATE_CAPABILITIES:
            raise ValueError(
                f"Unknown delegate capability: '{request.capability}'. "
                f"Supported: {_DELEGATE_CAPABILITIES}"
            )

        capability = request.capability.removeprefix("shift.")
        return {
            "capability": capability,
            "requires_approval": request.requires_approval,
            "inputs": request.inputs,
            "step_id": request.step_id,
        }

    def parse_capabilities(self, step0_output: dict[str, Any]) -> dict[str, Any]:
        """Parse step 0 output into capability flags."""
        return {
            "dbt_available": bool(step0_output.get("dbt_available", False)),
            "dbt_mcp_available": bool(step0_output.get("dbt_mcp_available", False)),
            "fallback_mode": step0_output.get("fallback_mode", "snowflake_only"),
        }

    def estimate_prompt_tokens(self, prompt: str) -> int:
        """Estimate token count for assembled prompt (~4 chars/token)."""
        return len(prompt) // 4

    # ── Observability event emitters ──────────────────────────

    def emit_step_started(self, step_id: str, mode: str) -> dict:
        return {"type": "step_started", "step_id": step_id, "mode": mode, "timestamp": _now()}

    def emit_context_assembled(self, request: ReasonRequest) -> dict:
        snapshot = ContextSnapshot(
            step_id=request.step_id,
            persona=request.persona.id,
            knowledge=tuple(request.context.static.keys()),
            tools=tuple(t.name for t in request.tools),
            prior_outputs=tuple(request.context.dynamic.keys()),
            instruction=request.instruction[:200],
            estimated_tokens=request.context.estimated_tokens,
        )
        return {
            "type": "context_assembled",
            "step_id": request.step_id,
            "snapshot": {
                "step_id": snapshot.step_id,
                "persona": snapshot.persona,
                "knowledge": list(snapshot.knowledge),
                "tools": list(snapshot.tools),
                "prior_outputs": list(snapshot.prior_outputs),
                "instruction": snapshot.instruction,
                "estimated_tokens": snapshot.estimated_tokens,
            },
            "timestamp": _now(),
        }

    def emit_tool_resolved(self, step_id: str, requested: list[str], available: list[str]) -> dict:
        return {
            "type": "tool_resolved",
            "step_id": step_id,
            "requested": requested,
            "available": available,
            "timestamp": _now(),
        }

    def emit_result_recorded(self, step_id: str, status: str, duration_ms: int) -> dict:
        return {
            "type": "result_recorded",
            "step_id": step_id,
            "status": status,
            "duration_ms": duration_ms,
            "timestamp": _now(),
        }

    # ── Private prompt builders ───────────────────────────────

    def _build_tool_section(self, request: ReasonRequest) -> str:
        lines = ["[TOOLS — ONLY USE THESE]"]
        if not request.tools:
            lines.append("No tools available for this step. Reason using context only.")
        else:
            lines.append("You have access to these tools and ONLY these tools:")
            for tool in request.tools:
                lines.append(f"- {tool.name}: {tool.instruction}")
                if tool.usage_pattern:
                    for pattern_line in tool.usage_pattern.strip().splitlines():
                        lines.append(f"    {pattern_line}")
            lines.append("Do NOT use any other tools.")
        return "\n".join(lines)

    def _build_persona_section(self, request: ReasonRequest) -> str:
        p = request.persona
        lines = [
            "[PERSONA]",
            f"You are {p.name}. {p.description}",
        ]
        if p.domain_knowledge:
            lines.append("")
            lines.append("Domain knowledge:")
            for item in p.domain_knowledge:
                lines.append(f"- {item}")
        if p.heuristics:
            lines.append("")
            lines.append("Heuristics (follow these):")
            for item in p.heuristics:
                lines.append(f"- {item}")
        if p.anti_patterns:
            lines.append("")
            lines.append("Anti-patterns (avoid these):")
            for item in p.anti_patterns:
                lines.append(f"- {item}")
        return "\n".join(lines)

    def _build_task_section(self, request: ReasonRequest) -> str:
        return f"[TASK]\n{request.instruction}"

    def _build_context_section(self, request: ReasonRequest) -> str:
        lines = ["[CONTEXT]"]
        if request.context.static:
            lines.append("Static knowledge:")
            lines.append(json.dumps(request.context.static, indent=2, default=str))
        if request.context.dynamic:
            lines.append("")
            lines.append("Prior step outputs:")
            lines.append(json.dumps(request.context.dynamic, indent=2, default=str))
        if not request.context.static and not request.context.dynamic:
            lines.append("No additional context.")
        return "\n".join(lines)

    def _build_output_section(self, request: ReasonRequest) -> str:
        lines = ["[OUTPUT]"]
        if request.output_schema:
            lines.append("Return your response as JSON matching this schema:")
            lines.append(json.dumps(request.output_schema, indent=2))
        else:
            lines.append("Return your response as structured JSON.")
        if request.quality_criteria:
            lines.append("")
            lines.append("Quality criteria (self-check before responding):")
            for criterion in request.quality_criteria:
                lines.append(f"- {criterion}")
        return "\n".join(lines)

    def _build_budget_section(self, request: ReasonRequest) -> str:
        return f"[BUDGET]\nMax tool calls: {request.budget.max_llm_turns}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
