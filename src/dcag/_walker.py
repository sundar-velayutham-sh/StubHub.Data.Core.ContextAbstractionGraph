"""DAG walker — traverses workflow steps linearly or via conditional transitions."""
from __future__ import annotations

from typing import Any

from dcag._evaluator import evaluate
from dcag.types import StepDef


class Walker:
    """Walks through workflow steps. Supports conditional transitions (v2).

    If a step has `transitions`, advance() evaluates them against step_output.
    If no transitions or no match, falls back to linear (next index).
    """

    def __init__(self, steps: list[StepDef]):
        self._steps = steps
        self._index = 0
        self._step_index: dict[str, int] = {s.id: i for i, s in enumerate(steps)}

    def current(self) -> StepDef:
        """Get current step."""
        return self._steps[self._index]

    def advance(self, step_output: dict[str, Any] | None = None) -> None:
        """Move to next step.

        Args:
            step_output: Output from current step. Used to evaluate transitions.
                         If None or no transitions match, advances linearly.
        """
        step = self._steps[self._index]
        target = self._resolve_next(step, step_output)

        if target is not None:
            if target not in self._step_index:
                raise ValueError(
                    f"Transition goto '{target}' not found in workflow steps. "
                    f"Available: {list(self._step_index.keys())}"
                )
            self._index = self._step_index[target]
        else:
            self._index += 1

    def is_complete(self) -> bool:
        """True when all steps have been processed."""
        return self._index >= len(self._steps)

    def _resolve_next(self, step: StepDef, step_output: dict[str, Any] | None) -> str | None:
        """Evaluate transitions and return target step id, or None for linear."""
        if not step.transitions or step_output is None:
            return None

        for transition in step.transitions:
            if "when" in transition:
                if evaluate(transition["when"], step_output):
                    return transition["goto"]
            elif "default" in transition:
                return transition["default"]

        return None  # no match -> linear fallback
