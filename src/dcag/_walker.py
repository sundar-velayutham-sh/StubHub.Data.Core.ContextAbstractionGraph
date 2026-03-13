"""DAG walker — traverses workflow steps linearly, conditionally, or in loops."""
from __future__ import annotations

from typing import Any

from dcag._evaluator import evaluate
from dcag.types import StepDef


class Walker:
    """Walks through workflow steps.

    Supports:
    - Linear traversal (default)
    - Conditional transitions via step_output evaluation
    - Loop iteration via set_loop_items()
    """

    def __init__(self, steps: list[StepDef]):
        self._steps = steps
        self._index = 0
        self._step_index: dict[str, int] = {s.id: i for i, s in enumerate(steps)}
        # Loop state
        self._loop_items: list[Any] = []
        self._loop_index: int = -1

    def current(self) -> StepDef:
        """Get current step."""
        return self._steps[self._index]

    def advance(self, step_output: dict[str, Any] | None = None) -> None:
        """Move to next step, respecting loops and transitions.

        Args:
            step_output: Output from current step. Used to evaluate transitions.
        """
        # If in a loop, try to advance within it
        if self._loop_index >= 0:
            self._loop_index += 1
            if self._loop_index < len(self._loop_items):
                return  # stay on same step, next item
            # Loop exhausted -- clear and fall through to advance step
            self._loop_items = []
            self._loop_index = -1

        # Check conditional transitions
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

    def set_loop_items(self, items: list[Any]) -> None:
        """Set the collection for the current loop step.

        Called by the engine before the first iteration of a loop step.
        If items is empty, the loop is effectively skipped on next advance().
        """
        if items:
            self._loop_items = items
            self._loop_index = 0
        else:
            self._loop_items = []
            self._loop_index = -1

    def current_loop_item(self) -> Any | None:
        """Get the current loop item, or None if not in a loop."""
        if self._loop_index >= 0 and self._loop_index < len(self._loop_items):
            return self._loop_items[self._loop_index]
        return None

    def loop_index(self) -> int:
        """Get current loop index. Returns -1 if not in a loop."""
        return self._loop_index

    def is_in_loop(self) -> bool:
        """True if currently iterating through a loop."""
        return self._loop_index >= 0

    def loop_variable_name(self) -> str | None:
        """Get the 'as' variable name for the current loop step."""
        step = self._steps[self._index]
        if step.loop and isinstance(step.loop, dict):
            return step.loop.get("as")
        return None

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
