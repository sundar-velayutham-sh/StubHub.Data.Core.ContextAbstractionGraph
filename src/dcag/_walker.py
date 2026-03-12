"""DAG walker — traverses workflow steps in sequence."""
from __future__ import annotations

from dcag.types import StepDef


class Walker:
    """Walks through workflow steps linearly. Conditional transitions in v2."""

    def __init__(self, steps: list[StepDef]):
        self._steps = steps
        self._index = 0

    def current(self) -> StepDef:
        """Get current step."""
        return self._steps[self._index]

    def advance(self) -> None:
        """Move to next step."""
        self._index += 1

    def is_complete(self) -> bool:
        """True when all steps have been processed."""
        return self._index >= len(self._steps)
