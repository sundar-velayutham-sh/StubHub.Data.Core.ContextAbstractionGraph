"""Structural validators for step outputs."""
from __future__ import annotations

from typing import Any


def validate_structural(output: Any, rules: list[dict]) -> list[str]:
    """Run structural validation rules. Returns list of error messages (empty = passed)."""
    errors: list[str] = []
    for rule in rules:
        if "output_has" in rule:
            key = rule["output_has"]
            if not isinstance(output, dict) or key not in output or output[key] is None:
                errors.append(f"Output missing required key '{key}'")
    return errors
