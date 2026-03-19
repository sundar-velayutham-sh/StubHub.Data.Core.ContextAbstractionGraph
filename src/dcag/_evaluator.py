"""Expression evaluator for conditional transitions.

Supports: ==, !=, >, <, in operators on dict values.
Dot notation traverses nested dicts.
"""
from __future__ import annotations

import ast
import re
from typing import Any

# Pattern: "path.to.field <op> <value>"
_EXPR_RE = re.compile(
    r"^([\w.]+)\s+(==|!=|>|<|in)\s+(.+)$"
)


def _resolve_path(data: dict, path: str) -> Any:
    """Traverse nested dict via dot notation. Returns _MISSING on failure."""
    parts = path.split(".")
    current: Any = data
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return _MISSING
    return current


class _MissingSentinel:
    """Sentinel for missing values."""
    pass


_MISSING = _MissingSentinel()


def _parse_value(raw: str) -> Any:
    """Parse a literal value from expression RHS."""
    raw = raw.strip()
    # Try Python literal (handles strings, ints, floats, bools, lists)
    try:
        return ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return raw


def evaluate(expression: str, context: dict) -> bool:
    """Evaluate a simple expression against a context dict.

    Args:
        expression: e.g. "output.bug_type == 'cast_error'"
        context: dict to evaluate against

    Returns:
        True if expression matches, False otherwise (including missing paths).
    """
    match = _EXPR_RE.match(expression.strip())
    if not match:
        return False

    path, operator, raw_value = match.groups()
    actual = _resolve_path(context, path)

    if isinstance(actual, _MissingSentinel):
        return False

    expected = _parse_value(raw_value)

    if operator == "==":
        return actual == expected
    elif operator == "!=":
        return actual != expected
    elif operator == ">":
        return actual > expected
    elif operator == "<":
        return actual < expected
    elif operator == "in":
        return actual in expected if isinstance(expected, (list, tuple, set)) else False
    return False
