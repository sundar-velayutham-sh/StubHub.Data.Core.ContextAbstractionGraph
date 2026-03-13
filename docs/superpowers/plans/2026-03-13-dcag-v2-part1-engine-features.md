# DCAG v2 Implementation Plan — Part 1: Engine Features

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox syntax for tracking.

**Goal:** Build 4 engine features: conditional walker, schema cache, step loops, decision traces. Plus 8 knowledge YAML files.

**Architecture:** Each feature is additive to the existing engine. Existing 125 tests must keep passing. Features build on each other: conditional walker → loops depend on it. Schema cache and decision traces are independent.

**Tech Stack:** Python 3.14, dataclasses, PyYAML, pytest

**Spec:** `docs/superpowers/specs/2026-03-13-dcag-v2-engine-and-workflows-design.md`

**Current state:** 125 tests passing, 2 skipped.

---

## Task 1: Expression Evaluator

**Goal:** Create `src/dcag/_evaluator.py` — a safe expression evaluator for conditional transitions.
**Pattern:** `"output.field == 'value'"` parsed into `(path, operator, value)`, evaluated against a dict.

### Step 1.1 — Write tests first

- [ ] Create `tests/test_evaluator.py`

```python
"""Tests for the expression evaluator."""
import pytest
from dcag._evaluator import evaluate


class TestEvaluate:
    """Expression evaluator unit tests."""

    def test_equality_string(self):
        ctx = {"output": {"bug_type": "cast_error"}}
        assert evaluate("output.bug_type == 'cast_error'", ctx) is True

    def test_equality_string_false(self):
        ctx = {"output": {"bug_type": "join_error"}}
        assert evaluate("output.bug_type == 'cast_error'", ctx) is False

    def test_inequality(self):
        ctx = {"output": {"bug_type": "join_error"}}
        assert evaluate("output.bug_type != 'cast_error'", ctx) is True

    def test_greater_than(self):
        ctx = {"output": {"row_count": 1000}}
        assert evaluate("output.row_count > 500", ctx) is True

    def test_greater_than_false(self):
        ctx = {"output": {"row_count": 100}}
        assert evaluate("output.row_count > 500", ctx) is False

    def test_less_than(self):
        ctx = {"output": {"row_count": 100}}
        assert evaluate("output.row_count < 500", ctx) is True

    def test_in_operator_list(self):
        ctx = {"output": {"strategy": "CLUSTER_BY"}}
        assert evaluate("output.strategy in ['CLUSTER_BY', 'SOS']", ctx) is True

    def test_in_operator_not_found(self):
        ctx = {"output": {"strategy": "SKIP"}}
        assert evaluate("output.strategy in ['CLUSTER_BY', 'SOS']", ctx) is False

    def test_nested_dot_path(self):
        ctx = {"output": {"column_info": {"sf_type": "VARCHAR"}}}
        assert evaluate("output.column_info.sf_type == 'VARCHAR'", ctx) is True

    def test_top_level_key(self):
        ctx = {"status": "ready"}
        assert evaluate("status == 'ready'", ctx) is True

    def test_integer_comparison(self):
        ctx = {"output": {"size_gb": 15}}
        assert evaluate("output.size_gb > 10", ctx) is True

    def test_equality_integer(self):
        ctx = {"output": {"count": 0}}
        assert evaluate("output.count == 0", ctx) is True

    def test_missing_path_returns_false(self):
        ctx = {"output": {}}
        assert evaluate("output.nonexistent == 'value'", ctx) is False

    def test_empty_context_returns_false(self):
        assert evaluate("output.field == 'value'", {}) is False

    def test_bool_value(self):
        ctx = {"output": {"is_valid": True}}
        assert evaluate("output.is_valid == True", ctx) is True
```

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_evaluator.py -v
# Expected: ALL FAIL (module not created yet)
```

### Step 1.2 — Implement evaluator

- [ ] Create `src/dcag/_evaluator.py`

```python
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
```

### Step 1.3 — Verify

- [ ] Run evaluator tests

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_evaluator.py -v
# Expected: 15 passed
```

- [ ] Run full suite to confirm no regressions

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
# Expected: 140 passed, 2 skipped (125 existing + 15 new)
```

### Step 1.4 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add src/dcag/_evaluator.py tests/test_evaluator.py
git commit -m "$(cat <<'EOF'
feat(engine): add expression evaluator for conditional transitions

Supports ==, !=, >, <, in operators on nested dict values via dot
notation. Foundation for conditional walker (Feature 1).

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Conditional Walker

**Goal:** Steps can declare `transitions` that route to different next steps based on output values. Without transitions, Walker stays linear (backward-compatible).

**Key files:**
- `src/dcag/types.py` — `StepDef` already has `transitions: list[dict] | None` at line 207
- `src/dcag/_walker.py` — `Walker` class (lines 7-24), needs `resolve_next()` and updated `advance()`
- `src/dcag/_loaders.py` — `_parse_step()` already parses `transitions` at line 171
- `tests/test_walker.py` — `make_step()` helper (lines 6-15), `TestWalker` class (lines 18-36)

### Step 2.1 — Write tests first

- [ ] Add `TestConditionalWalker` to `tests/test_walker.py`

Append to `tests/test_walker.py` (after line 36):

```python
def make_step_with_transitions(id: str, transitions: list[dict] | None = None) -> StepDef:
    """Helper to create a StepDef with transitions."""
    return StepDef(
        id=id, mode="reason", execute_type=None,
        template=None, script=None, delegate=None,
        tools=[ToolDirective("t", "i")],
        instruction=None, context_static=[], context_dynamic=[], context_knowledge=[],
        heuristics=[], anti_patterns=[], quality_criteria=[], output_schema=None,
        validation=[], requires_approval=False, budget=None,
        transitions=transitions,
    )


class TestConditionalWalker:
    """Tests for conditional transitions in Walker."""

    def test_transition_routes_to_matching_step(self):
        """When output matches a transition, Walker jumps to the goto step."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
                {"when": "output.bug_type == 'join_error'", "goto": "fix_join"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("fix_join"),
        ]
        walker = Walker(steps)
        assert walker.current().id == "classify"
        walker.advance(step_output={"output": {"bug_type": "cast_error"}})
        assert walker.current().id == "fix_cast"

    def test_transition_second_match(self):
        """Second transition matches when first doesn't."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
                {"when": "output.bug_type == 'join_error'", "goto": "fix_join"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("fix_join"),
        ]
        walker = Walker(steps)
        walker.advance(step_output={"output": {"bug_type": "join_error"}})
        assert walker.current().id == "fix_join"

    def test_transition_default_fallback(self):
        """When no 'when' matches, 'default' transition is used."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
                {"default": "fix_generic"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("fix_generic"),
        ]
        walker = Walker(steps)
        walker.advance(step_output={"output": {"bug_type": "unknown_error"}})
        assert walker.current().id == "fix_generic"

    def test_no_transitions_linear_advance(self):
        """Without transitions, advance() still works linearly (backward compat)."""
        steps = [make_step("a"), make_step("b"), make_step("c")]
        walker = Walker(steps)
        walker.advance()
        assert walker.current().id == "b"

    def test_advance_no_output_linear(self):
        """advance() without step_output falls through to linear even with transitions."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("next_step"),
        ]
        walker = Walker(steps)
        walker.advance()  # no output → linear
        assert walker.current().id == "fix_cast"

    def test_transition_goto_unknown_step_raises(self):
        """If goto references a step that doesn't exist, raise ValueError."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "nonexistent"},
            ]),
        ]
        walker = Walker(steps)
        with pytest.raises(ValueError, match="nonexistent"):
            walker.advance(step_output={"output": {"bug_type": "cast_error"}})

    def test_transition_no_match_no_default_linear(self):
        """If transitions exist but none match and no default, fall through to linear."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("linear_next"),
        ]
        walker = Walker(steps)
        walker.advance(step_output={"output": {"bug_type": "unknown"}})
        assert walker.current().id == "fix_cast"  # linear: next in list
```

Add `import pytest` to the top of `tests/test_walker.py` (currently missing).

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_walker.py -v
# Expected: TestWalker passes (3), TestConditionalWalker FAILS (Walker.advance doesn't accept step_output yet)
```

### Step 2.2 — Implement conditional walker

- [ ] Modify `src/dcag/_walker.py`

Replace the entire file:

```python
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

        return None  # no match → linear fallback
```

### Step 2.3 — Update engine.py to pass output to advance()

- [ ] Modify `src/dcag/engine.py` line 192 — change `self._walker.advance()` to pass output

In `record_result()`, at line 192, the call `self._walker.advance()` must become:

```python
            self._walker.advance(step_output={"output": outcome.output})
```

Also at line 211, the `self._walker.advance()` in the `StepSkipped` branch stays linear (no output):

```python
            self._walker.advance()  # skipped steps → linear
```

This is already correct — no change needed for line 211.

### Step 2.4 — Verify

- [ ] Run walker tests

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_walker.py -v
# Expected: 10 passed (3 original + 7 new)
```

- [ ] Run full suite

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
# Expected: 147 passed, 2 skipped (125 existing + 15 evaluator + 7 walker)
```

### Step 2.5 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add src/dcag/_walker.py src/dcag/engine.py tests/test_walker.py
git commit -m "$(cat <<'EOF'
feat(engine): conditional walker with transition routing

Walker.advance() now accepts step_output and evaluates transitions
before falling back to linear. Backward-compatible: no transitions
= same linear behavior. Engine passes output to advance() on
StepSuccess.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Schema Cache

**Goal:** Steps can declare `cache_as: <key>` to cache their output. Later steps declare `context.cache: [key1, key2]` to pull cached values into their context without re-executing MCP calls.

**Key files:**
- `src/dcag/types.py` — `StepDef` at lines 186-208, needs `cache_as: str | None = None`
- `src/dcag/engine.py` — `WorkflowRun.__init__()` at lines 77-98, `record_result()` at lines 161-211
- `src/dcag/_context.py` — `ContextAssembler` at lines 21-129, needs `build_cache()` and integration into `assemble_reason()`
- `src/dcag/_loaders.py` — `_parse_step()` at lines 121-173, needs to parse `cache_as` and `context.cache`
- `tests/test_context.py` — add `TestSchemaCache` class

### Step 3.1 — Write tests first

- [ ] Add `TestSchemaCache` to `tests/test_context.py`

Append to `tests/test_context.py` (after line 133):

```python
class TestSchemaCache:
    """Tests for schema cache assembly."""

    def test_build_cache_returns_matching_entries(self, assembler):
        """build_cache returns only the requested cache keys."""
        cache = {"table_columns": {"col1": "VARCHAR"}, "storage_metrics": {"bytes": 1024}}
        result = assembler.build_cache(["table_columns"], cache)
        assert "table_columns" in result
        assert "storage_metrics" not in result

    def test_build_cache_empty_refs(self, assembler):
        """No cache refs returns empty dict."""
        cache = {"table_columns": {"col1": "VARCHAR"}}
        result = assembler.build_cache([], cache)
        assert result == {}

    def test_build_cache_missing_key_skipped(self, assembler):
        """Missing cache key is silently skipped (not an error)."""
        cache = {"table_columns": {"col1": "VARCHAR"}}
        result = assembler.build_cache(["table_columns", "nonexistent"], cache)
        assert "table_columns" in result
        assert "nonexistent" not in result

    def test_build_cache_empty_cache(self, assembler):
        """Empty cache with refs returns empty dict."""
        result = assembler.build_cache(["table_columns"], {})
        assert result == {}

    def test_build_cache_all_keys(self, assembler):
        """Multiple keys all found."""
        cache = {
            "table_columns": {"col1": "VARCHAR"},
            "storage_metrics": {"bytes": 1024},
            "row_count": 50000,
        }
        result = assembler.build_cache(["table_columns", "storage_metrics", "row_count"], cache)
        assert len(result) == 3
```

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_context.py::TestSchemaCache -v
# Expected: FAIL (build_cache doesn't exist yet)
```

### Step 3.2 — Add `cache_as` to StepDef

- [ ] Modify `src/dcag/types.py` — add `cache_as` field to `StepDef`

Insert after line 208 (`fallback_on_failure: str | None = None`):

```python
    cache_as: str | None = None
```

The `StepDef` fields at lines 188-209 become (lines 188-210):

```
    id: str
    mode: ...
    ...
    fallback_on_failure: str | None = None
    cache_as: str | None = None
```

### Step 3.3 — Parse `cache_as` and `context.cache` in loader

- [ ] Modify `src/dcag/_loaders.py` — in `_parse_step()` at line 152

Add `cache_as` to the `StepDef` constructor. After the `fallback_on_failure` line (line 172):

```python
            cache_as=raw.get("cache_as"),
```

Also store `context.cache` refs. The `ctx` variable is already parsed at line 143. Add to the returned StepDef — but first we need a new field `context_cache` on StepDef.

**Actually:** Add `context_cache: list[str]` field to StepDef in types.py as well.

- [ ] Modify `src/dcag/types.py` — add after `cache_as`:

```python
    context_cache: list[str] = field(default_factory=list)
```

- [ ] Modify `src/dcag/_loaders.py` — in the StepDef constructor (line 152-173), add:

```python
            context_cache=ctx.get("cache", []),
```

### Step 3.4 — Add `_schema_cache` to WorkflowRun

- [ ] Modify `src/dcag/engine.py`

In `WorkflowRun.__init__()` (after line 93 `self._prior_outputs: dict[str, Any] = {}`), add:

```python
        self._schema_cache: dict[str, Any] = {}
```

In `record_result()`, after line 179 (`self._prior_outputs[step_id] = outcome.output`), add cache population:

```python
            # Populate schema cache if step declares cache_as
            if step.cache_as and isinstance(outcome.output, dict):
                self._schema_cache[step.cache_as] = outcome.output
```

Pass `_schema_cache` to `assemble_reason()` — modify the call at lines 119-124. Add `schema_cache=self._schema_cache`:

```python
        if step.mode == "reason":
            return self._assembler.assemble_reason(
                step=step,
                persona=self._persona,
                prior_outputs=self._prior_outputs,
                workflow_inputs=self._inputs,
                schema_cache=self._schema_cache,
            )
```

### Step 3.5 — Add `build_cache()` to ContextAssembler

- [ ] Modify `src/dcag/_context.py`

Add `build_cache()` method to `ContextAssembler` (after `build_dynamic()`, around line 63):

```python
    def build_cache(self, cache_refs: list[str], schema_cache: dict[str, Any]) -> dict[str, Any]:
        """Load cached metadata entries by key."""
        result: dict[str, Any] = {}
        for ref in cache_refs:
            if ref in schema_cache:
                result[ref] = schema_cache[ref]
        return result
```

Modify `assemble_reason()` signature (line 65) to accept `schema_cache`:

```python
    def assemble_reason(
        self,
        step: StepDef,
        persona: PersonaBundle,
        prior_outputs: dict[str, Any],
        workflow_inputs: dict[str, Any],
        schema_cache: dict[str, Any] | None = None,
    ) -> ReasonRequest:
```

Inside `assemble_reason()`, after `dynamic = self.build_dynamic(...)` (line 93), add:

```python
        cached = self.build_cache(step.context_cache, schema_cache or {})
        if cached:
            dynamic.update(cached)
```

### Step 3.6 — Update make_step helpers in tests

- [ ] Update `tests/test_walker.py` `make_step()` and `make_step_with_transitions()` to include new fields

In `make_step()` (line 8), add `cache_as=None, context_cache=[],` before the closing paren, or add them as keyword args to the StepDef constructor. Since StepDef uses `field(default_factory=list)` for `context_cache` and `None` for `cache_as`, these have defaults and existing calls won't break.

**Wait** — StepDef is `frozen=True` with positional fields. Since `cache_as` and `context_cache` both have defaults, they'll be appended after `fallback_on_failure` which already has a default. Existing code constructing StepDef with keyword arguments will continue to work. The `make_step()` helper in test_walker.py uses keyword arguments, so it's fine.

### Step 3.7 — Verify

- [ ] Run cache tests

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_context.py::TestSchemaCache -v
# Expected: 5 passed
```

- [ ] Run full suite

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
# Expected: 152 passed, 2 skipped (147 prior + 5 cache)
```

### Step 3.8 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add src/dcag/types.py src/dcag/engine.py src/dcag/_context.py src/dcag/_loaders.py tests/test_context.py
git commit -m "$(cat <<'EOF'
feat(engine): schema cache for cross-step metadata reuse

Steps declare cache_as to store output in WorkflowRun._schema_cache.
Later steps declare context.cache to pull cached entries into their
context without redundant MCP calls.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Step Loops

**Goal:** A step declares `loop: {over: <path>, as: <name>}` to iterate over a collection from a prior step output. The Walker executes the step N times, once per item.

**Key files:**
- `src/dcag/types.py` — `StepDef` at lines 186-210, needs `loop: dict | None = None`
- `src/dcag/_walker.py` — `Walker` class, needs loop tracking: `_loop_items`, `_loop_index`
- `src/dcag/_context.py` — needs to inject loop variable into dynamic context
- `src/dcag/_loaders.py` — `_parse_step()` needs to parse `loop` from YAML
- New: `tests/test_loops.py`

### Step 4.1 — Write tests first

- [ ] Create `tests/test_loops.py`

```python
"""Tests for step loop functionality."""
import pytest
from dcag._walker import Walker
from dcag.types import StepDef, ToolDirective


def make_step(id: str, mode: str = "reason", loop: dict | None = None,
              transitions: list[dict] | None = None) -> StepDef:
    """Helper to create a StepDef with optional loop."""
    return StepDef(
        id=id, mode=mode, execute_type="template" if mode == "execute" else None,
        template=None, script=None, delegate=None,
        tools=[ToolDirective("t", "i")] if mode == "reason" else [],
        instruction=None, context_static=[], context_dynamic=[], context_knowledge=[],
        heuristics=[], anti_patterns=[], quality_criteria=[], output_schema=None,
        validation=[], requires_approval=False, budget=None,
        transitions=transitions, loop=loop,
    )


class TestStepLoops:
    """Tests for loop iteration in Walker."""

    def test_loop_iterates_correct_number(self):
        """Loop step executes once per item in the collection."""
        steps = [
            make_step("setup"),
            make_step("modify_each", loop={"over": "setup.models", "as": "current_model"}),
            make_step("validate"),
        ]
        walker = Walker(steps)

        # Advance past setup
        walker.advance(step_output={
            "output": {"models": ["model_a", "model_b", "model_c"]}
        })
        assert walker.current().id == "modify_each"

        # Set loop items (engine does this before first iteration)
        walker.set_loop_items(["model_a", "model_b", "model_c"])

        # First iteration
        assert walker.current().id == "modify_each"
        assert walker.current_loop_item() == "model_a"
        assert walker.loop_index() == 0
        walker.advance()

        # Second iteration — same step
        assert walker.current().id == "modify_each"
        assert walker.current_loop_item() == "model_b"
        assert walker.loop_index() == 1
        walker.advance()

        # Third iteration — same step
        assert walker.current().id == "modify_each"
        assert walker.current_loop_item() == "model_c"
        assert walker.loop_index() == 2
        walker.advance()

        # Loop done — moves to next step
        assert walker.current().id == "validate"

    def test_loop_single_item(self):
        """Loop with one item executes once then advances."""
        steps = [
            make_step("modify_each", loop={"over": "setup.models", "as": "m"}),
            make_step("done"),
        ]
        walker = Walker(steps)
        walker.set_loop_items(["only_one"])

        assert walker.current_loop_item() == "only_one"
        walker.advance()
        assert walker.current().id == "done"

    def test_loop_empty_collection_skips(self):
        """Loop with empty collection advances to next step immediately."""
        steps = [
            make_step("modify_each", loop={"over": "setup.models", "as": "m"}),
            make_step("done"),
        ]
        walker = Walker(steps)
        walker.set_loop_items([])
        walker.advance()
        assert walker.current().id == "done"

    def test_no_loop_normal_advance(self):
        """Steps without loop advance normally."""
        steps = [make_step("a"), make_step("b")]
        walker = Walker(steps)
        assert walker.current_loop_item() is None
        walker.advance()
        assert walker.current().id == "b"

    def test_loop_index_resets_after_loop(self):
        """After loop completes, loop state is cleared."""
        steps = [
            make_step("loop_step", loop={"over": "x.items", "as": "item"}),
            make_step("after"),
        ]
        walker = Walker(steps)
        walker.set_loop_items(["a", "b"])
        walker.advance()  # a → b
        walker.advance()  # b → done → next step
        assert walker.current().id == "after"
        assert walker.current_loop_item() is None
        assert walker.loop_index() == -1

    def test_is_in_loop(self):
        """is_in_loop() returns True during loop iteration."""
        steps = [
            make_step("loop_step", loop={"over": "x.items", "as": "item"}),
            make_step("after"),
        ]
        walker = Walker(steps)
        assert walker.is_in_loop() is False
        walker.set_loop_items(["a"])
        assert walker.is_in_loop() is True
        walker.advance()
        assert walker.is_in_loop() is False

    def test_loop_variable_name(self):
        """loop_variable_name() returns the 'as' field."""
        steps = [
            make_step("loop_step", loop={"over": "x.items", "as": "current_model"}),
        ]
        walker = Walker(steps)
        assert walker.loop_variable_name() == "current_model"

    def test_no_loop_variable_name_returns_none(self):
        """loop_variable_name() returns None for non-loop steps."""
        steps = [make_step("normal")]
        walker = Walker(steps)
        assert walker.loop_variable_name() is None
```

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_loops.py -v
# Expected: FAIL (set_loop_items, current_loop_item etc don't exist yet)
```

### Step 4.2 — Add `loop` field to StepDef

- [ ] Modify `src/dcag/types.py` — add `loop` field to `StepDef`

Insert after `context_cache` (the field added in Task 3):

```python
    loop: dict | None = None
```

### Step 4.3 — Parse `loop` in loader

- [ ] Modify `src/dcag/_loaders.py` — in `_parse_step()` StepDef constructor, add:

```python
            loop=raw.get("loop"),
```

### Step 4.4 — Implement loop tracking in Walker

- [ ] Modify `src/dcag/_walker.py`

Replace the entire file with the loop-aware implementation:

```python
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
            # Loop exhausted — clear and fall through to advance step
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

        return None  # no match → linear fallback
```

### Step 4.5 — Inject loop variable into context

- [ ] Modify `src/dcag/_context.py` — in `assemble_reason()`, after the cache merge block, add loop variable injection:

```python
        # Inject loop variable if walker provides one
        if loop_var is not None:
            var_name, var_value = loop_var
            dynamic[var_name] = var_value
```

Update `assemble_reason()` signature to accept `loop_var`:

```python
    def assemble_reason(
        self,
        step: StepDef,
        persona: PersonaBundle,
        prior_outputs: dict[str, Any],
        workflow_inputs: dict[str, Any],
        schema_cache: dict[str, Any] | None = None,
        loop_var: tuple[str, Any] | None = None,
    ) -> ReasonRequest:
```

- [ ] Modify `src/dcag/engine.py` — in `next_step()`, pass loop variable:

In the `step.mode == "reason"` block (around line 118-124), add loop_var:

```python
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
            )
```

### Step 4.6 — Engine loop item resolution

- [ ] Modify `src/dcag/engine.py` — in `next_step()`, resolve loop items from prior outputs when a loop step is first encountered

Before the `step.mode == "reason"` check, add:

```python
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
```

### Step 4.7 — Verify

- [ ] Run loop tests

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_loops.py -v
# Expected: 8 passed
```

- [ ] Run full suite

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
# Expected: 160 passed, 2 skipped (152 prior + 8 loops)
```

### Step 4.8 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add src/dcag/types.py src/dcag/_walker.py src/dcag/_context.py src/dcag/_loaders.py src/dcag/engine.py tests/test_loops.py
git commit -m "$(cat <<'EOF'
feat(engine): step loops for iterating over collections

Steps declare loop: {over: path, as: name} to iterate over a
collection from a prior step. Walker tracks loop index and items.
Engine resolves loop items from prior outputs and injects the
loop variable into step context.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Decision Trace Persistence

**Goal:** After a workflow completes, persist key decisions as searchable JSON files. Future workflow runs can query past decisions by entity name.

**Key files:**
- New: `src/dcag/_decisions.py` (~80 lines)
- New: `tests/test_decisions.py`
- `src/dcag/engine.py` — `WorkflowRun` at lines 74-215, add persistence on completion
- `src/dcag/_context.py` — add `build_decisions()` method
- `src/dcag/_loaders.py` — parse `context.decisions` from YAML
- `src/dcag/types.py` — add `context_decisions: list[dict]` to StepDef

### Step 5.1 — Write tests first

- [ ] Create `tests/test_decisions.py`

```python
"""Tests for decision trace persistence."""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from dcag._decisions import DecisionStore


@pytest.fixture
def store(tmp_path):
    """Create a DecisionStore with a temp directory."""
    return DecisionStore(tmp_path / "decisions")


class TestDecisionStore:
    """Tests for writing, loading, and searching decision traces."""

    def test_write_creates_file(self, store):
        """write() creates a JSON file in entity subdirectory."""
        store.write(
            run_id="dcag-abc123",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY", "keys": ["SALE_DATE"]},
            confidence="high",
        )
        entity_dir = store._base_dir / "DW.RPT.TRANSACTION"
        assert entity_dir.exists()
        files = list(entity_dir.glob("*.json"))
        assert len(files) == 1

    def test_write_content_structure(self, store):
        """Written JSON has expected structure."""
        store.write(
            run_id="dcag-abc123",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY"},
            confidence="high",
        )
        files = list((store._base_dir / "DW.RPT.TRANSACTION").glob("*.json"))
        data = json.loads(files[0].read_text())
        assert data["workflow"] == "table-optimizer"
        assert data["run_id"] == "dcag-abc123"
        assert data["entity"] == "DW.RPT.TRANSACTION"
        assert data["facts"]["strategy"] == "CLUSTER_BY"
        assert data["confidence"] == "high"
        assert "decided_at" in data

    def test_load_returns_decision(self, store):
        """load() reads back a written decision."""
        store.write(
            run_id="dcag-abc123",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY"},
            confidence="high",
        )
        decisions = store.load("DW.RPT.TRANSACTION")
        assert len(decisions) == 1
        assert decisions[0]["run_id"] == "dcag-abc123"

    def test_load_empty_entity(self, store):
        """load() returns empty list for unknown entity."""
        decisions = store.load("NONEXISTENT.TABLE")
        assert decisions == []

    def test_search_by_entity(self, store):
        """search_by_entity finds decisions for a given entity."""
        store.write(
            run_id="dcag-111",
            workflow_id="table-optimizer",
            entity="DW.RPT.TRANSACTION",
            facts={"strategy": "CLUSTER_BY"},
            confidence="high",
        )
        store.write(
            run_id="dcag-222",
            workflow_id="add-column",
            entity="DW.RPT.TRANSACTION",
            facts={"column": "PCID"},
            confidence="medium",
        )
        store.write(
            run_id="dcag-333",
            workflow_id="table-optimizer",
            entity="DW.CORE.VENUE_DIM",
            facts={"strategy": "SKIP"},
            confidence="high",
        )
        results = store.search_by_entity("DW.RPT.TRANSACTION")
        assert len(results) == 2
        run_ids = {r["run_id"] for r in results}
        assert run_ids == {"dcag-111", "dcag-222"}

    def test_multiple_writes_same_entity(self, store):
        """Multiple writes to same entity create separate files."""
        for i in range(3):
            store.write(
                run_id=f"dcag-{i}",
                workflow_id="opt",
                entity="DW.RPT.T",
                facts={"i": i},
                confidence="high",
            )
        decisions = store.load("DW.RPT.T")
        assert len(decisions) == 3

    def test_write_with_valid_until(self, store):
        """valid_until field is persisted."""
        store.write(
            run_id="dcag-abc",
            workflow_id="opt",
            entity="DW.RPT.T",
            facts={},
            confidence="high",
            valid_until="2026-06-12",
        )
        decisions = store.load("DW.RPT.T")
        assert decisions[0]["valid_until"] == "2026-06-12"

    def test_base_dir_created_on_write(self, tmp_path):
        """Base directory is created if it doesn't exist."""
        deep_path = tmp_path / "a" / "b" / "decisions"
        store = DecisionStore(deep_path)
        store.write(
            run_id="dcag-x",
            workflow_id="w",
            entity="E",
            facts={},
            confidence="low",
        )
        assert deep_path.exists()
```

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_decisions.py -v
# Expected: FAIL (module doesn't exist)
```

### Step 5.2 — Implement DecisionStore

- [ ] Create `src/dcag/_decisions.py`

```python
"""Decision trace persistence — write/read/search decision traces as JSON files."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class DecisionStore:
    """Persists decision traces as JSON files indexed by entity.

    Storage layout:
        {base_dir}/{entity_name}/{run_id}.json
    """

    def __init__(self, base_dir: Path):
        self._base_dir = Path(base_dir)

    def write(
        self,
        run_id: str,
        workflow_id: str,
        entity: str,
        facts: dict[str, Any],
        confidence: str,
        valid_until: str | None = None,
    ) -> Path:
        """Write a decision trace to disk.

        Returns:
            Path to the written JSON file.
        """
        entity_dir = self._base_dir / entity
        entity_dir.mkdir(parents=True, exist_ok=True)

        trace = {
            "workflow": workflow_id,
            "run_id": run_id,
            "entity": entity,
            "decided_at": datetime.now(timezone.utc).isoformat(),
            "facts": facts,
            "confidence": confidence,
        }
        if valid_until:
            trace["valid_until"] = valid_until

        path = entity_dir / f"{run_id}.json"
        path.write_text(json.dumps(trace, indent=2, default=str))
        return path

    def load(self, entity: str) -> list[dict[str, Any]]:
        """Load all decision traces for an entity.

        Returns:
            List of decision dicts, sorted by decided_at (newest first).
        """
        entity_dir = self._base_dir / entity
        if not entity_dir.exists():
            return []

        decisions = []
        for path in entity_dir.glob("*.json"):
            decisions.append(json.loads(path.read_text()))

        decisions.sort(key=lambda d: d.get("decided_at", ""), reverse=True)
        return decisions

    def search_by_entity(self, entity: str) -> list[dict[str, Any]]:
        """Search for decisions matching an entity. Alias for load()."""
        return self.load(entity)
```

### Step 5.3 — Add `context_decisions` to StepDef

- [ ] Modify `src/dcag/types.py` — add field to `StepDef` after `context_cache`:

```python
    context_decisions: list[dict] = field(default_factory=list)
```

### Step 5.4 — Parse `context.decisions` in loader

- [ ] Modify `src/dcag/_loaders.py` — in `_parse_step()` StepDef constructor, add:

```python
            context_decisions=ctx.get("decisions", []),
```

### Step 5.5 — Add `build_decisions()` to ContextAssembler

- [ ] Modify `src/dcag/_context.py`

Add import at top:

```python
from dcag._decisions import DecisionStore
```

Add `build_decisions()` method to `ContextAssembler`:

```python
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
            # Resolve template variables: {{inputs.table_name}} → actual value
            if "{{" in entity:
                for key, val in workflow_inputs.items():
                    entity = entity.replace(f"{{{{inputs.{key}}}}}", str(val))
            decisions = decision_store.search_by_entity(entity)
            if decisions:
                result[f"decisions:{entity}"] = decisions
        return result
```

In `assemble_reason()`, add `decision_store` parameter and merge decisions:

```python
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
```

After the loop variable injection block, add:

```python
        # Load decision traces
        decisions = self.build_decisions(step.context_decisions, decision_store, workflow_inputs)
        if decisions:
            dynamic.update(decisions)
```

### Step 5.6 — Persist decisions on workflow completion

- [ ] Modify `src/dcag/engine.py`

Add import at top:

```python
from dcag._decisions import DecisionStore
```

In `WorkflowRun.__init__()`, add decision store parameter and field:

```python
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
        # ... existing init ...
        self._decision_store = DecisionStore(decisions_dir) if decisions_dir else None
```

In `record_result()`, after the block that sets `self._status = "completed"` at line 196-197, add persistence:

```python
                if self._decision_store:
                    self._persist_decisions()
```

Add the persistence method to `WorkflowRun`:

```python
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
```

In `DCAGEngine.start()`, pass `decisions_dir`:

```python
    def start(self, workflow_id: str, inputs: dict[str, Any],
              decisions_dir: str | Path | None = None) -> WorkflowRun:
        """Start a new workflow run."""
        # ... existing code ...
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
```

Pass `decision_store` to `assemble_reason()` in `next_step()`:

```python
                decision_store=self._decision_store,
```

### Step 5.7 — Verify

- [ ] Run decision tests

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_decisions.py -v
# Expected: 9 passed
```

- [ ] Run full suite

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
# Expected: 169 passed, 2 skipped (160 prior + 9 decisions)
```

### Step 5.8 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add src/dcag/_decisions.py src/dcag/types.py src/dcag/engine.py src/dcag/_context.py src/dcag/_loaders.py tests/test_decisions.py
git commit -m "$(cat <<'EOF'
feat(engine): decision trace persistence for cross-workflow learning

DecisionStore writes/reads JSON traces indexed by entity name. Engine
persists decisions on workflow completion. Steps declare
context.decisions to load matching traces into their context.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Knowledge Files (8 files)

**Goal:** Create 8 knowledge YAML files in `content/knowledge/` following the existing pattern (see `optimization_rules.yml` for structure: `knowledge.id`, `knowledge.description`, `knowledge.guidance` list).

### Step 6.1 — Create troubleshooting_patterns.yml

- [ ] Create `content/knowledge/troubleshooting_patterns.yml`

```yaml
knowledge:
  id: troubleshooting_patterns
  domain: debugging
  description: "Common dbt model error patterns and their canonical fixes"
  guidance:
    - "CAST errors: check source column types vs target — use TRY_CAST for nullable conversions"
    - "JOIN errors: verify join keys match cardinality — look for fanout (1:N becoming N:M)"
    - "NULL propagation: trace NULLs upstream — fix at the source, not with COALESCE hacks"
    - "Ambiguous column: always qualify with table alias in multi-table queries"
    - "Type mismatch in UNION: align column types explicitly — Snowflake implicit casts are unreliable"
    - "Division by zero: wrap with NULLIF(denominator, 0) — never use CASE WHEN"
    - "Timestamp timezone issues: always use CONVERT_TIMEZONE before date comparisons"
    - "Duplicate rows after JOIN: check if join produces fanout — add QUALIFY ROW_NUMBER if needed"
  error_categories:
    cast_error:
      signals: ["invalid type", "cannot cast", "numeric value is not recognized"]
      fix_pattern: "Identify source column type → TRY_CAST to target → add NOT NULL test"
    join_error:
      signals: ["ambiguous column", "duplicate rows", "fanout"]
      fix_pattern: "Check join cardinality → add DISTINCT or QUALIFY → verify row counts"
    logic_error:
      signals: ["unexpected NULL", "wrong aggregation", "missing rows"]
      fix_pattern: "Trace data lineage → compare expected vs actual at each CTE → fix source"
```

### Step 6.2 — Create data_quality_checks.yml

- [ ] Create `content/knowledge/data_quality_checks.yml`

```yaml
knowledge:
  id: data_quality_checks
  domain: testing
  description: "Data quality check patterns for validating dbt model outputs"
  guidance:
    - "Always check row count before and after transformation — flag >10% variance"
    - "NULL rate checks: compare source vs target NULL percentages per column"
    - "Uniqueness: verify primary keys are truly unique after joins"
    - "Freshness: add source freshness tests — warn at 12h, error at 24h for daily models"
    - "Referential integrity: foreign keys must exist in parent table"
    - "Range validation: numeric columns should stay within expected bounds"
    - "Accepted values: enum columns must only contain known values"
  check_types:
    row_count: "SELECT COUNT(*) vs expected range"
    null_rate: "SELECT COUNT(*) FILTER (WHERE col IS NULL) / COUNT(*)"
    uniqueness: "SELECT col, COUNT(*) GROUP BY col HAVING COUNT(*) > 1"
    freshness: "SELECT MAX(updated_at), DATEDIFF(hour, MAX(updated_at), CURRENT_TIMESTAMP())"
```

### Step 6.3 — Create model_templates.yml

- [ ] Create `content/knowledge/model_templates.yml`

```yaml
knowledge:
  id: model_templates
  domain: modeling
  description: "CTE patterns and materialization templates for dbt staging models"
  guidance:
    - "Staging models: always start with a source CTE, then rename, then type-cast, then filter"
    - "Use 'stg_' prefix for staging, 'int_' for intermediate, 'fct_' for facts, 'dim_' for dimensions"
    - "Materialization: staging = view, intermediate = ephemeral, facts/dims = table or incremental"
    - "Incremental: use unique_key + merge strategy — never append-only for fact tables"
    - "Column order: keys first, dimensions second, metrics third, metadata last"
    - "Always include _loaded_at and _source_id metadata columns in staging models"
  cte_pattern:
    staging: |
      WITH source AS (
          SELECT * FROM {{ source('schema', 'table') }}
      ),
      renamed AS (
          SELECT
              column_a AS clean_name_a,
              column_b AS clean_name_b
          FROM source
      ),
      typed AS (
          SELECT
              clean_name_a::VARCHAR AS clean_name_a,
              clean_name_b::NUMBER AS clean_name_b
          FROM renamed
      )
      SELECT * FROM typed
```

### Step 6.4 — Create test_inference_rules.yml

- [ ] Create `content/knowledge/test_inference_rules.yml`

```yaml
knowledge:
  id: test_inference_rules
  domain: testing
  description: "Rules for inferring dbt tests from column names and types"
  guidance:
    - "Primary keys (_id, _key suffix): add unique + not_null tests"
    - "Foreign keys (_fk suffix or 'parent_' prefix): add relationships test"
    - "Enum columns (status, type, category): add accepted_values test"
    - "Date columns: add not_null + recency test where applicable"
    - "Boolean columns (is_, has_ prefix): add accepted_values [true, false]"
    - "Amount/currency columns: add not_null + expression test for non-negative"
    - "Email columns: add not_null + format test"
  inference_rules:
    - pattern: ".*_id$|.*_key$"
      tests: ["unique", "not_null"]
    - pattern: ".*_fk$|^parent_.*"
      tests: ["relationships", "not_null"]
    - pattern: "^(status|type|category)$|.*_(status|type|category)$"
      tests: ["accepted_values", "not_null"]
    - pattern: ".*_(date|at|timestamp)$"
      tests: ["not_null"]
    - pattern: "^(is_|has_).*"
      tests: ["accepted_values"]
    - pattern: ".*_(amount|price|cost|revenue|total)$"
      tests: ["not_null"]
```

### Step 6.5 — Create pipeline_threading_conventions.yml

- [ ] Create `content/knowledge/pipeline_threading_conventions.yml`

```yaml
knowledge:
  id: pipeline_threading_conventions
  domain: modeling
  description: "Conventions for threading a new field through a multi-model dbt pipeline"
  guidance:
    - "Start from the source/staging layer — never add a column mid-pipeline without tracing upstream"
    - "Thread in order: staging → intermediate → fact/dim — never skip layers"
    - "Each layer must explicitly SELECT the new column — never rely on SELECT *"
    - "Add the column to schema.yml at every layer it appears"
    - "Add appropriate tests at the first layer (staging) and the final layer (fact/dim)"
    - "If the column requires a JOIN to obtain, add it at the intermediate layer"
    - "Naming must be consistent across layers — use the final business name from staging onward"
    - "After threading, run dbt build for the full pipeline to catch downstream breakage"
  layer_order:
    - { name: "staging", prefix: "stg_", action: "Add column from source, rename, type-cast" }
    - { name: "intermediate", prefix: "int_", action: "Add JOINs if needed, apply business logic" }
    - { name: "fact_or_dim", prefix: "fct_/dim_", action: "Include in final SELECT, add to schema.yml" }
```

### Step 6.6 — Create database_classes.yml

- [ ] Create `content/knowledge/database_classes.yml`

```yaml
knowledge:
  id: database_classes
  domain: ingestion
  description: "Catalog of source database classes and their ingestion characteristics"
  guidance:
    - "SQL Server sources: use CDC for high-volume tables (>1M rows), batch for smaller"
    - "PostgreSQL sources: prefer logical replication for real-time, pg_dump for initial loads"
    - "MySQL sources: use binlog CDC — avoid full table scans on large tables"
    - "MongoDB sources: use change streams for real-time, mongodump for initial loads"
    - "API sources: implement retry with exponential backoff — respect rate limits"
    - "File sources (S3/GCS): use COPY INTO with pattern matching — never stage manually"
  database_types:
    sql_server:
      connector: "cdc_debezium"
      staging_format: "VARIANT → typed columns"
      frequency: "near-real-time or hourly"
    postgresql:
      connector: "logical_replication"
      staging_format: "typed columns direct"
      frequency: "near-real-time"
    api_rest:
      connector: "custom_python"
      staging_format: "VARIANT → typed columns"
      frequency: "hourly or daily"
    file_s3:
      connector: "snowpipe"
      staging_format: "stage → COPY INTO"
      frequency: "event-driven"
```

### Step 6.7 — Create sla_contracts.yml

- [ ] Create `content/knowledge/sla_contracts.yml`

```yaml
knowledge:
  id: sla_contracts
  domain: operations
  description: "SLA contracts for data freshness and pipeline completion times"
  guidance:
    - "Tier 1 (revenue-critical): data must land within 1 hour of source — alert at 30min delay"
    - "Tier 2 (operational): data must land within 4 hours — alert at 2h delay"
    - "Tier 3 (analytical): data must land within 24 hours — alert at 12h delay"
    - "All SLAs measured from source system commit time to Snowflake availability"
    - "CDC pipelines: target 15-minute lag for Tier 1 tables"
    - "Batch pipelines: schedule with 2x buffer over median runtime"
    - "Always set dbt source freshness tests matching the SLA tier"
  tiers:
    tier_1:
      max_delay_hours: 1
      alert_threshold_hours: 0.5
      examples: ["transactions", "payments", "inventory"]
    tier_2:
      max_delay_hours: 4
      alert_threshold_hours: 2
      examples: ["user_activity", "campaign_metrics"]
    tier_3:
      max_delay_hours: 24
      alert_threshold_hours: 12
      examples: ["historical_snapshots", "audit_logs"]
```

### Step 6.8 — Create dag_catalog.yml

- [ ] Create `content/knowledge/dag_catalog.yml`

```yaml
knowledge:
  id: dag_catalog
  domain: orchestration
  description: "Catalog of Airflow DAG patterns and scheduling conventions"
  guidance:
    - "DAG naming: {domain}_{frequency}_{action} — e.g., core_daily_transform"
    - "Use Cosmos DbtDagParser for all dbt-based DAGs — never manual task wiring"
    - "Schedule daily DAGs at staggered times to avoid warehouse contention"
    - "Set max_active_runs=1 for all transformation DAGs — prevent overlap"
    - "Use ExternalTaskSensor for cross-DAG dependencies — never time-based waits"
    - "Retries: 2 for transforms, 3 for ingestion, 0 for alerting"
    - "SLA callbacks: attach to final task in each DAG — not intermediate tasks"
    - "Tag all DAGs with domain and tier for filtering in Airflow UI"
  dag_patterns:
    daily_transform:
      schedule: "0 6 * * *"
      retries: 2
      warehouse: "TRANSFORM_M"
    hourly_ingestion:
      schedule: "0 * * * *"
      retries: 3
      warehouse: "INGEST_S"
    weekly_snapshot:
      schedule: "0 8 * * 0"
      retries: 1
      warehouse: "TRANSFORM_M"
```

### Step 6.9 — Verify knowledge files load correctly

- [ ] Quick validation test

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -c "
from pathlib import Path
from dcag._loaders import KnowledgeLoader
loader = KnowledgeLoader(Path('content/knowledge'))
new_files = [
    'troubleshooting_patterns', 'data_quality_checks', 'model_templates',
    'test_inference_rules', 'pipeline_threading_conventions', 'database_classes',
    'sla_contracts', 'dag_catalog',
]
for f in new_files:
    k = loader.load(f)
    assert 'id' in k, f'{f} missing id'
    assert 'guidance' in k, f'{f} missing guidance'
    print(f'  {f}: {len(k[\"guidance\"])} guidance items')
print('All 8 knowledge files validated.')
"
```

- [ ] Run full suite

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
# Expected: 169 passed, 2 skipped (no new tests, just knowledge files)
```

### Step 6.10 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add content/knowledge/troubleshooting_patterns.yml content/knowledge/data_quality_checks.yml content/knowledge/model_templates.yml content/knowledge/test_inference_rules.yml content/knowledge/pipeline_threading_conventions.yml content/knowledge/database_classes.yml content/knowledge/sla_contracts.yml content/knowledge/dag_catalog.yml
git commit -m "$(cat <<'EOF'
feat(knowledge): add 8 knowledge files for v2 workflows

troubleshooting_patterns, data_quality_checks, model_templates,
test_inference_rules, pipeline_threading_conventions, database_classes,
sla_contracts, dag_catalog — covering all 6 planned workflows.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Update Public API and Final Verification

**Goal:** Export new public types, run the complete test suite, make a final commit.

### Step 7.1 — Update `__init__.py`

- [ ] Modify `src/dcag/__init__.py` (currently lines 1-57)

Add new imports and exports:

```python
from dcag._decisions import DecisionStore
from dcag._evaluator import evaluate
```

Add to `__all__` list:

```python
    "DecisionStore",
    "evaluate",
```

### Step 7.2 — Final full test run

- [ ] Run complete suite

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -v
# Expected: 169 passed, 2 skipped
# Breakdown:
#   - 125 existing tests (unchanged)
#   - 15 evaluator tests (Task 1)
#   - 7 conditional walker tests (Task 2)
#   - 5 schema cache tests (Task 3)
#   - 8 loop tests (Task 4)
#   - 9 decision tests (Task 5)
#   Total new: 44 tests
```

### Step 7.3 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add src/dcag/__init__.py
git commit -m "$(cat <<'EOF'
feat(api): export DecisionStore and evaluate in public API

Final commit for DCAG v2 Part 1: 4 engine features (expression
evaluator, conditional walker, schema cache, step loops, decision
traces) + 8 knowledge files. 44 new tests, 169 total passing.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Summary

| Task | New Files | Modified Files | New Tests | Lines (est.) |
|------|-----------|----------------|-----------|-------------|
| 1. Expression Evaluator | `_evaluator.py`, `test_evaluator.py` | — | 15 | ~130 |
| 2. Conditional Walker | — | `_walker.py`, `engine.py`, `test_walker.py` | 7 | ~120 |
| 3. Schema Cache | — | `types.py`, `engine.py`, `_context.py`, `_loaders.py`, `test_context.py` | 5 | ~60 |
| 4. Step Loops | `test_loops.py` | `types.py`, `_walker.py`, `_context.py`, `_loaders.py`, `engine.py` | 8 | ~140 |
| 5. Decision Traces | `_decisions.py`, `test_decisions.py` | `types.py`, `engine.py`, `_context.py`, `_loaders.py` | 9 | ~200 |
| 6. Knowledge Files | 8 `.yml` files | — | 0 | ~250 |
| 7. Public API | — | `__init__.py` | 0 | ~5 |
| **Total** | **12 new files** | **8 modified files** | **44** | **~905** |

**Build order:** Task 1 → Task 2 (depends on 1) → Task 3 (independent) → Task 4 (depends on 1+2) → Task 5 (independent) → Task 6 (independent) → Task 7 (final)

**Existing test preservation:** Every task ends with a full-suite run verifying 125 original tests still pass. The Walker's `advance()` is backward-compatible: `step_output=None` (default) = linear advance.
