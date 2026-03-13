# Tool Filtering in Degraded Mode — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire ToolRegistry.resolve_available() into the context assembly path so ReasonRequest.tools only contains tools that are actually available at runtime.

**Architecture:** Pass ToolRegistry into ContextAssembler. The assembler calls resolve_available() before building ReasonRequest, so prompts are self-contained. Generalize the step 0 capability hook to work with any setup step. Add github_available to the degradation matrix. Update workflow YAMLs with degraded-mode hints.

**Tech Stack:** Python 3.14, dataclasses, YAML, pytest

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/dcag/_registry.py:22-24` | Add `github_available` requirement to github_cli tools |
| Modify | `src/dcag/_context.py:20-25,100,120` | Accept ToolRegistry, filter tools, fix token estimate |
| Modify | `src/dcag/engine.py:42,56,82,95,116-121,182-184` | Pass registry to assembler, generalize step 0 hook |
| Modify | `content/workflows/add-column-to-model.yml:27-28` | Add `github_available` to step 0 output schema |
| Modify | `content/workflows/add-column-to-model.yml` | Add degraded-mode hints to steps 1, 4, 6, 7, 8 |
| Modify | `content/workflows/table-optimizer.yml` | Add hint to step 3 (assess_clustering unclustered table error) |
| Modify | `tests/test_registry.py` | Add github_available filtering tests |
| Modify | `tests/test_context.py` | Update assembler fixture, add tool filtering tests |
| Create | `tests/test_tool_filtering_e2e.py` | E2E test: full workflow in degraded mode verifies filtered tools |

---

## Chunk 1: Registry + Assembler + Engine Wiring

### Task 1: Add github_available to ToolRegistry

**Files:**
- Modify: `src/dcag/_registry.py:22-24`
- Test: `tests/test_registry.py`

- [ ] **Step 1: Write failing tests for github_cli filtering**

Add to `tests/test_registry.py`:

```python
def test_github_filtered_when_unavailable(self):
    reg = ToolRegistry()
    reg.update_capabilities({
        "dbt_available": True,
        "dbt_mcp_available": True,
        "github_available": False,
    })
    tools = [
        ToolDirective("dbt_mcp.compile", "compile model"),
        ToolDirective("github_cli.search_code", "search code"),
        ToolDirective("github_cli.read_file", "read file"),
        ToolDirective("snowflake_mcp.execute_query", "run query"),
    ]
    available = reg.resolve_available(tools)
    names = [t.name for t in available]
    assert "github_cli.search_code" not in names
    assert "github_cli.read_file" not in names
    assert "dbt_mcp.compile" in names
    assert "snowflake_mcp.execute_query" in names

def test_snowflake_only_mode(self):
    """When both dbt and github are unavailable, only snowflake tools remain."""
    reg = ToolRegistry()
    reg.update_capabilities({
        "dbt_available": False,
        "dbt_mcp_available": False,
        "github_available": False,
    })
    tools = [
        ToolDirective("dbt_mcp.compile", "compile"),
        ToolDirective("github_cli.read_file", "read"),
        ToolDirective("snowflake_mcp.execute_query", "query"),
        ToolDirective("snowflake_mcp.describe_table", "describe"),
    ]
    available = reg.resolve_available(tools)
    names = [t.name for t in available]
    assert names == ["snowflake_mcp.execute_query", "snowflake_mcp.describe_table"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/test_registry.py -v`
Expected: 2 FAIL (github_cli tools not filtered because they have `[]` requirements)

- [ ] **Step 3: Update DEFAULT_TOOL_REQUIREMENTS**

In `src/dcag/_registry.py`, change lines 22-24 from:

```python
    "github_cli.read_file": [],
    "github_cli.search_code": [],
    "github_cli.create_pr": [],
```

to:

```python
    "github_cli.read_file": ["github_available"],
    "github_cli.search_code": ["github_available"],
    "github_cli.create_pr": ["github_available"],
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/test_registry.py -v`
Expected: ALL PASS (7 tests)

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All 118 pass, 2 skipped. The existing test `test_dbt_filtered_when_unavailable` currently asserts `github_cli.search_code` IS available (line 33: `assert len(available) == 2`). This will still pass because that test doesn't set `github_available: False` — unknown capabilities default to True.

- [ ] **Step 6: Commit**

```bash
git add src/dcag/_registry.py tests/test_registry.py
git commit -m "feat: add github_available to tool degradation matrix"
```

---

### Task 2: Wire ToolRegistry into ContextAssembler

**Files:**
- Modify: `src/dcag/_context.py:20-25,100,120`
- Test: `tests/test_context.py`

- [ ] **Step 1: Write failing test for tool filtering in assembler**

Add to `tests/test_context.py`:

```python
from dcag._registry import ToolRegistry

@pytest.fixture
def registry_degraded():
    """Registry in snowflake-only mode."""
    reg = ToolRegistry()
    reg.update_capabilities({
        "dbt_available": False,
        "dbt_mcp_available": False,
        "github_available": False,
    })
    return reg

@pytest.fixture
def assembler_with_registry(registry_degraded):
    return ContextAssembler(
        persona_loader=PersonaLoader(CONTENT_DIR / "personas"),
        knowledge_loader=KnowledgeLoader(CONTENT_DIR / "knowledge"),
        registry=registry_degraded,
    )
```

Add test class:

```python
class TestToolFiltering:
    def test_degraded_mode_filters_dbt_and_github(self, assembler_with_registry, workflow):
        """In snowflake_only mode, only snowflake tools appear in ReasonRequest."""
        step = workflow.steps[1]  # resolve_model: has dbt + github + snowflake tools
        persona = PersonaLoader(CONTENT_DIR / "personas").load("analytics_engineer")
        request = assembler_with_registry.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        tool_names = [t.name for t in request.tools]
        assert "snowflake_mcp.execute_query" in tool_names
        assert "dbt_mcp.get_node_details_dev" not in tool_names
        assert "github_cli.search_code" not in tool_names
        assert "github_cli.read_file" not in tool_names

    def test_full_mode_keeps_all_tools(self, workflow):
        """With no registry (default), all tools pass through."""
        assembler = ContextAssembler(
            persona_loader=PersonaLoader(CONTENT_DIR / "personas"),
            knowledge_loader=KnowledgeLoader(CONTENT_DIR / "knowledge"),
        )
        step = workflow.steps[1]  # resolve_model
        persona = PersonaLoader(CONTENT_DIR / "personas").load("analytics_engineer")
        request = assembler.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        # All 4 tools from YAML pass through when no registry
        assert len(request.tools) == 4

    def test_token_estimate_uses_filtered_tools(self, assembler_with_registry, workflow):
        """Token estimate should reflect only available tools, not all declared tools."""
        assembler_full = ContextAssembler(
            persona_loader=PersonaLoader(CONTENT_DIR / "personas"),
            knowledge_loader=KnowledgeLoader(CONTENT_DIR / "knowledge"),
        )
        step = workflow.steps[1]
        persona = PersonaLoader(CONTENT_DIR / "personas").load("analytics_engineer")

        request_full = assembler_full.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        request_degraded = assembler_with_registry.assemble_reason(
            step=step, persona=persona, prior_outputs={},
            workflow_inputs={"model_name": "test", "column_name": "pcid"},
        )
        # Degraded mode has fewer tools → fewer tokens
        assert request_degraded.context.estimated_tokens < request_full.context.estimated_tokens
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/test_context.py::TestToolFiltering -v`
Expected: FAIL (ContextAssembler doesn't accept `registry` param yet)

- [ ] **Step 3: Update ContextAssembler to accept and use ToolRegistry**

In `src/dcag/_context.py`:

1. Add import at line 7:
```python
from dcag._registry import ToolRegistry
```

2. Update `__init__` (line 23) to accept optional registry:
```python
def __init__(self, persona_loader: PersonaLoader, knowledge_loader: KnowledgeLoader, registry: ToolRegistry | None = None):
    self._personas = persona_loader
    self._knowledge = knowledge_loader
    self._registry = registry
```

3. In `assemble_reason`, change line 100 (token estimation) to use filtered tools:
```python
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
```

4. Change line 120 to use filtered tools:
```python
            tools=available_tools,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source .venv/bin/activate && pytest tests/test_context.py -v`
Expected: ALL PASS

- [ ] **Step 5: Run full suite for regressions**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All 118+ pass, 2 skipped. Existing tests use `ContextAssembler(persona_loader, knowledge_loader)` without registry — this still works because registry defaults to None (no filtering).

- [ ] **Step 6: Commit**

```bash
git add src/dcag/_context.py tests/test_context.py
git commit -m "feat: wire ToolRegistry into ContextAssembler for tool filtering"
```

---

### Task 3: Pass ToolRegistry from WorkflowRun to Assembler + Generalize Step 0 Hook

**Files:**
- Modify: `src/dcag/engine.py:42,51-57,82,95,116-121,182-184`
- Test: `tests/test_context.py` (existing tests cover this via E2E)

- [ ] **Step 1: Update DCAGEngine to create registry and pass to assembler**

In `src/dcag/engine.py`:

1. Move assembler creation into `start()` so each run gets its own registry. Change lines 37-58:

```python
class DCAGEngine:
    """Entry point. Loads content, creates workflow runs."""

    def __init__(self, content_dir: str | Path):
        self._content_dir = Path(content_dir)
        self._persona_loader = PersonaLoader(self._content_dir / "personas")
        self._knowledge_loader = KnowledgeLoader(self._content_dir / "knowledge")
        self._workflow_loader = WorkflowLoader(self._content_dir / "workflows")

    def start(self, workflow_id: str, inputs: dict[str, Any]) -> WorkflowRun:
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
        )
```

2. Update `WorkflowRun.__init__` (lines 75-95) to accept registry:

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
):
    self._run_id = run_id
    self._workflow = workflow
    self._persona = persona
    self._inputs = inputs
    self._assembler = assembler
    self._walker = Walker(workflow.steps)
    self._prior_outputs: dict[str, Any] = {}
    self._status = "running"
    self._trace = TraceWriter(run_id, Path(tempfile.gettempdir()) / "dcag-runs")
    self._trace.record_start(workflow.id, inputs, config_hash)
    self._step_start_time: float = 0
    self._registry = registry or ToolRegistry()
```

3. Generalize the step 0 capability hook (lines 182-184). Change from:

```python
if step_id == "setup_dbt_project" and isinstance(outcome.output, dict):
    self._registry.update_capabilities(outcome.output)
```

to:

```python
# Auto-populate ToolRegistry from any execute/script step output
# that reports capability fields (dbt_available, github_available, etc.)
if step.mode == "execute" and step.execute_type == "script" and isinstance(outcome.output, dict):
    capability_keys = {"dbt_available", "dbt_mcp_available", "github_available", "fallback_mode"}
    if capability_keys & outcome.output.keys():
        self._registry.update_capabilities(outcome.output)
```

- [ ] **Step 2: Run full suite to verify no regressions**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All pass. The change is backward-compatible — `registry` defaults to None in WorkflowRun, and ContextAssembler defaults to no-filtering when registry is None.

- [ ] **Step 3: Commit**

```bash
git add src/dcag/engine.py
git commit -m "feat: pass ToolRegistry to assembler, generalize capability detection hook"
```

---

## Chunk 2: E2E Test + Workflow YAML Updates

### Task 4: E2E test — full workflow in degraded mode verifies filtered tools

**Files:**
- Create: `tests/test_tool_filtering_e2e.py`

- [ ] **Step 1: Write E2E test**

Create `tests/test_tool_filtering_e2e.py`:

```python
"""E2E test: add-column-to-model in degraded mode filters unavailable tools."""
import json
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.types import (
    DelegateRequest,
    ExecuteScriptRequest,
    ReasonRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"


class TestDegradedModeToolFiltering:
    """Verify that after step 0 reports degraded capabilities,
    subsequent ReasonRequests only contain available tools."""

    def test_snowflake_only_filters_dbt_and_github_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-column-to-model", {
            "model_name": "test_model",
            "column_name": "TEST_COL",
        })

        reason_tools: dict[str, list[str]] = {}

        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            if isinstance(request, ExecuteScriptRequest):
                # Report degraded: snowflake only
                run.record_result(request.step_id, StepSuccess(output={
                    "dbt_project_path": None,
                    "dbt_available": False,
                    "dbt_mcp_available": False,
                    "github_available": False,
                    "fallback_mode": "snowflake_only",
                }))

            elif isinstance(request, ReasonRequest):
                # Capture which tools each step received
                reason_tools[request.step_id] = [t.name for t in request.tools]

                # Provide dummy output to advance
                dummy_outputs = {
                    "resolve_model": {
                        "model_path": "models/test.sql",
                        "sources_yml_path": "models/sources.yml",
                        "source_ref": "{{ source('raw', 'test') }}",
                        "existing_columns": ["COL_A"],
                        "source_table_fqn": "RAW.PUBLIC.TEST",
                    },
                    "discover_column": {
                        "column_info": {"name": "TEST_COL", "source_type": "VARCHAR", "sf_type": "VARCHAR", "nullable": True},
                    },
                    "determine_logic": {
                        "intent_level": "passthrough", "column_expression": "TEST_COL", "join_required": False,
                    },
                    "check_downstream_impact": {
                        "downstream_impact": {"affected_models": [], "select_star_risk": False, "impact_level": "safe"},
                    },
                    "modify_staging_sql": {"modified_sql": "SELECT COL_A, TEST_COL FROM src", "changes_made": ["added"]},
                    "update_schema_yml": {"modified_yml": "- name: test_col", "changes_made": ["added"]},
                    "validate": {"tests_passed": True, "compile_ok": True, "parse_ok": True},
                }
                run.record_result(request.step_id, StepSuccess(output=dummy_outputs.get(request.step_id, {})))

            elif isinstance(request, DelegateRequest):
                run.record_result(request.step_id, StepSuccess(output={"approved": True}))

        assert run.status == "completed"

        # Key assertions: no dbt or github tools in any step
        for step_id, tools in reason_tools.items():
            for tool in tools:
                assert not tool.startswith("dbt_mcp."), (
                    f"Step '{step_id}' has dbt tool '{tool}' in degraded mode"
                )
                assert not tool.startswith("github_cli."), (
                    f"Step '{step_id}' has github tool '{tool}' in degraded mode"
                )

        # resolve_model should still have snowflake tool
        assert "snowflake_mcp.execute_query" in reason_tools["resolve_model"]

        # discover_column should still have both snowflake tools
        assert "snowflake_mcp.describe_table" in reason_tools["discover_column"]
        assert "snowflake_mcp.execute_query" in reason_tools["discover_column"]

        # determine_logic has no tools (pure reasoning) — should be empty
        assert reason_tools["determine_logic"] == []

        # validate should have only snowflake
        assert "snowflake_mcp.execute_query" in reason_tools["validate"]
        assert len([t for t in reason_tools["validate"] if t.startswith("dbt_mcp.")]) == 0

    def test_full_mode_keeps_all_tools(self):
        """When step 0 reports full capabilities, all tools remain."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-column-to-model", {
            "model_name": "test_model",
            "column_name": "TEST_COL",
        })

        request = run.next_step()  # step 0: ExecuteScriptRequest
        assert isinstance(request, ExecuteScriptRequest)

        run.record_result(request.step_id, StepSuccess(output={
            "dbt_project_path": "/tmp/project",
            "dbt_available": True,
            "dbt_mcp_available": True,
            "github_available": True,
            "fallback_mode": "full",
        }))

        request = run.next_step()  # step 1: resolve_model
        assert isinstance(request, ReasonRequest)
        tool_names = [t.name for t in request.tools]

        # All 4 tools present
        assert "dbt_mcp.get_node_details_dev" in tool_names
        assert "github_cli.search_code" in tool_names
        assert "github_cli.read_file" in tool_names
        assert "snowflake_mcp.execute_query" in tool_names
```

- [ ] **Step 2: Run test to verify it passes**

Run: `source .venv/bin/activate && pytest tests/test_tool_filtering_e2e.py -v`
Expected: 2 PASS

- [ ] **Step 3: Run full suite**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All pass (120+ tests, 2 skipped)

- [ ] **Step 4: Commit**

```bash
git add tests/test_tool_filtering_e2e.py
git commit -m "test: E2E test for tool filtering in degraded mode"
```

---

### Task 5: Update workflow YAMLs with degraded-mode hints and github_available

**Files:**
- Modify: `content/workflows/add-column-to-model.yml`
- Modify: `content/workflows/table-optimizer.yml`

- [ ] **Step 1: Update add-column-to-model.yml step 0 output schema**

Add `github_available` to the required fields at line 28:

```yaml
      output_schema:
        type: object
        required: [dbt_project_path, dbt_available, dbt_mcp_available, github_available, fallback_mode]
```

- [ ] **Step 2: Add degraded-mode hints to add-column-to-model.yml steps**

For `resolve_model` (step 1), append to instruction:

```yaml
      instruction: |
        Find the dbt model file and its sources.yml for the given model name.
        Use dbt_mcp.get_node_details_dev to locate the model, then read the SQL file
        and sources.yml via GitHub CLI to get existing columns and source definition.
        Also map the dbt source reference to a fully-qualified Snowflake table name —
        read sources.yml to get database/schema, then verify the table exists in Snowflake.
        DEGRADED MODE: If dbt and GitHub tools are not available, use Snowflake
        INFORMATION_SCHEMA.COLUMNS to discover the table structure directly.
        Report source_table_fqn from the input or by searching INFORMATION_SCHEMA.TABLES.
```

For `check_downstream_impact` (step 4), append to instruction:

```yaml
      instruction: |
        Check what models depend on this model. Use dbt_mcp.get_lineage_dev first,
        fall back to GitHub code search for ref() patterns if lineage unavailable.
        Assess SELECT * risk and overall impact level.
        DEGRADED MODE: If neither dbt nor GitHub tools are available, report
        impact_level as "unknown" and recommend manual downstream review.
```

For `modify_staging_sql` (step 6), append to instruction:

```yaml
      instruction: |
        Read the existing SQL file and produce the FULL modified file with the new column added.
        Infer the existing coding style (uppercase/lowercase, indentation, column ordering).
        Place the new column near related columns, not at the end.
        Output the complete modified file content — not a diff or partial snippet.
        DEGRADED MODE: If the file cannot be read (GitHub tools unavailable),
        reconstruct the SQL from existing_columns (Step 1) using standard dbt CTE pattern.
```

For `update_schema_yml` (step 7), append to instruction:

```yaml
      instruction: |
        Read the existing schema.yml and add the column entry.
        Include appropriate tests (not_null if column is non-nullable, unique if PK).
        Write a clear description. Output the column entry YAML.
        DEGRADED MODE: If the file cannot be read (GitHub tools unavailable),
        generate a standalone column entry YAML block based on column_info from Step 2.
```

For `validate` (step 8) — already has degraded-mode instruction, no change needed.

- [ ] **Step 3: Add hint to table-optimizer.yml step 3 (assess_clustering)**

Append to the `assess_clustering` step instruction:

```yaml
        NOTE: SYSTEM$CLUSTERING_INFORMATION will throw an error if the table is not
        currently clustered. This is expected — catch the error and report
        current_clustering as NONE, then proceed with your recommendation.
```

- [ ] **Step 4: Run full test suite**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All pass. YAML changes don't break tests — they only add text to instruction fields.

- [ ] **Step 5: Commit**

```bash
git add content/workflows/add-column-to-model.yml content/workflows/table-optimizer.yml
git commit -m "feat: add degraded-mode hints and github_available to workflow YAMLs"
```

---

### Task 6: Final verification and push

- [ ] **Step 1: Run full test suite one final time**

Run: `source .venv/bin/activate && pytest tests/ -v --tb=short`
Expected: All pass (120+ tests, 2 skipped)

- [ ] **Step 2: Review git log**

Run: `git log --oneline -8`
Expected: 4 new commits on top of Phase 3

- [ ] **Step 3: Push to personal GitHub**

Run: `git push origin main`
