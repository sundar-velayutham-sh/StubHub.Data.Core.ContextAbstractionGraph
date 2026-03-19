# CLAUDE.md — DCAG (Data Context Abstraction Graph)

> Repo-specific conventions for the DCAG workflow engine.
> These override global defaults in `~/code/CLAUDE.md`.

---

## What This Repo Is

DCAG is a **headless workflow engine** that orchestrates how Shift (StubHub's Slack AI assistant) investigates and resolves data engineering problems. It emits typed step requests — it **makes no LLM calls itself** and has no external runtime dependencies beyond YAML parsing.

**Core loop:** `engine.start()` → `run.next_step()` → driver executes → `run.record_result()` → repeat until done.

---

## Quick Reference

```bash
# Run all tests
just test                          # or: pytest

# Run with coverage
just test-cov                      # or: pytest --cov=dcag --cov-report=term-missing

# Lint
just lint                          # or: ruff check src/ tests/

# Format
just fmt                           # or: ruff format src/ tests/

# Pre-push check (lint + test)
just check

# Start API (dev)
just api                           # or: uvicorn dcag.api:app --reload

# Specific test categories
just test-conformance              # Workflow YAML validity
just test-e2e                      # Full workflow execution with fixtures
pytest tests/test_api.py           # REST API endpoints
```

**Known test state:** All tests pass. `test_context.py::test_build_dynamic_missing_raises` is marked xfail (engine gracefully degrades instead of raising).

---

## Architecture Rules

1. **DCAG is the orchestrator, not the executor.** Never add LLM calls, HTTP requests, or external I/O to engine code. The engine returns typed requests; drivers fulfill them.

2. **Type contracts are sacred.** `types.py` defines the API boundary (`StepRequest`, `StepOutcome`, `ContextBundle`, `Budget`, `Trace`). Changes to types require updating both engine and driver code.

3. **Module responsibilities are strict:**
   - `engine.py` — entry point, `DCAGEngine` + `WorkflowRun`
   - `_walker.py` — DAG traversal, transitions, loops
   - `_context.py` — context assembly (static + dynamic + decisions + cache)
   - `_loaders.py` — YAML parsing into typed dataclasses
   - `_evaluator.py` — transition expression evaluation
   - `_validation.py` — structural output validation (`output_has`)
   - `_trace.py` — JSONL streaming trace
   - `_decisions.py` — cross-run decision persistence
   - `_registry.py` — tool filtering by runtime capabilities
   - `api.py` — FastAPI REST wrapper
   - `drivers/shift.py` — Shift integration driver

4. **Two execution models exist:**
   - **Guardrails model** (creative workflows): context + freestyle generation + validation
   - **Full orchestration** (ops workflows): strict step-by-step with tool gates

---

## Content Conventions

### Workflows (`content/workflows/*.yml`)

- Every workflow needs: `id`, `name`, `persona`, `inputs`, `steps`
- Every workflow must have a matching `.test.yml` fixture for conformance + e2e tests
- Every workflow must have an entry in `manifest.yml` with trigger keywords
- Steps declare `mode: reason|execute` — reason = LLM, execute = script/template/delegate
- Tool gates (`tools:`) restrict which MCP tools a step can use
- Transitions use expression syntax: `output.field == 'value'`, `output.count > 3`, `output.x in ['a','b']`

### Knowledge (`content/knowledge/*.yml`)

- Referenced by steps via `context.static: [knowledge_id]`
- Domain guidance only — no executable logic
- Keep entries factual and current; stale knowledge misleads the LLM

### Personas (`content/personas/*.yml`)

- Define role heuristics, anti-patterns, quality standards
- Currently: `analytics_engineer`, `data_engineer`
- Referenced by workflow-level `persona:` field

---

## Adding a New Workflow

1. Create `content/workflows/{workflow-id}.yml` following existing patterns
2. Create `content/workflows/{workflow-id}.test.yml` with mock step outputs
3. Add entry to `content/workflows/manifest.yml`
4. Add `tests/test_conformance_{workflow_id}.py` — validates YAML structure
5. Add `tests/test_e2e_{workflow_id}.py` — validates full execution with fixtures
6. Run `pytest` — all tests must pass

---

## Adding/Modifying Knowledge

- Add YAML to `content/knowledge/`
- Reference it in workflow steps via `context.static`
- No test file needed, but verify referencing workflows still pass

---

## Integration Points

- **Shift driver** (`drivers/shift.py`): assembles prompts, routes delegates
- **REST API** (`api.py`): FastAPI, step-at-a-time enforcement, HTTP Basic auth
- **dbt MCP + Airflow MCP**: available in Shift as of 2026-03-16
- **Snowflake MCP**: used for metadata introspection in tool gates

---

## What NOT to Do

- Don't add business logic to the engine — it belongs in workflow YAML or knowledge files
- Don't hardcode table/column names in Python — use knowledge files or workflow inputs
- Don't skip conformance or e2e tests for new workflows
- Don't modify `types.py` without checking all consumers (engine, walker, context, drivers, API, tests)
- Don't add runtime dependencies without strong justification — DCAG's minimal footprint is a feature
