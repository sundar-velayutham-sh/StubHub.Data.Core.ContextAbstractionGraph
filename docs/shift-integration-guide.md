# Shift Integration Guide for DCAG

## Overview

DCAG (Data Core Abstraction Graph) is a **headless workflow engine**. It defines workflows as YAML, walks steps in order, and emits typed requests for each step. It does NOT make LLM calls, create PRs, or post to Slack. That is the driver's job.

**Shift** is one possible driver. It receives DCAG's typed requests, calls Anthropic's API for reasoning steps, posts plans to Slack for approval, and creates GitHub PRs for implementation steps.

```
User (Slack) -> Shift -> DCAG Engine -> Shift Driver -> Anthropic API
                                     -> Shift Driver -> Slack (approval)
                                     -> Shift Driver -> GitHub (PR creation)
```

## Installation

```bash
git clone https://github.com/stubhub/StubHub.Data.Core.ContextAbstractionGraph.git
cd StubHub.Data.Core.ContextAbstractionGraph
pip install -e ".[dev]"
```

## The Driver Pattern

DCAG uses a **pull model**. The driver controls the loop:

```python
from dcag import DCAGEngine
from dcag.drivers.shift import ShiftDriver
from dcag.types import ReasonRequest, DelegateRequest, StepSuccess, StepFailure

engine = DCAGEngine(content_dir="content")
driver = ShiftDriver()

# 1. Start a workflow run
run = engine.start("table-optimizer", {"table_name": "TRANSACTION"})

# 2. Pull steps until complete
while run.status == "running":
    request = run.next_step()  # Returns typed request or None
    if request is None:
        break

    # 3. Handle each request type (see below)
    ...

    # 4. Record the result to advance the walker
    run.record_result(request.step_id, StepSuccess(output={...}))

# 5. Check final status
print(run.status)  # "completed" | "failed" | "paused"
```

### WorkflowRun API

| Method | Returns | Description |
|--------|---------|-------------|
| `run.next_step()` | `ReasonRequest \| DelegateRequest \| ExecuteScriptRequest \| None` | Get the next step to execute |
| `run.record_result(step_id, outcome)` | `None` | Record outcome and advance the walker |
| `run.status` | `str` | Current status: `"running"`, `"completed"`, `"failed"`, `"paused"` |
| `run.run_id` | `str` | Unique run identifier (e.g., `dcag-a1b2c3d4`) |
| `run.get_trace()` | `dict` | Full execution trace with timing, outputs, errors |

### Outcome Types

| Type | When to use | Fields |
|------|------------|--------|
| `StepSuccess(output={...})` | Step completed successfully | `output`: dict or str, `artifacts`: list[str] |
| `StepFailure(error="...")` | Step failed | `error`: str, `retryable`: bool, `retry_count`: int |
| `StepSkipped(reason="...")` | Step should be skipped | `reason`: str |

## Handling Each Request Type

### ReasonRequest (LLM reasoning)

The most common request type. The driver must:
1. Assemble a prompt using `ShiftDriver.assemble_prompt()`
2. Call the Anthropic API
3. Parse the JSON response
4. Record the result

```python
if isinstance(request, ReasonRequest):
    # 1. Assemble prompt (tool-gate-first format)
    prompt = driver.assemble_prompt(request)

    # 2. Call Anthropic API
    response = anthropic_client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=request.budget.max_tokens,
        system="You are a Snowflake data engineering expert. Respond with valid JSON.",
        messages=[{"role": "user", "content": prompt}],
    )

    # 3. Parse JSON from response
    output = json.loads(response.content[0].text)

    # 4. Record result
    run.record_result(request.step_id, StepSuccess(output=output))
```

The assembled prompt has these sections in order:
- `[TOOLS -- ONLY USE THESE]` — tool gate (what MCP tools the LLM may call)
- `[PERSONA]` — who the LLM is, domain knowledge, heuristics, anti-patterns
- `[TASK]` — the step instruction
- `[CONTEXT]` — static knowledge + prior step outputs
- `[OUTPUT]` — expected JSON schema + quality criteria
- `[BUDGET]` — max tool calls allowed

### DelegateRequest (driver-native capability)

Delegate steps need the driver's own capabilities (Slack posting, PR creation). Use `ShiftDriver.route_delegate()` to get routing info:

```python
if isinstance(request, DelegateRequest):
    action = driver.route_delegate(request)
    # action = {
    #   "capability": "show_plan" | "create_pr",
    #   "requires_approval": True,
    #   "inputs": {...},
    #   "step_id": "show_recommendations"
    # }

    if action["capability"] == "show_plan":
        # Post plan to Slack thread, wait for user approval
        slack_response = post_plan_to_slack(action["inputs"])
        approved = wait_for_approval(slack_response)
        run.record_result(request.step_id, StepSuccess(output={"approved": approved}))

    elif action["capability"] == "create_pr":
        # Create GitHub PR via Shift's PR tooling
        pr_url = create_github_pr(action["inputs"])
        run.record_result(request.step_id, StepSuccess(output={"pr_url": pr_url}))
```

Supported delegate capabilities:
- **`shift.show_plan`** — Post optimization plan to Slack thread for user review. Requires approval before proceeding.
- **`shift.create_pr`** — Create a GitHub PR with the implementation SQL or dbt config changes.

### ExecuteScriptRequest (shell commands)

For steps that run scripts or shell commands:

```python
if isinstance(request, ExecuteScriptRequest):
    result = subprocess.run(request.script, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        run.record_result(request.step_id, StepSuccess(output={"stdout": result.stdout}))
    else:
        run.record_result(
            request.step_id,
            StepFailure(error=result.stderr, retryable=request.fallback_on_failure is not None),
        )
```

## The table-optimizer Workflow

The `table-optimizer` workflow analyzes a Snowflake table and recommends optimization strategies. It has **9 steps**:

| # | Step ID | Mode | Description |
|---|---------|------|-------------|
| 0 | `identify_table` | reason | Find table in Snowflake, get FQN and storage metrics |
| 1 | `detect_load_frequency` | reason | Query DW.DATAOPS_DBT for load schedule (daily/hourly/sub-hourly) |
| 2 | `analyze_query_patterns` | reason | Analyze QUERY_HISTORY for WHERE clause patterns |
| 3 | `assess_clustering` | reason | Evaluate current clustering vs optimal keys |
| 4 | `check_partitioning` | reason | Analyze micro-partition stats and pruning efficiency |
| 5 | `analyze_materialization` | reason | Check if dbt-managed, evaluate materialization strategy |
| 6 | `show_recommendations` | delegate | Post plan to Slack for user approval (shift.show_plan) |
| 7 | `generate_report` | reason | Produce final optimization report with implementation SQL |
| 8 | `apply_changes` | delegate | Create GitHub PR with changes (shift.create_pr) |

### Key detail: detect_load_frequency

Step 1 queries `DW.DATAOPS_DBT.DBT_MODELS` and `DW.DATAOPS_DBT.DBT_RUN_RESULTS` to determine how often the table is loaded. This is critical because:

- **SUB_HOURLY / HOURLY** tables should **SKIP** clustering (micro-partition churn wastes re-clustering credits)
- **DAILY / DAILY_OR_LESS** tables are candidates for clustering optimization

The step uses two signals: explicit dbt `TAGS` (authoritative) and computed `avg_runs_per_day` (fallback).

### Strategy outcomes

The final `generate_report` step produces one of these strategies:

| Strategy | When |
|----------|------|
| `SKIP` | Hourly/sub-hourly loads make clustering counterproductive |
| `ORDER_BY` | Table is small enough for ORDER BY optimization |
| `CLUSTER_BY` | Table benefits from clustering on identified filter columns |
| `CLUSTER_BY_AND_SOS` | Clustering + Search Optimization Service for high-cardinality lookups |
| `MONITOR` | Current state is acceptable, monitor for changes |

## Example: Running table-optimizer End-to-End

```python
import json
import anthropic
from dcag import DCAGEngine
from dcag.drivers.shift import ShiftDriver
from dcag.types import ReasonRequest, DelegateRequest, StepSuccess

engine = DCAGEngine(content_dir="content")
driver = ShiftDriver()
client = anthropic.Anthropic()  # uses ANTHROPIC_API_KEY

run = engine.start("table-optimizer", {
    "table_name": "TRANSACTION",
    "database": "DW",
    "schema": "RPT",
})

while run.status == "running":
    request = run.next_step()
    if request is None:
        break

    if isinstance(request, ReasonRequest):
        prompt = driver.assemble_prompt(request)

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4096,
            system="You are a Snowflake data engineering expert. Respond with valid JSON.",
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text
        # Handle markdown code blocks in response
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        output = json.loads(text.strip())

        run.record_result(request.step_id, StepSuccess(output=output))

    elif isinstance(request, DelegateRequest):
        action = driver.route_delegate(request)

        if action["capability"] == "show_plan":
            # In production: post to Slack, wait for approval
            run.record_result(request.step_id, StepSuccess(output={"approved": True}))
        elif action["capability"] == "create_pr":
            # In production: create GitHub PR
            run.record_result(
                request.step_id,
                StepSuccess(output={"pr_url": "https://github.com/stubhub/repo/pull/123"}),
            )

# Get execution trace
trace = run.get_trace()
print(f"Status: {trace['status']}")
print(f"Steps: {len(trace['steps'])}")
```

## Live Testing

Run the live integration tests to verify the full Shift driver loop with real Anthropic API calls:

```bash
# Set your API key
export ANTHROPIC_API_KEY=sk-...

# Run live tests
DCAG_LIVE_TEST=1 pytest tests/integration/ -v

# Or use the convenience script
./scripts/run_live_test.sh
```

The live tests are **skipped by default** in normal test runs. They only execute when:
- `DCAG_LIVE_TEST=1` environment variable is set, OR
- `--run-live` flag is passed to pytest

### What the live tests validate

1. **test_table_optimizer_live** — Runs the full 9-step workflow with real Anthropic API calls. Verifies the LLM returns valid JSON matching the expected schema for each step.

2. **test_driver_prompt_quality** — Verifies that `ShiftDriver.assemble_prompt()` produces well-structured prompts with all required sections (TOOLS, PERSONA, TASK, CONTEXT, OUTPUT, BUDGET).

## Observability

The `ShiftDriver` provides observability event emitters for integration with Shift's event bus:

```python
# Emit events at each stage
driver.emit_step_started(request.step_id, "reason")
driver.emit_context_assembled(request)
driver.emit_tool_resolved(request.step_id, requested=["snowflake_mcp.execute_query"], available=[...])
driver.emit_result_recorded(request.step_id, status="completed", duration_ms=1234)
```

Each event includes a UTC timestamp and can be forwarded to your observability pipeline.

## Trace Format

Every `WorkflowRun` records a full execution trace:

```python
trace = run.get_trace()
# {
#   "run_id": "dcag-a1b2c3d4",
#   "workflow_id": "table-optimizer",
#   "status": "completed",
#   "inputs": {"table_name": "TRANSACTION"},
#   "started_at": "2026-03-12T...",
#   "completed_at": "2026-03-12T...",
#   "steps": [
#     {"step_id": "identify_table", "mode": "reason", "status": "completed", "duration_ms": 2340, ...},
#     ...
#   ],
#   "config_snapshot": "sha256:abc123..."
# }
```

Use traces for debugging, audit trails, and performance monitoring.
