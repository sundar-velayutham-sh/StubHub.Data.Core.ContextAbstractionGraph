# DCAG Observability Design Spec

**Date:** 2026-03-19
**Status:** Approved
**Goal:** Add durable workflow trace persistence so Shift can report what happened at each step, stored in Snowflake Postgres, queryable per user/workflow/run.

---

## Current State

DCAG has observability contracts (TraceWriter, ObservabilityEvent, ContextSnapshot) but they write to `/tmp` and are ephemeral. The ShiftDriver has zero trace integration. When Shift drives a workflow via Level 1 (YAML-direct), there is no persistent record of what steps ran, what context was assembled, or what the LLM produced.

## Design Principles

1. **Engine stays pure** — no external I/O in engine code. Persistence lives in the API layer only.
2. **Shift reports** — Shift calls trace endpoints after executing each step. Works for both Level 1 and Level 2.
3. **Graceful degradation** — if Postgres is unavailable, the API still works. Tracing is opt-in.
4. **Adapted pattern** — follows the Astronomer `AgentOpsPersistence` pattern (connection helper, environment-aware schema, JSONB flexibility).

---

## Section 1: Architecture

```
Shift executes step
    |
    v
POST /api/v1/traces/runs          (on workflow start)
POST /api/v1/traces/steps         (after each step)
PATCH /api/v1/traces/runs/{id}    (on workflow end)
    |
    v
API layer -> DCAGPersistence -> Snowflake Postgres
    |
    v
3 tables: workflow_runs, workflow_steps, workflow_events
Schema: dcag_dev / dcag_prod (auto-detected)
```

The engine remains a pure orchestrator. The API layer handles persistence. Shift is the reporter.

---

## Section 2: Database Schema

Three tables in Snowflake Postgres, schema `dcag_dev` / `dcag_prod`:

```sql
-- Workflow run tracking
CREATE TABLE workflow_runs (
    run_id              UUID PRIMARY KEY,
    workflow_id         TEXT NOT NULL,
    persona             TEXT NOT NULL,
    inputs              JSONB DEFAULT '{}',
    status              TEXT NOT NULL DEFAULT 'running',
    triggered_by        TEXT,
    channel_id          TEXT,
    thread_ts           TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    steps_executed      INT,
    total_duration_ms   INT,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Step-level trace (one row per step executed)
CREATE TABLE workflow_steps (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id              UUID NOT NULL REFERENCES workflow_runs(run_id),
    step_id             TEXT NOT NULL,
    step_index          INT NOT NULL,
    mode                TEXT NOT NULL,
    status              TEXT NOT NULL,
    duration_ms         INT,
    instruction         TEXT,
    tools_used          JSONB DEFAULT '[]',
    context_snapshot    JSONB DEFAULT '{}',
    output              JSONB DEFAULT '{}',
    tokens_estimated    INT,
    branch_taken        TEXT,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Lightweight audit events
CREATE TABLE workflow_events (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id              UUID NOT NULL REFERENCES workflow_runs(run_id),
    event_type          TEXT NOT NULL,
    detail              JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_workflow_runs_status ON workflow_runs(status);
CREATE INDEX idx_workflow_runs_workflow ON workflow_runs(workflow_id);
CREATE INDEX idx_workflow_runs_triggered ON workflow_runs(triggered_by);
CREATE INDEX idx_workflow_steps_run ON workflow_steps(run_id);
CREATE INDEX idx_workflow_events_run ON workflow_events(run_id);
```

### Data Management

- `workflow_steps.output` — truncated to 10KB max to prevent bloat. Full output lives in Shift's Slack thread.
- `workflow_steps.instruction` — first 500 chars of the assembled prompt.
- No auto-purge initially. At ~1KB per step row and ~10 steps per workflow, 1000 runs is ~10MB.

### Status Values

- `workflow_runs.status`: `running`, `completed`, `failed`
- `workflow_steps.status`: `success`, `failure`, `skipped`
- `workflow_events.event_type`: `run_start`, `step_complete`, `branch_decision`, `validation_result`, `budget_warning`, `run_complete`

---

## Section 3: Persistence Layer

### DCAGPersistence (`src/dcag/persistence.py`)

Adapted from Astronomer `AgentOpsPersistence` pattern:

```python
class DCAGPersistence:
    """Write workflow traces to Snowflake Postgres."""

    def __init__(self, connection):
        self._conn = connection

    # Write methods
    def write_run_start(self, run_id, workflow_id, persona, inputs,
                        triggered_by, channel_id, thread_ts) -> None
    def write_run_complete(self, run_id, status, steps_executed,
                           total_duration_ms, error_message=None) -> None
    def write_step(self, run_id, step_id, step_index, mode, status,
                   duration_ms, instruction, tools_used, context_snapshot,
                   output, tokens_estimated, branch_taken=None,
                   error_message=None) -> None
    def write_event(self, run_id, event_type, detail) -> None

    # Read methods
    def get_run(self, run_id) -> dict | None
    def get_steps_by_run(self, run_id) -> list[dict]
    def get_recent_runs(self, workflow_id=None, triggered_by=None,
                        limit=20) -> list[dict]
```

All write methods use parameterized queries (no SQL injection). JSONB fields serialized via `json.dumps()`. Output truncated to 10KB in `write_step()`.

### Connection Helper (`src/dcag/persistence_connection.py`)

Adapted from Astronomer `agents/utils/postgresql/connection.py`:

```python
def create_dcag_db() -> connection | None:
    """Create Postgres connection from DCAG_POSTGRES_* env vars.
    Returns None if env vars are not set.
    """
```

**Environment variables:**
- `DCAG_POSTGRES_HOST` — Snowflake Postgres host
- `DCAG_POSTGRES_PORT` — default 5432
- `DCAG_POSTGRES_DATABASE` — default `postgres`
- `DCAG_POSTGRES_USER`
- `DCAG_POSTGRES_PASSWORD`
- `DCAG_POSTGRES_SSLMODE` — default `require`

**Schema auto-detection:**
- `DCAG_POSTGRES_SCHEMA` env var overrides everything
- `ASTRONOMER_DEPLOYMENT_ID` present → `dcag_prod`
- Otherwise → `dcag_dev`

**Connection features:**
- `gssencmode=disable` for Snowflake compatibility
- Auto-commit enabled
- Schema existence validated on connect
- `_ensure_connection()` for auto-reconnect on timeout

---

## Section 4: REST Endpoints

Three new endpoints in `api.py`. No auth required (write-only observability data).

### POST /api/v1/traces/runs

Shift reports workflow start.

```json
{
    "run_id": "550e8400-e29b-41d4-a716-446655440000",
    "workflow_id": "table-optimizer",
    "persona": "data_engineer",
    "inputs": {"table_name": "DW.RPT.TRANSACTION"},
    "triggered_by": "sundar.velayutham",
    "channel_id": "C0123ABC",
    "thread_ts": "1234567890.123456"
}
```

Response: `201 Created` with `{"status": "recorded"}`.

### POST /api/v1/traces/steps

Shift reports step completion.

```json
{
    "run_id": "550e8400-e29b-41d4-a716-446655440000",
    "step_id": "identify_table",
    "step_index": 0,
    "mode": "reason",
    "status": "success",
    "duration_ms": 4500,
    "instruction": "Find table in INFORMATION_SCHEMA...",
    "tools_used": ["snowflake_mcp.execute_query"],
    "context_snapshot": {
        "persona": "data_engineer",
        "knowledge": ["snowflake_environment", "optimization_rules"],
        "dynamic_refs": [],
        "tokens_estimated": 3200
    },
    "output": {"table_fqn": "DW.RPT.TRANSACTION", "row_count": 244000000},
    "tokens_estimated": 3200,
    "branch_taken": null
}
```

Response: `201 Created` with `{"status": "recorded"}`.

### PATCH /api/v1/traces/runs/{run_id}

Shift reports workflow end.

```json
{
    "status": "completed",
    "steps_executed": 9,
    "total_duration_ms": 45000,
    "error_message": null
}
```

Response: `200 OK` with `{"status": "updated"}`.

### Graceful Degradation

If persistence is not configured (env vars missing) or Postgres is unreachable:
- Endpoints return `202 Accepted` with `{"status": "skipped", "reason": "persistence not configured"}`
- Warning logged, no exception raised
- Shift continues normally — tracing failure never blocks workflow execution

---

## Section 5: File Structure & Dependencies

### New Files

```
src/dcag/persistence.py              # DCAGPersistence class
src/dcag/persistence_connection.py   # Postgres connection helper
sql/schema.sql                       # DDL for 3 tables + indexes
```

### Modified Files

```
src/dcag/api.py                      # Add 3 trace endpoints + persistence init
pyproject.toml                       # Add psycopg2-binary to optional deps
.env.example                         # Add DCAG_POSTGRES_* vars
```

### Dependencies

Add `psycopg2-binary>=2.9` to `[project.optional-dependencies] dev`. Stays optional — engine doesn't need it.

### Environment Variables (.env.example additions)

```
# DCAG Trace Persistence (Snowflake Postgres)
DCAG_POSTGRES_HOST=
DCAG_POSTGRES_PORT=5432
DCAG_POSTGRES_DATABASE=postgres
DCAG_POSTGRES_USER=
DCAG_POSTGRES_PASSWORD=
DCAG_POSTGRES_SSLMODE=require
DCAG_POSTGRES_SCHEMA=            # auto-detected if empty (dcag_dev / dcag_prod)
```

---

## Implementation Order

| Step | Deliverable |
|------|-------------|
| 1 | SQL schema file (`sql/schema.sql`) |
| 2 | Connection helper (`persistence_connection.py`) |
| 3 | Persistence class (`persistence.py`) + unit tests |
| 4 | Trace endpoints in `api.py` + API tests |
| 5 | Update `.env.example` and `pyproject.toml` |
| 6 | Update README and CLAUDE.md with observability section |

Steps 1-2 are independent. Step 3 depends on 2. Step 4 depends on 3. Steps 5-6 after all code is done.

---

## Out of Scope

- Dashboards or visualization (query Postgres directly for now)
- Auto-purge / retention policy (table growth is manageable)
- Trace endpoints auth (can add separate API key later if needed)
- Level 2 auto-tracing (engine writes trace on record_result — future enhancement)
- ObservabilityEvent integration (existing events stay as-is, not wired to Postgres yet)
