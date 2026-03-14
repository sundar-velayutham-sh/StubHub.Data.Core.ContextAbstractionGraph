# Similar Incident Search — Design Spec

**Date:** 2026-03-13
**Status:** Approved
**Workflow:** `triage-ae-alert`

## Summary

Add two new steps to the triage-ae-alert workflow that search Slack threads in `#ae-alerts` and `#dw-alerts` for similar past incidents before classification. When a match is found, downstream steps receive structured context about what happened last time — root cause, resolution, who fixed it, and how long it took.

## Motivation

The most valuable triage context is often "someone already investigated this exact thing last week." Today, the on-call person has to manually search Slack. This feature automates that search and feeds the findings into the classification, diagnosis, and reporting steps.

Example: `int_user_lifetime_metrics` fails with a duplicate_row error. The search finds Cole Romano's thread from 5 days ago where he identified stale `anon_to_resolved_user` mapping (3.5-day refresh gap) and resolved it by rerunning via `on_demand_dag` from the affected date. The triage report surfaces this before any human looks at it.

## Design

### Placement

Inserted after `check_prior_remediation` (current step 2), before `check_cascade` (current step 3):

```
parse_alert
  → check_failure_history
  → check_prior_remediation
  → search_similar_incidents    ← NEW
  → analyze_similar_incidents   ← NEW
  → check_cascade
  → get_model_context
  → classify_alert
  → diagnose_* (4 branches)
  → determine_resolution
  → generate_triage_report
  → post_to_thread
```

> Steps use string IDs, not numeric indices. The workflow engine resolves order from the YAML list position.

### Step: `search_similar_incidents`

**Mode:** `reason`

**Fallback on failure:** If the Slack MCP is unavailable (token expired, rate limited, service down), this step gracefully degrades: `matches_found: 0`, empty `candidate_threads`, `match_strategy: "none"`. The workflow continues normally — this feature is purely additive.

**Tools:**
- `slack_mcp.get_channel_history` — pull messages from both channels (maps to `mcp__slack__slack_get_channel_history`)

> **Note:** The Slack MCP does not expose a workspace-wide search tool. Both passes use `get_channel_history` with the LLM scanning messages for matches. To keep API calls manageable, each channel pull is limited to the most recent 200 messages (2 paginated calls of 100).

**Context:**
- Static: `on_call_conventions`
- Dynamic:
  - `parse_alert` → `model_name`, `error_message`
  - `check_failure_history` → `pattern`

**Search strategy (two-pass, both via `get_channel_history`):**

1. **Pass 1 — Same model, any error:** Pull history from `#ae-alerts` (C0590MFQN1W) and `#dw-alerts` (C040SRYF9HS). LLM scans messages for the model name (e.g., `int_user_lifetime_metrics`) in the last 30 days. Identify messages that are alert threads (have replies) mentioning the model.
2. **Pass 2 — Same error pattern, any model:** Only if Pass 1 returns < 2 matches. LLM re-scans the already-fetched channel history for error classification keywords (e.g., `duplicate_row`, `timeout`, `invalid identifier`). No additional API calls needed — reuses the history from Pass 1.

Cap at 5 candidate threads max.

**Output schema:**

```yaml
type: object
required: [matches_found, candidate_threads]
properties:
  matches_found:
    type: integer
  match_strategy:
    type: string
    enum: [same_model, same_error_pattern, both, none]
  candidate_threads:
    type: array
    items:
      type: object
      required: [channel_id, thread_ts, match_type, preview]
      properties:
        channel_id:
          type: string
        thread_ts:
          type: string
        match_type:
          type: string  # "same_model" or "same_error_pattern"
        preview:
          type: string  # first message snippet
```

**Budget:** `max_llm_turns: 6`, `max_tokens: 10000`

> Budget increased to 6 turns to allow for pagination across 2 channels (2 pages x 2 channels = 4 API calls minimum) plus reasoning.

### Step: `analyze_similar_incidents`

**Mode:** `reason`

**Tools:**
- `slack_mcp.get_thread_replies` — read full thread conversations (maps to `mcp__slack__slack_get_thread_replies`)

**Context:**
- Static: `troubleshooting_patterns`
- Dynamic:
  - `parse_alert` → `model_name`, `error_message`
  - `search_similar_incidents` → `candidate_threads`

**What it does:**
1. Takes the candidate threads from `search_similar_incidents`
2. Reads full thread replies for each
3. Extracts structured findings from each thread
4. Ranks by relevance: same model + same error > same model + different error > different model + same error

**Output schema:**

```yaml
type: object
required: [similar_incidents_found, incidents, summary]
properties:
  similar_incidents_found:
    type: boolean
  incidents:
    type: array
    items:
      type: object
      required: [model_name, error_type, root_cause, resolution, relevance]
      properties:
        model_name:
          type: string
        error_type:
          type: string
        root_cause:
          type: string
        resolution:
          type: string
        resolved_by:
          type: string   # optional — use "unknown" if not extractable from thread
        time_to_resolve_hours:
          type: number   # optional — use null if resolution timestamp not clear
        relevance:
          type: string
          enum: [high, medium, low]
  summary:
    type: string  # one-paragraph synthesis for downstream steps
```

**Budget:** `max_llm_turns: 5`, `max_tokens: 12000`

**When no matches are found:** `similar_incidents_found: false`, empty `incidents` array, summary says "No similar incidents found in the last 30 days." Downstream steps proceed normally.

**Short-circuit:** When `candidate_threads` from `search_similar_incidents` is empty, skip thread fetching entirely and return the no-match output immediately — no wasted LLM turns.

### Downstream Wiring

Seven existing steps get new dynamic context:

**`classify_alert`:**
```yaml
dynamic:
  - from: analyze_similar_incidents
    select: [similar_incidents_found, summary]
```

**`diagnose_code_error`, `diagnose_data_issue`, `diagnose_infrastructure`, `diagnose_known_issue` — each gets:**
```yaml
dynamic:
  - from: analyze_similar_incidents
    select: [incidents, summary]
```

**`determine_resolution`:**
```yaml
dynamic:
  - from: analyze_similar_incidents
    select: [similar_incidents_found, incidents, summary]
```

> Similar incident context directly informs resolution — knowing "last time Cole fixed this by rerunning from the affected date" determines whether to recommend `fix_directly` vs. `escalate_to_owner`.

**`generate_triage_report`:**
```yaml
dynamic:
  - from: analyze_similar_incidents
```

### Report Format Change

The triage report gains a "Prior Similar Incidents" section:

```
## Prior Similar Incidents
Found {N} similar incident(s) in the last 30 days:
- {model_name} ({date}): {root_cause} → resolved by {resolved_by} via {resolution}
```

Or: "No similar incidents found in the last 30 days."

## Channels Searched

| Channel | ID | Purpose |
|---------|-----|---------|
| `#ae-alerts` | C0590MFQN1W | Primary AE on-call alerts |
| `#dw-alerts` | C040SRYF9HS | Ingestion/infrastructure alerts |

## Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Search window | 30 days | Catches monthly patterns without too much noise |
| Max candidate threads | 5 | Keeps analysis step focused and token-efficient |
| Channels | 2 (`#ae-alerts`, `#dw-alerts`) | Covers AE and infra alert surfaces |
| Search passes | 2 (same model first, then same error pattern) | Prioritizes exact matches, broadens only when needed |

## Files Changed

| File | Change |
|------|--------|
| `content/workflows/triage-ae-alert.yml` | Add 2 new steps, update dynamic context on 7 existing steps |
| `content/workflows/triage-ae-alert.test.yml` | Add 2 new step entries to conformance spec, update `dynamic_refs_from` on 7 existing steps |
| `tests/test_conformance_triage_ae_alert.py` | Update `code_error_path` list and `step_outputs` dict with 2 new steps |
| `tests/test_e2e_triage_ae_alert.py` | Update all 4 step-order constants (`CODE_ERROR_STEPS`, `DATA_ISSUE_STEPS`, `INFRASTRUCTURE_STEPS`, `KNOWN_ISSUE_STEPS`) to include 2 new steps; add new E2E test for similar-incident path |
| `tests/cassettes/triage-ae-alert-code-error/` | Add 2 cassettes: `search_similar_incidents.json`, `analyze_similar_incidents.json` |
| `tests/cassettes/triage-ae-alert-data-issue/` | Add 2 cassettes (same) |
| `tests/cassettes/triage-ae-alert-infrastructure/` | Add 2 cassettes (same) |
| `tests/cassettes/triage-ae-alert-known-issue/` | Add 2 cassettes (same) |

> Total: 8 new cassette files across the 4 existing branch paths, plus updates to all 4 test classes.

## Non-Goals

- No persistent incident index or database — search is live against Slack every time
- No embedding-based similarity — keyword search via Slack MCP is sufficient for v1
- No changes to the branching logic — this is purely additive context
- No new knowledge files — existing `on_call_conventions` and `troubleshooting_patterns` are sufficient
