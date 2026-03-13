# triage-ae-alert — AE On-Call Triage Workflow Design

> **Status**: Design approved
> **Date**: 2026-03-13
> **Author**: Sundar Velayutham + Claude
> **Scope**: Single DCAG workflow with conditional branching for automated AE alert triage
> **Research basis**: 12 real #ae-alerts Slack threads, 30 days Snowflake operational data (650 errors, 180 models), industry AI on-call research

---

## 1. Problem Statement

StubHub's AE on-call rotation handles ~50 alerts per week in #ae-alerts. Each alert requires manual investigation: reading error logs, querying Snowflake, reading model code, tracing lineage, cross-referencing threads, and posting findings. The same diagnostic patterns repeat across alerts.

Analysis of 12 real investigation threads revealed 7 distinct resolution patterns and 18 specific AE actions. 14 of 18 actions are automatable with existing MCP tools (Snowflake, dbt, GitHub). The 4 that aren't (disable DAG, backfill, rerun, terminate job) require Airflow write access — deferred to Phase 2.

### Current State (manual triage)
- Average triage time: 30-90 minutes per alert
- AE on-call: weekly rotation, all domains
- Alert source: incident.io → #ae-alerts Slack channel
- Tools used manually: Snowflake UI, Airflow UI, GitHub, dbt docs

### Target State (DCAG-assisted triage)
- Triage time: 2-5 minutes (DCAG investigation) + AE review
- Output: Concise thread summary + detailed investigation file
- AE role: review findings, execute suggested actions (rerun, tag owner, merge PR)
- For actions requiring Airflow: provide exact links and commands

---

## 2. Evidence Base — 7 AE On-Call Patterns

From 12 real #ae-alerts threads:

### Pattern A: "Diagnose, Fix, PR, Verify" (~30% of investigated alerts)
AE investigates → writes diagnostic SQL → identifies root cause → creates PR → merges → reruns → confirms.
Threads: exp_web_vitals (PR #16650), strategic_finance (PR #16498), search_fact, home_content_fact.

### Pattern B: "Trace Upstream → Escalate" (~20%)
AE traces error to upstream data issue → tags owning team → waits.
Threads: gpm_repeat_buyer_dim (backfill caused dupes → tagged DE).

### Pattern C: "Transient / Self-Resolving → Acknowledge" (~25%)
Snowflake internal error or timing conflict → resolves on retry → AE notes it.
Threads: TASK_EXEC_LOAD_SELLER_EVENT_DAY_LISTING_AGG ("same internal error, resolved").

### Pattern D: "Discuss Alert Routing / Suppress" (~10%)
Alert is real but AE questions whether it belongs in critical path.
Threads: severe_tests__hourly ("exclude from critical AE alerting"), pos_hourly_tests (incident severity discussion).

### Pattern E: "Infrastructure Contention → Find Culprit" (~5%)
Non-dbt job blocks warehouse, causing cascade failures.
Threads: Metaflow stuck job → terminated → DE doubled warehouse clusters.

### Pattern F: "Defer to Next Day" (~10% during off-hours)
After-hours alert → AE disables test/DAG → investigates next morning.
Threads: core-data-dbt_test ("turned test off"), transform_optimal ("turned it off again").

### Pattern G: "Pipeline May Be Deprecated" (rare)
Investigation reveals pipeline has low/zero usage.
Threads: marketshare Hex ("rpt marketshare pipeline which I don't think is used anymore").

---

## 3. Workflow Definition

**ID**: triage-ae-alert
**Persona**: analytics_engineer (on-call mode)
**Trigger**: Raw Slack alert text from incident.io in #ae-alerts
**Inputs**:
- alert_text (required): Raw Slack message text
- channel_id (optional): Slack channel for thread reply
- thread_ts (optional): Thread timestamp for reply

**Output**: Two artifacts:
1. Concise thread summary (posted to Slack)
2. Detailed investigation file (attached to thread)

---

## 4. Steps (10 steps with branching)

### Step 0: parse_alert (REASON)
**What**: Extract DAG name, task name, model name, priority from incident.io alert format. Query DBT_RUN_RESULTS for the actual error message and metadata.
**Tools**: snowflake_mcp.execute_query
**Context static**: [alert_classification, on_call_conventions]
**Output**: model_name, dag_name, task_name, error_message, error_code, priority, alert_url

**Key queries**:
```sql
SELECT NAME, STATUS, MESSAGE, CREATED_AT, EXECUTION_TIME
FROM DW.DATAOPS_DBT.DBT_RUN_RESULTS
WHERE NAME = '{model_name}' AND STATUS = 'error'
ORDER BY CREATED_AT DESC LIMIT 1
```

### Step 1: check_failure_history (REASON)
**What**: Is this recurring, intermittent, or new? Query last 7 days of same model.
**Tools**: snowflake_mcp.execute_query
**Context dynamic**: from parse_alert select model_name
**Output**: is_recurring, failure_count_7d, first_failure, last_success, pattern (new/recurring/intermittent)

**Key queries**:
```sql
SELECT STATUS, CREATED_AT, EXECUTION_TIME, MESSAGE
FROM DW.DATAOPS_DBT.DBT_RUN_RESULTS
WHERE NAME = '{model_name}' AND CREATED_AT >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY CREATED_AT DESC
```

### Step 2: check_cascade (REASON)
**What**: Are other models failing in the same DAG run? Find the root failure.
**Tools**: snowflake_mcp.execute_query
**Context dynamic**: from parse_alert select [dag_name, model_name, error_message]
**Output**: is_cascade, cascade_count, root_model, affected_models list

**Key queries**:
```sql
-- Find other failures in a similar time window (same DAG run)
SELECT NAME, STATUS, MESSAGE, CREATED_AT
FROM DW.DATAOPS_DBT.DBT_RUN_RESULTS
WHERE STATUS = 'error'
  AND CREATED_AT BETWEEN DATEADD(hour, -1, '{alert_time}') AND DATEADD(hour, 1, '{alert_time}')
ORDER BY CREATED_AT ASC
```

### Step 3: get_model_context (REASON)
**What**: Read model SQL, get owner/tags/schedule, get lineage, check recent commits.
**Tools**: snowflake_mcp.execute_query, dbt_mcp.get_node_details_dev, dbt_mcp.get_lineage_dev, github_cli.read_file, github_cli.search_code
**Context static**: [dbt_project_structure, troubleshooting_patterns]
**Context dynamic**: from parse_alert select model_name
**Output**: model_sql, owner, tags, schedule, materialization, upstream_models, downstream_models, recent_commits, model_path

**Key queries**:
```sql
SELECT NAME, MATERIALIZATION, SCHEMA_NAME, TAGS,
  JSON_EXTRACT_PATH_TEXT(META, 'owner') as owner,
  JSON_EXTRACT_PATH_TEXT(META, 'cost_center') as cost_center,
  JSON_EXTRACT_PATH_TEXT(META, 'channel') as alert_channel
FROM DW.DATAOPS_DBT.DBT_MODELS WHERE NAME = '{model_name}'
```

### Step 4: classify_alert (REASON — BRANCHING STEP)
**What**: Classify using all prior context. No tools — pure reasoning.
**Tools**: (none)
**Context static**: [alert_classification, troubleshooting_patterns]
**Context dynamic**: from parse_alert, from check_failure_history, from check_cascade, from get_model_context

**Classification taxonomy**:

| Classification | Trigger Pattern |
|---------------|----------------|
| invalid_identifier | "invalid identifier" in error |
| permission_error | "Insufficient privileges" or "access control" |
| compilation_error | Other "SQL compilation error" |
| duplicate_row | "Duplicate row" or "DML action" |
| test_failure | Test name in model, "Got N results" |
| upstream_data | Error traces to upstream, backfill suspected |
| timeout | "exceeded", "timeout", long execution time |
| internal_error | "internal error", "Processing aborted" |
| warehouse_contention | Multiple DAGs failing, queue depth high |
| recurring_transient | Same error 3+ times, self-resolves |
| flaky_test | Test fails intermittently |
| deprecated_pipeline | Zero queries in QUERY_HISTORY last 30d |

**Transitions**:
```yaml
transitions:
  - when: "output.classification in ['invalid_identifier', 'permission_error', 'compilation_error']"
    goto: diagnose_code_error
  - when: "output.classification in ['duplicate_row', 'test_failure', 'upstream_data']"
    goto: diagnose_data_issue
  - when: "output.classification in ['timeout', 'internal_error', 'warehouse_contention']"
    goto: diagnose_infrastructure
  - when: "output.classification in ['recurring_transient', 'flaky_test', 'deprecated_pipeline']"
    goto: diagnose_known_issue
  - default: diagnose_code_error
```

### Step 5a: diagnose_code_error (REASON — branch)
**What**: For code-level errors — diff columns, check grants, identify exact line, check recent commits.
**Tools**: snowflake_mcp.execute_query, snowflake_mcp.describe_table, github_cli.read_file
**Context static**: [troubleshooting_patterns, naming_conventions]
**Context dynamic**: from parse_alert, from get_model_context select [model_sql, model_path, recent_commits]

**Key queries**:
```sql
-- For invalid_identifier: check if column exists in source
SELECT COLUMN_NAME FROM {database}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{source_table}' AND COLUMN_NAME ILIKE '%{identifier}%'

-- For permission_error: check grants
SHOW GRANTS ON SCHEMA {schema_name}

-- For compilation_error: check column types
DESCRIBE TABLE {source_table}
```
**Output**: root_cause, affected_line, proposed_fix, fix_type (rename/grant/schema_change), diagnostic_queries_run

**Transition**: `default: determine_resolution`

### Step 5b: diagnose_data_issue (REASON — branch)
**What**: For data quality issues — write and run diagnostic SQL, trace to upstream source, find the bad data.
**Tools**: snowflake_mcp.execute_query, dbt_mcp.get_lineage_dev
**Context static**: [troubleshooting_patterns, data_quality_checks]
**Context dynamic**: from parse_alert, from get_model_context select [model_sql, upstream_models]

**Key queries**:
```sql
-- For duplicate_row: find the duplicates
SELECT {unique_key}, COUNT(*) as cnt
FROM {table} GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 10

-- For test_failure: run the compiled test SQL
-- (extract from DBT_RUN_RESULTS.MESSAGE which shows the compiled SQL path)

-- For upstream_data: check upstream for recent changes
SELECT MAX(CREATED_AT), COUNT(*) FROM {upstream_table}
WHERE CREATED_AT >= DATEADD(day, -1, CURRENT_TIMESTAMP())
```
**Output**: root_cause, bad_data_sample, upstream_source, diagnostic_queries_run, affected_row_count

**Transition**: `default: determine_resolution`

### Step 5c: diagnose_infrastructure (REASON — branch)
**What**: For resource issues — check warehouse queues, find long-running queries, identify blocking jobs.
**Tools**: snowflake_mcp.execute_query
**Context static**: [troubleshooting_patterns]
**Context dynamic**: from parse_alert, from check_cascade

**Key queries**:
```sql
-- For timeout: check execution history
SELECT QUERY_TYPE, TOTAL_ELAPSED_TIME/1000 as seconds, WAREHOUSE_NAME, QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = '{warehouse}'
ORDER BY TOTAL_ELAPSED_TIME DESC LIMIT 10

-- For warehouse_contention: check queue depth
SELECT WAREHOUSE_NAME, COUNT(*) as queued
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
  AND EXECUTION_STATUS = 'RUNNING'
GROUP BY WAREHOUSE_NAME
```
**Output**: root_cause, blocking_queries, warehouse_utilization, is_transient, diagnostic_queries_run

**Transition**: `default: determine_resolution`

### Step 5d: diagnose_known_issue (REASON — branch)
**What**: For recurring/known issues — check if self-resolved, check usage, determine if suppressable.
**Tools**: snowflake_mcp.execute_query
**Context static**: [on_call_conventions]
**Context dynamic**: from parse_alert, from check_failure_history, from get_model_context

**Key queries**:
```sql
-- Check if self-resolved (next run succeeded)
SELECT STATUS, CREATED_AT FROM DW.DATAOPS_DBT.DBT_RUN_RESULTS
WHERE NAME = '{model_name}' ORDER BY CREATED_AT DESC LIMIT 5

-- Check if pipeline is used (for deprecated assessment)
SELECT COUNT(*) as query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%{table_name}%'
  AND START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
  AND USER_NAME NOT IN ('ORCHESTRATION_PROD')
```
**Output**: root_cause, is_self_resolved, should_suppress, usage_last_30d, recommendation

**Transition**: `default: determine_resolution`

### Step 6: determine_resolution (REASON)
**What**: Based on diagnosis, determine the resolution path.
**Tools**: (none)
**Context dynamic**: from classify_alert, from diagnose_* (whichever branch ran), from check_cascade, from get_model_context

**Resolution types**:
| Resolution | When | AE Action Needed |
|-----------|------|-----------------|
| fix_directly | Code fix identified, low risk | Review + merge PR |
| escalate_to_owner | Domain-specific issue, owner identified | Review findings, tag owner |
| escalate_to_de | Infrastructure or upstream pipeline issue | Review findings, tag DE |
| acknowledge_transient | Self-resolved, Snowflake internal error | Acknowledge, no action |
| recommend_suppression | Recurring noise, wrong routing | Review recommendation, update alert config |
| defer_to_morning | After-hours, non-critical | Review context in morning |

**Output**: resolution_type, suggested_actions (list), who_to_tag, urgency, airflow_links

### Step 7: generate_triage_report (REASON — no tools, pure synthesis)
**What**: Produce two artifacts from ALL prior step outputs.
**Tools**: (none)
**Context dynamic**: ALL prior steps
**Budget**: max_tokens: 20000 (report can be long)

**Output 1 — Thread summary** (concise, posted to Slack):
```
TRIAGE: {model_name} — {classification}
Root cause: {one-line summary}
Impact: {cascade_count} models affected
Resolution: {resolution_type}
Action: {first suggested action}
Owner: {owner} | Tag: {who_to_tag}
Full investigation attached
```

**Output 2 — Detailed investigation file** (Markdown, attached):
Full report with: error details, failure history table, cascade analysis, model analysis (SQL structure, lineage diagram), diagnostic queries run with results, root cause analysis, suggested actions with Airflow links, preventive recommendations.

### Step 8: post_to_thread (DELEGATE)
**What**: Post summary to alert thread, attach investigation file.
**Capability**: shift.post_to_thread
**Requires approval**: false (triage is informational, not destructive)
**Context dynamic**: from generate_triage_report

---

## 5. New Knowledge Files

| File | Content |
|------|---------|
| alert_classification.yml | Alert type taxonomy with error patterns, severity mapping, routing rules |
| on_call_conventions.yml | Response SLAs, escalation paths, team-to-owner mapping, when to defer vs act |
| runbook_code_errors.yml | Step-by-step for invalid_identifier, permission, compilation errors |
| runbook_data_issues.yml | Step-by-step for duplicate rows, test failures, upstream data problems |
| runbook_infrastructure.yml | Step-by-step for timeouts, internal errors, warehouse contention |

---

## 6. New Persona Enrichment

Add to analytics_engineer.yml:
```yaml
on_call_heuristics:
  - "If 5+ models fail in same timeframe, check for cascade before investigating individually"
  - "If error contains 'internal error' or '300005', likely transient — check if next run succeeded"
  - "If model has zero queries in QUERY_HISTORY last 30 days, flag as potentially deprecated"
  - "Always check recent GitHub commits before deep investigation — recent push is most common cause"
  - "For duplicate row errors, always trace upstream joins — the DML uniqueness violation is a symptom, not root cause"

on_call_anti_patterns:
  - "Don't investigate cascade children — find and investigate the root model only"
  - "Don't rerun a failing model without understanding why it failed"
  - "Don't suppress alerts without documenting why and getting domain owner agreement"
  - "Don't fix upstream data issues in the downstream model — fix at the source"
```

---

## 7. Context Architecture

All data comes from existing Snowflake MCP:

| Data Source | Used In Steps | Purpose |
|------------|---------------|---------|
| DW.DATAOPS_DBT.DBT_RUN_RESULTS | 0, 1, 2, 5d | Error details, history, cascade detection |
| DW.DATAOPS_DBT.DBT_MODELS | 3 | Owner, tags, schedule, domain, META |
| DW.INFORMATION_SCHEMA.COLUMNS | 5a | Column existence, types for invalid_identifier |
| SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY | 5c, 5d | Warehouse utilization, pipeline usage |
| dbt_mcp (get_lineage_dev) | 3, 5b | Upstream/downstream lineage |
| github_cli (read_file, search_code) | 3, 5a | Model SQL, recent commits |

No new MCP servers required. Airflow actions (rerun, disable) are OUTPUT as suggested actions with links — not executed by the workflow.

---

## 8. Testing Strategy

### Cassette sets (one per branch path):
- tests/cassettes/triage-ae-alert-code-error/ (invalid_identifier path)
- tests/cassettes/triage-ae-alert-data-issue/ (duplicate_row path)
- tests/cassettes/triage-ae-alert-infrastructure/ (internal_error path)
- tests/cassettes/triage-ae-alert-known-issue/ (recurring_transient path)

### E2E tests:
- test_triage_invalid_identifier (classification → code_error branch)
- test_triage_duplicate_row (classification → data_issue branch)
- test_triage_internal_error (classification → infrastructure branch)
- test_triage_recurring_transient (classification → known_issue branch)
- test_cascade_detection (5+ failures → groups correctly)
- test_resolution_types (each resolution type produces correct output)

### Conformance test:
- Verify all 10 steps produce correct request types
- Verify branching transitions are defined correctly
- Verify delegate step has correct capability

---

## 9. Success Criteria

1. Workflow completes for all 4 branch paths with cassette data
2. Classification matches expected type for each test scenario
3. Generated triage report contains: error details, history, cascade analysis, lineage, diagnostic queries, root cause, suggested actions with Airflow links
4. Thread summary is ≤ 10 lines
5. Investigation file contains all diagnostic queries that were run with results
6. Owner is correctly extracted from DBT_MODELS.META
7. Cascade detection correctly groups related failures

---

## 10. Implementation Estimate

| Component | Files | Effort |
|-----------|-------|--------|
| Workflow YAML | 1 | ~250 lines |
| Conformance spec | 1 | ~80 lines |
| Knowledge files (5) | 5 | ~200 lines total |
| Cassette sets (4 paths) | ~40 JSON files | ~800 lines total |
| E2E tests | 1 | ~300 lines |
| Conformance tests | 1 | ~100 lines |
| Persona enrichment | 1 (modify) | ~20 lines |
| Manifest update | 1 (modify) | ~10 lines |
| **Total** | **~50 files** | **~1,760 lines** |

---

## 11. Phased Delivery

**Phase 1 (this spec)**: Triage-only workflow. Investigates, reports, suggests actions. AE executes actions manually.

**Phase 2 (future)**: Add Airflow MCP (`astro-airflow-mcp`). Steps can rerun tasks, clear failed states. Add `execute_fix` step after human gate.

**Phase 3 (future)**: Auto-trigger from Slack webhook. Shift listens to #ae-alerts, auto-starts triage for every Firing alert. AE reviews in thread.

---

## Sources

- 12 real #ae-alerts Slack threads (March 6-12, 2026)
- 30-day Snowflake operational data: 650 errors, 180 models
- Industry research: PagerDuty AIOps, incident.io AI SRE, Monte Carlo, Shoreline.io
- docs/research/RESEARCH-SUMMARY.md
- docs/research/ae-alerts-slack-analysis.md
- docs/research/ae-oncall-snowflake-analysis.md
- docs/research/ai-oncall-assistant-industry-research.md
