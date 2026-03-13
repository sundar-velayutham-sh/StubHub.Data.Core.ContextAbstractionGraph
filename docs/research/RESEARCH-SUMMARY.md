# AE On-Call Assistant Research — Summary for Morning Review

**Date**: 2026-03-13
**Status**: 3 of 4 research tracks complete, industry research agent still running

---

## What We Found

### 1. The Alert Channels (#ae-alerts + #dw-alerts)

**Two-tier alert architecture:**
- `#ae-alerts` (C0590MFQN1W, 51 members) — transformation layer: DAG failures, dbt build/test errors, Hex failures, SLA breaches
- `#dw-alerts` (C040SRYF9HS, 47 members) — ingestion layer: source freshness, reconciliation, DLT loads

**On-call rotation**: Weekly, tracked in #ae-alerts channel topic. AEs cover all domains (CX, pricing, ops, supply, finance, acquisition).

**Domain-specific channels (#ae-supply-alerts, etc.) do NOT exist** — everything routes to #ae-alerts. The META field in dbt models references them aspirationally but they're not created yet.

### 2. The Numbers (30 days of Snowflake data)

| Metric | Value |
|--------|-------|
| Total dbt errors | **650** across **180 models** |
| SQL compilation errors | 46% of all errors |
| Object not found / unauthorized | 121 occurrences, 48 models |
| Invalid identifier (column renames) | 127 occurrences, 28 models |
| Snowflake internal errors | 13% |
| Timeouts | 6% |

### 3. The Biggest Insight: Cascade Amplification

A single root cause (e.g., one column rename on `ad_campaign_flight_agg`) generates **64+ alert messages** — the model failure + 8 test failures × multiple retries. **40-50% of alert noise is cascading failures from a single root cause.**

The #1 value DCAG can deliver: **group cascading failures into a single root-cause triage**.

### 4. The Top 10 Alert Types (for DCAG branching)

| # | Alert Type | Frequency | Auto-fixable? | DCAG Branch |
|---|-----------|-----------|---------------|-------------|
| 1 | Cascade failures (multi-model from single root cause) | Very high | Yes — group + triage root | `cascade_triage` |
| 2 | Object not found / unauthorized | 121/month | Partially — check permissions, flag schema change | `object_not_found` |
| 3 | Invalid identifier (column rename) | 127/month | Yes — diff columns, propose rename | `invalid_identifier` |
| 4 | Snowflake internal error | ~85/month | No — transient, auto-retry or suppress | `internal_error` |
| 5 | Timeout / resource exhaustion | ~40/month | Partially — resize warehouse, optimize query | `timeout` |
| 6 | Source freshness breach | High in #dw-alerts | No — upstream, notify owner | `freshness_stale` |
| 7 | dbt test failure (data quality) | ~36/month | No — data issue, investigate | `test_failure` |
| 8 | Duplicate row / DML error | Moderate | Partially — identify dedup source | `duplicate_row` |
| 9 | Hex notebook failure | ~3/week | No — check Hex API, rerun | `hex_failure` |
| 10 | SLA missed | ~2/week | No — identify blocking model | `sla_missed` |

### 5. Shift Already Has Most Integrations

| Integration | Status |
|------------|--------|
| Snowflake MCP | EXISTS |
| GitHub | EXISTS |
| incident.io | EXISTS |
| Grafana/Prometheus | EXISTS |
| Jira/Confluence | EXISTS |
| Kusto (app logs) | EXISTS |
| contextgraph package | EXISTS in Shift |
| **Airflow/Astronomer API** | **MISSING — #1 gap** |
| Elementary | MISSING |
| Hex API | MISSING |

### 6. The Thread You Shared (p1773301925247169)

Real AE on-call response to `gpm_repeat_buyer_dim` duplicate row failure:
1. Alert fires (incident.io → Slack)
2. AE posts error from Airflow logs
3. Investigation: traced to upstream backfill causing duplicates
4. Cross-team coordination: AE → DE (Tao) → Pricing team
5. Still unresolved 12 hours later

**Key insight**: The on-call AE is a **coordinator**, not just a fixer. DCAG needs to support escalation paths, not just fixes.

---

## Proposed DCAG Workflow: `triage-ae-alert`

```
Alert in #ae-alerts
     │
     ▼
parse_alert → get_airflow_context → get_dbt_context → classify_alert_type
                                                              │
              ┌──────────┬──────────┬──────────┬──────────────┼─────────────┐
              ▼          ▼          ▼          ▼              ▼             ▼
         cascade    object_not   invalid_id  timeout    freshness    test_failure
         _triage    _found       _entifier   /resource  _stale
              │          │          │          │              │             │
              └──────────┴──────────┴──────────┴──────────────┴─────────────┘
                                        │
                                        ▼
                               run_diagnostics → propose_action → human_gate
                                                                      │
                                                          ┌───────────┼──────────┐
                                                          ▼           ▼          ▼
                                                     auto_fix    escalate    suppress
                                                     (PR/rerun)  (tag team)  (transient)
```

---

## New MCP Servers Needed

| Server | Priority | What It Does |
|--------|----------|-------------|
| **airflow_mcp** | P0 | DAG runs, task status, task logs, rerun tasks |
| **elementary_mcp** | P1 | Source freshness, schema changes, anomaly detection |
| **hex_mcp** | P2 | Hex run status, rerun notebooks |

## New Knowledge Files Needed

| File | Content |
|------|---------|
| `alert_classification.yml` | Alert type taxonomy with patterns and severity |
| `escalation_paths.yml` | Team → owner mapping, who to tag for each domain |
| `runbook_dbt_failures.yml` | Step-by-step triage for each dbt error type |
| `runbook_freshness.yml` | Source freshness triage steps |
| `runbook_infrastructure.yml` | Warehouse/Metaflow triage steps |
| `on_call_conventions.yml` | How to respond, SLAs, handoff protocol |

---

## Files Produced by This Research

| File | Content |
|------|---------|
| `docs/research/ae-alerts-channel-initial-findings.md` | Manual analysis of #ae-alerts thread + channel |
| `docs/research/ae-alerts-slack-analysis.md` | Full Slack channel analysis (from agent) |
| `docs/research/ae-oncall-snowflake-analysis.md` | 30-day dbt failure + operational analysis |
| `docs/research/shift-tech-stack-and-mcp-inventory.md` | Shift architecture + existing MCP inventory |
| `docs/research/ai-oncall-assistant-industry-research.md` | Industry research (pending — agent still running) |
