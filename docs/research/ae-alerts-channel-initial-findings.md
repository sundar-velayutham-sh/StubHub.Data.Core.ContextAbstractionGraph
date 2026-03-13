# AE Alerts Channel (C0590MFQN1W) — Initial Findings

**Channel**: #ae-alerts (C0590MFQN1W)
**Alert source**: incident.io (bot: B03DQNHQVE2)
**Service**: StubHub.AirflowAE
**Escalation path**: Analytics Engineering
**On-call rotation**: Weekly, tracked in channel topic

## Sample Thread Analyzed

**Thread**: p1773301925247169
**Alert**: `Firing - HTTP - Failed Task in DAG transform_pricing__daily: dbt_build.gpm_repeat_buyer_dim_run`
**Error**: `100090 (42P18): Duplicate row detected during DML action` in `gpm_repeat_buyer_dim`

**What happened (real AE on-call response)**:
1. Alert fires at ~midnight (incident.io webhook → Slack)
2. AE (albert.hu) posts the actual error message from Airflow logs
3. On-call AE (shruti, then covering: someone else) acknowledges
4. Investigation: traced to upstream duplicate participations caused by a backfill
5. Cross-reference with another thread about the same backfill
6. Tao (DE) kicked off dedup backfill
7. Still not resolved 12 hours later — "will fail tomorrow, pricing team aware"

**Key insight**: This is a MULTI-TEAM issue (AE alert → DE root cause → pricing team impact). The on-call AE is a coordinator, not just a fixer.

## Alert Types Seen in Last 50 Messages

### By DAG / Domain
| DAG | Alert Count | Domain |
|-----|-------------|--------|
| transform_cx__hourly | 8 | Customer Experience |
| transform_pricing__daily | 2 | Pricing |
| transform_marketshare__daily | 2 | Market Share |
| transform_optimal_external__hourly | 2 | Optimal/POS |
| transform_external_rpt__subhourly | 1 | External Reporting |
| transform_ops__daily | 3 | Operations |
| transform_strategic_finance | 1 | Finance |
| transform_acquisition__hourly | 1 | Acquisition |
| transform_marketshare_ds__daily | 1 | Market Share DS |
| transform_marketshare_ds__hourly | 1 | Market Share DS |
| refresh_tableau_workbook__* | 1 | Tableau Refresh |
| monitor__acquisition_sources_tests | 1 | Acquisition Tests |

### By Alert Type
| Type | Count | Description |
|------|-------|-------------|
| Failed dbt build task | ~15 | dbt model compilation or runtime error |
| Failed dbt test task | ~5 | dbt test failure (data quality) |
| Failed Hex project run | ~3 | Hex notebook execution failure |
| SLA missed | ~2 | Dataset freshness SLA breached |
| Unresolved alerts count | ~1 | Meta-alert: too many open alerts |
| Human comms | ~5 | On-call handoffs, status updates, FYIs |

### By Status
| Status | Count |
|--------|-------|
| Resolved | ~25 (auto-resolved after retry or manual fix) |
| Firing | ~4 (currently active) |
| Human updates | ~10 (context, handoffs, investigations) |

### By Severity Pattern
| Pattern | Description |
|---------|-------------|
| Self-resolving | Fires, then resolves within 1-2 hours (most common) |
| Needs investigation | Fires, AE investigates, finds root cause, escalates if needed |
| Upstream dependency | Failure caused by upstream data issue or backfill |
| Infrastructure | Metaflow/warehouse overload causing cascading failures |

## On-Call Patterns Observed

1. **Weekly rotation** — AEs swap weeks, update incident.io + channel topic
2. **Covering for sick/OOO** — explicit handoffs in channel
3. **Cross-team coordination** — AE on-call triages, then tags DE or domain team
4. **Hex report monitoring** — Hex notebook failures also route to AE alerts
5. **SLA monitoring** — "IMPORTANT DATASET SLAS MISSED" is a distinct alert type
6. **After-hours awareness** — alerts fire 24/7, AEs respond during business hours ("In-hours" priority)

## Tech Stack Components Observed

| Component | Role | Integration Point |
|-----------|------|-------------------|
| **incident.io** | Alert routing + lifecycle | Slack webhook → alert message |
| **Astronomer/Airflow** | DAG execution + task monitoring | Task failure → incident.io |
| **dbt** | Model build + test execution | Build/test tasks within Airflow |
| **Hex** | Report execution | Hex project runs within Airflow |
| **Tableau** | Workbook refresh | Refresh tasks within Airflow |
| **Snowflake** | Query execution | All dbt builds execute here |
| **Metaflow** | ML pipeline execution | Can cause warehouse contention |
| **Elementary** | dbt observability | Source freshness + schema monitoring |

## MCP Servers Needed for On-Call Assistant

| MCP Server | Purpose | Priority |
|------------|---------|----------|
| **snowflake_mcp** | Query metadata, run diagnostics, check data | EXISTS |
| **dbt_mcp** | Compile, test, lineage, node details | EXISTS |
| **github_cli** | Read model SQL, create fix PRs | EXISTS |
| **airflow_mcp** (NEW) | Get DAG run status, task logs, rerun tasks | HIGH |
| **incident_io_mcp** (NEW) | Acknowledge alerts, update status, resolve | HIGH |
| **hex_mcp** (NEW) | Check Hex run status, rerun notebooks | MEDIUM |
| **slack_mcp** | Post updates to thread, tag people | EXISTS (Shift native) |
| **elementary_mcp** (NEW) | Source freshness, schema change detection | MEDIUM |
