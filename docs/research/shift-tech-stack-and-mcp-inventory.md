# Shift Tech Stack & MCP Inventory

## Shift Architecture (from stubhub/Shift repo)

| Component | Location | Tech | Purpose |
|-----------|----------|------|---------|
| Slack Bot | slack-bot/ | Go + Slack Socket Mode | Command parsing, container management |
| Command Server | command-server/ | Go + Claude Code SDK | Session management, MCP integration |
| Orchestrator | orchestrator/ | Go + Docker/K8s | Container lifecycle |

## Auth Providers (MCP Connections Shift Already Has)

| Provider | Directory | What It Connects To |
|----------|-----------|-------------------|
| **anthropic** | auth/anthropic | Claude API |
| **atlassian** | auth/atlassian | Jira + Confluence |
| **azure** | auth/azure | Azure services |
| **azure_devops** | auth/azure_devops | Azure DevOps |
| **github** | auth/github | GitHub (repos, PRs, code) |
| **google** | auth/google | Google services |
| **grafana** | auth/grafana | Grafana dashboards + Prometheus |
| **incidentio** | auth/incidentio | incident.io (alert lifecycle) |
| **openai** | auth/openai | OpenAI API (embeddings?) |
| **snowflake** | auth/snowflake | Snowflake MCP |
| **portio** | auth/portio | Port.io (service catalog?) |
| **enterprise_knowledge** | auth/enterprise_knowledge | Enterprise knowledge base |

## Existing MCP Servers (StubHub.Platform.McpServers)

| Server | Purpose |
|--------|---------|
| StubHub.ConfigCli | Configuration management |
| StubHub.DbTool.CliTool | Database tooling |
| StubHub.GrafanaCli | Grafana + Prometheus queries |
| StubHub.KustoCliTool | Azure Data Explorer (Kusto) queries |
| StubHub.SqlCliTool | SQL Server queries |

## Existing Skills (StubHub.Platform.McpServers/skills/)

| Skill | Purpose |
|-------|---------|
| grafana-prometheus-query | Query Grafana/Prometheus metrics |
| kusto-applogs-query | Query application logs in Kusto |

## Shift Internal Packages Relevant to On-Call

| Package | Purpose |
|---------|---------|
| contextgraph | Context graph integration (!) |
| enterprise_knowledge | Enterprise knowledge retrieval |
| persona | Persona management |
| skill | Skill execution |
| tools | Tool management |

## Shift.Context Service (stubhub/StubHub.Shift.Context)

| Component | Purpose |
|-----------|---------|
| agent-registry | Register and discover agents |
| cas | Content-Addressable Store |
| chat | Chat context management |
| context-registry | Context registration + retrieval |
| db | Database layer |
| ingestion | Content ingestion pipeline |

## What Already Exists vs What's Needed for AE On-Call

| Capability | Status | Notes |
|-----------|--------|-------|
| Snowflake queries | EXISTS | auth/snowflake in Shift |
| GitHub code access | EXISTS | auth/github in Shift |
| incident.io alerts | EXISTS | auth/incidentio in Shift |
| Grafana metrics | EXISTS | auth/grafana + GrafanaCli MCP |
| Jira/Confluence | EXISTS | auth/atlassian in Shift |
| Kusto app logs | EXISTS | KustoCliTool MCP |
| Context graph | EXISTS | contextgraph package in Shift |
| dbt MCP | PARTIAL | Available but permission-gated in some contexts |
| Airflow API | MISSING | Need airflow_mcp or Astronomer API integration |
| Elementary | MISSING | dbt observability — source freshness, schema changes |
| Hex | MISSING | Hex run status + rerun capability |

## Key Insight: Airflow Integration

Astronomer (hosted Airflow) exposes a REST API:
- `GET /api/v1/dags/{dag_id}/dagRuns` — list DAG runs
- `GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances` — task status
- `GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs` — task logs
- `POST /api/v1/dags/{dag_id}/dagRuns` — trigger a DAG run
- `PATCH /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}` — clear/retry a task

The Astronomer deployment URL is visible in alert messages:
`https://clqbkgtqw026r01qpmoae9wnw.astronomer.run/dw9qxrd8/`

An airflow_mcp server would wrap these REST endpoints.

## Architecture for On-Call Assistant

```
Alert fires in Slack (#ae-alerts)
     │
     ▼
Shift detects alert (incident.io webhook or Slack listener)
     │
     ├── incident.io API: get alert details, severity, service
     │
     ▼
DCAG workflow: triage-ae-alert
     │
     ├── Step 1: Parse alert (extract DAG, task, error from Slack message)
     ├── Step 2: Get Airflow context (DAG run status, task logs) ← NEW: airflow_mcp
     ├── Step 3: Get dbt context (model status, lineage) ← EXISTS: dbt_mcp
     ├── Step 4: Classify alert type (branching)
     │     ├── dbt_build_failure → diagnose model error
     │     ├── dbt_test_failure → diagnose data quality
     │     ├── freshness_failure → check source staleness
     │     ├── hex_failure → check Hex run status
     │     ├── sla_missed → identify blocking models
     │     └── infra_issue → check warehouse + Metaflow
     ├── Step 5: Run diagnostics (Snowflake queries, log analysis)
     ├── Step 6: Propose fix or escalation
     ├── Step 7: Human gate (show findings in thread)
     └── Step 8: Execute (rerun task, create PR, or escalate)
```
