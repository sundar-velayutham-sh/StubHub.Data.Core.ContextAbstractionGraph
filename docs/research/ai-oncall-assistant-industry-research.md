# AI On-Call Assistant — Industry Research

**Date:** 2026-03-12
**Purpose:** Groundwork for building an AE (Analytics Engineer) on-call assistant using DCAG + Shift at StubHub
**Scope:** Industry products, architecture patterns, data engineering on-call workflows, alert taxonomy, integration design

---

## Table of Contents

1. [Existing AI On-Call Products](#1-existing-ai-on-call-products)
2. [Architecture Patterns for AI On-Call](#2-architecture-patterns-for-ai-on-call)
3. [Data Engineering Specific On-Call Patterns](#3-data-engineering-specific-on-call-patterns)
4. [Alert Classification Taxonomy](#4-alert-classification-taxonomy)
5. [DCAG On-Call Workflow Design](#5-dcag-on-call-workflow-design)
6. [Integration Architecture](#6-integration-architecture)
7. [Sources](#7-sources)

---

## 1. Existing AI On-Call Products

### 1.1 PagerDuty AIOps

**How it works:** PagerDuty AIOps uses AI/ML algorithms to automatically correlate and deduplicate large volumes of incoming events. The system reduces alert noise, improves incident visibility, and removes manual, repetitive work from incident response.

**Key capabilities:**
- **Event Intelligence:** ML-driven alert grouping, deduplication, and suppression across all integrated monitoring tools
- **Outlier Incident Classification:** Tells on-call if an incident is frequent, rare, or an anomaly — critical for prioritization
- **Past Incidents:** Shows if a similar incident has occurred before, with frequency data over the last 6 months and links to historical metadata
- **SRE Agent (GA October 2025):** End-to-end incident automation with human oversight — runs diagnostics, surfaces context, provides analysis, suggests remediation, and can execute actions upon approval
- **Spring 2026 Release:** Autonomous Detection, Triage, and Diagnosis — identifies anomalies, assesses the tech stack, and performs deep diagnostics before a human is awakened

**Relevance to DCAG:** PagerDuty's evolution from alert routing to autonomous triage-and-diagnose is exactly the trajectory we want for the AE on-call assistant. Their "Past Incidents" pattern is directly applicable — DCAG could store resolved alert patterns in its knowledge graph and surface them for new alerts.

**Ref:** [PagerDuty AIOps](https://www.pagerduty.com/platform/aiops/), [PagerDuty Spring 2026 Release](https://www.businesswire.com/news/home/20260312121276/en/PagerDuty-Unveils-Next-Generation-of-the-Operations-Cloud-Platform-with-the-Spring-2026-Release)

---

### 1.2 Datadog Bits AI SRE

**How it works:** Bits AI SRE is an autonomous AI agent that investigates alerts, identifies root causes, and helps engineers resolve incidents. It is aware of telemetry, architecture, and organizational context.

**Key capabilities:**
- **Autonomous Investigation:** When an alert fires, Bits AI SRE immediately launches an investigation — gathers context, reads monitor messages, checks linked Confluence runbooks, and dynamically generates multiple root cause hypotheses to test
- **Cross-dependency Root Cause:** Determines root cause for system-wide alerts involving multiple dependencies
- **Tested at Scale:** Over 2,000 customer environments, tens of thousands of investigations, from routine alerts to high-severity incidents
- **Speed:** Pinpoints root causes in minutes, helping teams resolve services 90% faster
- **Second Generation (2026):** Approximately twice as fast, more accurate on internal benchmarks, with deeper reasoning capabilities

**Relevance to DCAG:** Bits AI SRE's architecture of "gather context, form hypotheses, test hypotheses" maps cleanly onto a DCAG workflow. The key insight is that investigation is a search-and-reason loop, not a linear runbook. DCAG's conditional walker pattern supports this naturally.

**Ref:** [Bits AI SRE](https://www.datadoghq.com/product/ai/bits-ai-sre/), [Bits AI SRE Deeper Reasoning](https://www.datadoghq.com/blog/bits-ai-sre-deeper-reasoning/)

---

### 1.3 BigPanda

**How it works:** BigPanda processes events through a multi-stage pipeline: filtering, normalization, deduplication, aggregation, and enrichment. Claims 98% noise reduction through ML-driven correlation.

**Key capabilities:**
- **Event Normalization:** Heterogeneous data from different monitoring tools is normalized using key-value "tags" — providing a consistent format regardless of source
- **Event Deduplication:** Intelligently parses incoming events to identify duplicates or updates to existing alerts, which are merged or discarded
- **Event Filtering:** Automatically suppresses non-actionable events (maintenance windows, non-production environments)
- **Alert Correlation Engine:** Groups related alerts into meaningful, actionable incidents in real time
- **Agentic IT Operations (2025):** Rebranded to "BigPanda AI Detection and Response" — uses AI to detect, respond to, and prevent IT incidents at machine speed

**Relevance to DCAG:** BigPanda's normalization pipeline (filter > normalize > dedup > aggregate > enrich) is a pattern we should adopt. In the AE context: alert fires in Slack > DCAG normalizes (extract model name, error type, DAG, severity) > dedup against recent alerts > enrich with Snowflake metadata and dbt lineage.

**Ref:** [BigPanda Core](https://www.bigpanda.io/our-product/bigpanda-core/), [BigPanda Noise Reduction](https://www.bigpanda.io/blog/fast-track-video-series-slash-it-noise-by-up-to-98-with-alert-correlation-with-bigpanda/)

---

### 1.4 Moogsoft (Dell APEX AIOps)

**How it works:** Moogsoft uses a novel streaming-based clustering algorithm (ACE) to group alerts into "Situations." It then uses supervised learning for Probable Root Cause (PRC) identification.

**Key capabilities:**
- **ACE Clustering Engine:** Uses fuzzy matching to find the most appropriate group of events for each incoming alert — similarity-based directives control cluster assignment
- **Probable Root Cause (PRC):** An ML algorithm that assigns each alert in an incident a root cause score (High/Medium/Low) based on calculated probability of being causal
- **Supervised Learning for PRC:** Learns from user feedback to build a data model that predicts causal alerts — adapts to rapidly changing infrastructures, unlike static behavioral models
- **Situation Awareness:** Groups related alerts into "Situations" that organize and present incidents with rich context

**Relevance to DCAG:** Moogsoft's PRC concept is powerful for our use case. When multiple AE alerts fire simultaneously (e.g., a Snowflake warehouse queue causes cascading dbt timeouts), the system should identify the warehouse queue as the probable root cause rather than treating each dbt failure independently. This is something DCAG could implement using the knowledge graph to model alert causality chains.

**Ref:** [Moogsoft Probable Root Cause](https://docs.moogsoft.com/moogsoft-cloud/en/probable-root-cause-overview.html), [Moogsoft Alert Correlation](https://docs.moogsoft.com/moogsoft-cloud/en/correlate-alerts-into-incidents.html)

---

### 1.5 Shoreline.io

**How it works:** Shoreline focuses specifically on auto-remediation through executable runbooks. It collects real-time resource metrics across your fleet and triggers configured remediation actions.

**Key capabilities:**
- **Auto-Remediation:** Detects and auto-remediates over 50% of incidents without human intervention
- **Runbook Library:** 120+ pre-built runbooks created by experts — blueprints for end-to-end automated incident repair
- **Measurable Impact:** MTTR reduced by over 75% to less than 1 hour; 50% of incidents auto-remediated in seconds
- **Interactive Debugging:** Engineers can interactively debug at scale and quickly build new remediations to eliminate repetitive work
- **Metric-Triggered Actions:** Real-time resource metrics trigger configured remediation actions based on thresholds

**Relevance to DCAG:** Shoreline's runbook-as-code approach maps directly to DCAG workflows. Each alert type should have a corresponding DCAG workflow that codifies the investigation and remediation steps. The 120+ runbook library concept suggests we should build a library of AE-specific DCAG workflows — one per alert type.

**Ref:** [Shoreline Runbook Automation Guide](https://www.shoreline.io/blog/the-guide-to-automating-runbook-execution), [Shoreline Incident Automation](https://www.shoreline.io/blog/what-is-incident-automation)

---

### 1.6 Rootly

**How it works:** Rootly is an AI-native incident response platform with deep Slack/Teams integration. It automates incident workflows based on incident events.

**Key capabilities:**
- **Slack-Native:** Automatically creates dedicated Slack channels, Zoom meetings, and Jira tickets when incidents are declared
- **AI SRE (91% faster resolution):** Analyzes Slack conversations, GitHub diffs, and observability tools to surface probable root causes
- **Workflow Automation:** Runs workflows on incident events (create, update, field changes, channel events) to coordinate responders
- **AI-Agent-First API (March 2025):** Implemented the Agents JSON standard so LLM agents can interact with the full Rootly API — enables AI-powered co-pilots for incident management

**Relevance to DCAG:** Rootly's Slack-native workflow is the closest analog to what we want to build. Their AI-Agent-First API design is also instructive — DCAG should be designed so that the on-call assistant can interact with it programmatically, not just through a fixed workflow.

**Ref:** [Rootly AI SRE](https://rootly.com/), [Rootly AI-Agent-First API](https://www.businesswire.com/news/home/20250312871641/en/Rootly-Makes-Its-API-AI-Agent-First-to-Elevate-Incident-Management)

---

### 1.7 incident.io AI SRE

**How it works:** incident.io developed a multi-agent system for incident investigation. The system forms hypotheses, tests them, and even drafts fixes — all from within Slack.

**Key capabilities:**
- **Multi-Agent Investigation:** Parallel searches across GitHub PRs, Slack messages, historical incidents, logs, metrics, and traces to build root cause hypotheses
- **Hypothesis-Driven:** Creates investigations that run parallel searches, generate findings, formulate hypotheses, ask clarifying questions through sub-agents, and present actionable reports in Slack within 1-2 minutes
- **Code-Aware:** Spots the likely pull request behind an incident and can generate a fix and open a PR if the root cause is a code issue
- **Hybrid Retrieval Architecture:** Deterministic tagging and re-ranking over complex vector setups — emphasizes deterministic components with LLM-powered reranking rather than pure vector embeddings

**Relevance to DCAG:** incident.io's multi-agent architecture is the most directly relevant to our design. Their finding that "deterministic tagging + re-ranking beats complex vector setups" validates DCAG's approach of structured context graphs over pure embedding-based retrieval. Their 1-2 minute investigation time is our target benchmark.

**Ref:** [incident.io AI SRE](https://incident.io/ai-sre), [ZenML LLMOps: incident.io Multi-Agent Investigation](https://www.zenml.io/llmops-database/ai-powered-incident-response-system-with-multi-agent-investigation)

---

### 1.8 Opsgenie / Jira Service Management

**How it works:** Opsgenie provides alert routing, on-call scheduling, and escalation policies. Atlassian is consolidating Opsgenie into Jira Service Management (new purchases stopped June 2025).

**Key capabilities:**
- **5-Level Priority System:** P1 through P5, with routing rules and escalation policies based on priority
- **Source-Based Routing:** Alerts routed based on source, content, and time
- **AI-Driven Alert Grouping (JSM):** Migrating to Jira Service Management with enhanced AIOps: AI-driven alert grouping, automated resolution, and Post-Incident Review (PIR)

**Relevance to DCAG:** The P1-P5 priority model is industry standard and we should adopt it. The consolidation into JSM suggests the market is moving toward unified platforms rather than standalone alert managers.

**Ref:** [Opsgenie Alert Priority](https://support.atlassian.com/opsgenie/docs/what-are-incident-priority-levels/), [Opsgenie Consolidation into JSM](https://www.infoq.com/news/2025/03/atlassian-opsgenie-consolidation/)

---

### 1.9 Monte Carlo (Data Observability)

**How it works:** Monte Carlo uses machine learning to monitor data, pipelines, and AI systems for unintended changes in structure and quality. Over 1,000 data quality incidents resolved daily on the platform.

**Key capabilities:**
- **Monitoring Agent (GA 2025):** AI agent that recommends data quality monitoring rules and thresholds, deployed with push-of-button — 60% acceptance rate, 30%+ improvement in monitoring deployment efficiency
- **Troubleshooting Agent (Q2 2025):** Investigates, verifies, and explains root cause of data quality issues by testing hundreds of hypotheses across all relevant tables — reduces average time-to-resolve by 80%+
- **ML-Based Anomaly Detection:** Scans for unintended changes in data structure and quality without manual rule configuration

**Relevance to DCAG:** Monte Carlo's Troubleshooting Agent is the closest data-engineering-specific product to what we're building. Their "test hundreds of hypotheses across relevant tables" approach should inform our DCAG workflow design — the on-call assistant should systematically check upstream tables, row counts, schema changes, and freshness before presenting findings.

**Ref:** [Monte Carlo Observability Agents](https://www.montecarlodata.com/blog-monte-carlo-observability-agents), [Monte Carlo Data Quality Statistics](https://www.montecarlodata.com/blog-data-quality-statistics/)

---

### 1.10 Anomalo

**How it works:** Anomalo uses unsupervised ML to detect anomalies across structured, semi-structured, and unstructured data without manual configuration.

**Key capabilities:**
- **Unsupervised ML Detection:** No rule authoring required — learns patterns automatically
- **Automated Root Cause Analysis:** Provides root cause analysis and data lineage tools to rapidly mitigate issues
- **LLM-Powered Quality Scoring:** For unstructured data, uses LLMs to detect missing metadata, corrupted files, PII exposure, etc.
- **Native Integrations:** Snowflake, Databricks, BigQuery, Airflow, dbt, Jira, Slack

**Relevance to DCAG:** Anomalo's no-config anomaly detection is aspirational for our system. Initially, our DCAG workflows will be rule-based (explicit alert type classification), but over time we could add ML-based anomaly detection as a "discovery" mode that flags issues before they become alerts.

**Ref:** [Anomalo Platform](https://www.anomalo.com/), [Anomalo Anomaly Detection](https://www.anomalo.com/anomaly-detection-software/)

---

### 1.11 Elementary (dbt-native)

**How it works:** Elementary is an open-source dbt package that captures metadata, artifacts, and test results for anomaly detection and data quality monitoring.

**Key capabilities:**
- **dbt-Native:** Installs as a dbt package — runs anomaly detection tests as native dbt tests
- **Automated Alerting:** Sends alerts to Slack/Teams when tests or models fail, with custom channel routing and owner tagging
- **Anomaly Detection Tests:** Collects metrics over time (freshness, volume, distribution, cardinality) and detects anomalies statistically
- **Metadata Backbone:** Generates and updates metadata tables in the warehouse from dbt runs — the observability data store

**Relevance to DCAG:** Elementary is directly relevant because StubHub uses dbt. However, per our Slack analysis, Elementary does not currently have a dedicated Slack channel at StubHub — dbt test failures route through Airflow/incident.io instead. A DCAG integration could query Elementary's metadata tables in Snowflake directly for enrichment during triage.

**Ref:** [Elementary GitHub](https://github.com/elementary-data/elementary), [Elementary Documentation](https://docs.elementary-data.com/home)

---

### 1.12 Harness AI SRE

**How it works:** Harness provides runbook automation for incident resolution with pre-built action libraries across multiple categories.

**Key capabilities:**
- **Runbook Actions Library:** Pre-built actions for communication (Slack, Teams), ticketing (Jira, ServiceNow), monitoring (Datadog, New Relic), and on-call management
- **Pipeline Integration:** Native integration with Harness Pipelines for automated remediation and deployment control
- **One-Click Remediation:** Automation runbooks can remediate or delegate in one click
- **Pattern Recognition:** AI analyzes patterns in logs and metrics to predict failures and suggest remediation

**Relevance to DCAG:** Harness's action library concept is directly applicable. DCAG workflows should have a library of composable "actions" — query Snowflake, check dbt lineage, post to Slack, create Jira ticket, rerun Airflow task — that can be composed into alert-type-specific workflows.

**Ref:** [Harness AI SRE Runbooks](https://developer.harness.io/docs/ai-sre/runbooks/), [Harness AI SRE Overview](https://developer.harness.io/docs/ai-sre/get-started/overview/)

---

## 2. Architecture Patterns for AI On-Call

### 2.1 Alert Processing Pipeline (BigPanda Pattern)

Every product follows some variant of this pipeline:

```
Raw Alert
  |
  v
[1. INGEST] ---- Parse source-specific format (incident.io webhook, Airflow callback, etc.)
  |
  v
[2. NORMALIZE] - Extract common fields: severity, source, model, error, timestamp
  |
  v
[3. DEDUPLICATE] - Is this a duplicate of a recent alert? Merge or suppress.
  |
  v
[4. CORRELATE] --- Are related alerts firing? Group into a single incident.
  |
  v
[5. CLASSIFY] ---- What type of alert is this? (model failure, freshness, test, infra)
  |
  v
[6. ENRICH] ------ Pull context: dbt lineage, Snowflake metadata, past incidents, runbook
  |
  v
[7. TRIAGE] ------ Determine severity, priority, assignee, auto-remediable?
  |
  v
[8. ACT] --------- Auto-remediate OR present findings to human with recommended action
```

### 2.2 Multi-Agent Investigation (incident.io Pattern)

The most sophisticated products use multi-agent orchestration:

```
Alert Fires
  |
  v
[Supervisor Agent] -- Analyzes alert, determines investigation strategy
  |
  +---> [Log Agent] -------- Searches logs for error patterns
  +---> [Code Agent] ------- Searches recent PRs/commits for related changes
  +---> [Metrics Agent] ---- Checks telemetry for anomalies
  +---> [History Agent] ---- Searches past incidents for matches
  +---> [Runbook Agent] ---- Finds applicable runbook/remediation
  |
  v
[Synthesis] -- Combines findings, forms hypotheses, ranks by probability
  |
  v
[Report] ----- Presents to human in Slack with root cause + recommended action
```

Research shows multi-agent orchestration achieves:
- **100% actionable recommendation rate** vs 1.7% for single-agent (80x improvement)
- **92.1% root cause identification accuracy** vs 67.3% manual baseline
- **82% reduction** in mean time to diagnosis
- **45% faster problem resolution** and **60% more accurate outcomes** vs single-agent

**Ref:** [Multi-Agent LLM Orchestration for Incident Response (arXiv)](https://arxiv.org/abs/2511.15755), [AWS Multi-Agent SRE with Bedrock](https://aws.amazon.com/blogs/machine-learning/build-multi-agent-site-reliability-engineering-assistants-with-amazon-bedrock-agentcore/)

### 2.3 Human-in-the-Loop Architecture

Every production system implements HITL. The pattern:

```
[Agent proposes action]
  |
  v
[Risk Assessment]
  |
  +-- Low risk (read-only, diagnostic) --> Auto-execute
  +-- Medium risk (rerun task, resize WH) --> Propose + one-click approve
  +-- High risk (modify data, deploy code) --> Require explicit approval + audit log
```

Design principles:
- Every human override, confirmation, or correction must be logged for auditing
- Teams expand the autonomy envelope for low-risk actions over time
- Irreversible or financially sensitive operations always require approval
- The agent drafts changes and proposes next actions but only executes after approval in high-stakes environments

**Ref:** [Human-in-the-Loop Architecture Patterns](https://www.agentpatterns.tech/en/architecture/human-in-the-loop-architecture), [OpenAI Agents SDK HITL](https://openai.github.io/openai-agents-js/guides/human-in-the-loop/)

### 2.4 Runbook-as-Code (Shoreline Pattern)

Runbooks are treated as first-class software artifacts:
- **Declarative** — express desired state, not step-by-step instructions
- **Version-controlled** — stored in Git alongside infrastructure code
- **Parameterized** — accept alert context as input (model name, error type, DAG name)
- **Composable** — built from reusable actions (query Snowflake, check lineage, post to Slack)
- **Observable** — emit telemetry about execution time, success rate, human override rate

This maps directly to DCAG's workflow YAML format.

### 2.5 Learning from Past Incidents (PagerDuty + Moogsoft Pattern)

The feedback loop is critical:

```
[Incident Occurs] --> [Agent investigates] --> [Human resolves]
  |                                               |
  v                                               v
[Store: alert signature, root cause,         [Store: was agent helpful?
 resolution steps, time to resolve]           What did human do differently?]
  |                                               |
  +-----------------------------------------------+
  |
  v
[Update: classification model, runbook library, knowledge graph]
```

Moogsoft's PRC (Probable Root Cause) uses supervised learning from analyst feedback — each time a human confirms or overrides the system's root cause prediction, the model improves.

---

## 3. Data Engineering Specific On-Call Patterns

### 3.1 What a Typical AE On-Call Shift Looks Like (StubHub-Specific)

Based on our Slack analysis of #ae-alerts and #dw-alerts:

**Daily Pattern:**
- Overnight (midnight-6am): Automated alerts fire from nightly DAGs (pricing, operations, strategic finance). Most auto-resolve after Airflow retries.
- Morning (6am-10am): On-call AE reviews overnight alerts. Investigates any still-firing alerts. Hourly DAGs (CX, acquisition, market share) may have failures.
- Business hours (10am-5pm): Ad hoc issues, Hex report failures, Tableau refresh failures. Cross-team coordination if upstream data issues.
- Evening (5pm-midnight): Sub-hourly external reporting alerts. Monitoring for SLA breaches.

**Weekly Volume (from Slack analysis):**
- ~12 Firing alerts per week in #ae-alerts
- ~30 Resolved alerts (most self-resolve on retry)
- ~8 dbt test failures
- ~2 SLA breaches
- ~2 Snowflake task failures
- ~5 human triage/coordination messages

**Most Time-Consuming Activities:**
1. Tracing a dbt model failure to its upstream cause (30-60 min per incident)
2. Coordinating with other teams when root cause is outside AE (multi-hour)
3. Investigating duplicate key / data quality issues in source data
4. Monitoring cascading failures when warehouse is overloaded
5. Manually checking freshness of source tables when SLA alerts fire

### 3.2 Most Common Data Pipeline Failures

Based on industry research and StubHub Slack analysis:

| Rank | Failure Type | Frequency | Typical Cause | Typical Resolution |
|------|-------------|-----------|---------------|-------------------|
| 1 | dbt model runtime error | Very High | SQL compilation errors, invalid identifiers, type mismatches | Fix SQL, full refresh if schema drift |
| 2 | Source freshness breach | Very High | Upstream system delay, ingestion pipeline failure | Wait for upstream OR escalate to DE |
| 3 | dbt test failure (data quality) | High | Duplicate keys from backfill, null values, referential integrity | Investigate upstream, dedup, or update test threshold |
| 4 | Airflow task timeout | High | Snowflake warehouse congestion, query complexity | Resize warehouse, optimize query, or retry |
| 5 | Schema change in source | Medium | Upstream system deployed new columns or changed types | Update dbt model, add new columns, run full refresh |
| 6 | Warehouse queue timeout | Medium | Too many concurrent queries, warehouse undersized | Wait or resize; may cause cascading failures |
| 7 | Incremental model drift | Medium | Late-arriving data, on_schema_change misconfiguration | Full refresh, fix incremental logic |
| 8 | Permission / role error | Low | Role grants expired, new table without grants | Re-grant permissions |
| 9 | Hex notebook failure | Low | API rate limit, credential expiry, data shape change | Rerun or fix notebook |
| 10 | Tableau refresh failure | Low | Extract too large, credential issues, Snowflake connectivity | Investigate extract size, refresh credentials |

### 3.3 Existing Runbook Patterns

Industry best practices for data engineering runbooks:

**Freshness Alert Runbook:**
1. Check source table `loaded_at` timestamp in Snowflake
2. Check ingestion DAG status in Airflow
3. If ingestion DAG failed: check error logs, retry
4. If ingestion DAG succeeded but data stale: check source system status
5. If source system down: notify stakeholders with ETA, suppress alert
6. Document resolution in incident thread

**dbt Model Failure Runbook:**
1. Read error message from Airflow task log
2. Categorize: SQL compilation vs runtime vs timeout
3. For SQL errors: check compiled SQL in `target/` directory, compare with source schema
4. For runtime errors: check Snowflake query history for the failed query, look for data issues
5. For timeouts: check warehouse load history, consider resize or query optimization
6. Fix and test in dev, then merge + rerun

**dbt Test Failure Runbook:**
1. Identify failing test and affected model
2. Query the test failure audit table (dbt stores failed rows)
3. Check if this is a known issue or new
4. For unique violations: check for duplicate source data (backfill, replay)
5. For not_null violations: check if source schema changed
6. For freshness: check upstream pipeline
7. Decide: fix data, update test, or accept known issue

### 3.4 Key Metrics for Data Engineering On-Call

| Metric | Description | Target |
|--------|-------------|--------|
| MTTD | Mean Time to Detect — alert fires to on-call acknowledges | < 15 min (business hours) |
| MTTT | Mean Time to Triage — acknowledge to root cause identified | < 30 min |
| MTTR | Mean Time to Resolve — root cause to resolution | < 2 hours |
| Alert-to-Noise Ratio | Actionable alerts / total alerts | > 50% |
| Self-Resolution Rate | Alerts that resolve without human intervention | Track but don't optimize blindly |
| Recurrence Rate | Same alert type firing repeatedly | < 10% week-over-week |
| Escalation Rate | Alerts requiring cross-team escalation | Track for capacity planning |

---

## 4. Alert Classification Taxonomy

Based on industry patterns and StubHub #ae-alerts / #dw-alerts Slack analysis:

| # | Category | Sub-Type | Example Error | Severity | Auto-Remediable? | DCAG Action |
|---|----------|----------|---------------|----------|-------------------|-------------|
| 1 | **Model Failure** | SQL Compilation — Invalid Identifier | `SQL compilation error: invalid identifier 'COLUMN_X'` | P2 | **Yes** — check schema drift, suggest column rename | Query `INFORMATION_SCHEMA.COLUMNS`, compare with model SQL, suggest fix |
| 2 | **Model Failure** | SQL Compilation — Type Mismatch | `Numeric value 'abc' is not recognized` | P2 | **Yes** — suggest TRY_CAST or type coercion | Identify column and upstream type, propose TRY_CAST wrapper |
| 3 | **Model Failure** | SQL Compilation — Syntax Error | `unexpected 'FROM'` | P3 | **Partially** — can show compiled SQL and pinpoint error location | Show compiled SQL from `target/`, highlight error line |
| 4 | **Model Failure** | Runtime — Timeout | `Query exceeded 2 hour warehouse timeout` | P1 | **Partially** — can resize warehouse or suggest optimization | Check `QUERY_HISTORY` for bytes scanned, suggest clustering key or warehouse resize |
| 5 | **Model Failure** | Runtime — Duplicate Row (MERGE) | `Duplicate row detected during DML action` | P1 | **No** — upstream data issue, needs investigation | Check upstream table for duplicates, identify backfill or replay cause, escalate to DE |
| 6 | **Model Failure** | Runtime — Out of Memory | `Insufficient memory or disk` | P1 | **Partially** — resize warehouse | Check query profile, suggest warehouse upgrade or query refactor |
| 7 | **Model Failure** | Permission Error | `Insufficient privileges to operate on table` | P2 | **Yes** — if grant templates exist | Check role grants, suggest GRANT statement, escalate if role change needed |
| 8 | **Freshness** | Source Stale | `Freshness exceeded acceptable times on source X` | P2 | **No** — upstream issue | Check `loaded_at` timestamp, check ingestion DAG, report findings to on-call |
| 9 | **Freshness** | SLA Breach | `IMPORTANT DATASET SLAS MISSED` | P1 | **No** — needs human judgment on impact | Check which datasets missed SLA, identify downstream dashboards, notify stakeholders |
| 10 | **Test Failure** | Unique Violation | `Got X results, configured to fail if != 0` (unique test) | P1 | **No** — data issue | Query test failure table for duplicate rows, check upstream for backfill/replay |
| 11 | **Test Failure** | Not-Null Violation | `Got X results, configured to fail if != 0` (not_null test) | P2 | **No** — data or schema issue | Check if source column became nullable, check recent schema changes |
| 12 | **Test Failure** | Accepted Values Violation | `Got unexpected value 'X'` | P2 | **Partially** — may just need config update | Check if new enum value was added upstream, suggest updating accepted_values list |
| 13 | **Test Failure** | Relationship Violation | `Referential integrity violation` | P2 | **No** — data issue | Check parent table for missing keys, trace to source system |
| 14 | **Infrastructure** | Warehouse Queue | `Warehouse X has been queued for N seconds` | P1 | **Partially** — can suggest scaling | Check `WAREHOUSE_LOAD_HISTORY`, identify competing queries, suggest multi-cluster or scaling |
| 15 | **Infrastructure** | Snowflake Task Failure | `TASK_EXEC_LOAD_X failed` | P2 | **Partially** — can retry task | Check task history, retry if transient, escalate if persistent |
| 16 | **Infrastructure** | Snowflake Deployment Failure | `Deployment failed` | P1 | **No** — needs DE investigation | Gather deployment logs, identify failing migration, escalate |
| 17 | **Ingestion** | DLT Load Failure | `load_dlt__salesforce__X failed` | P2 | **Partially** — retry may work | Check DLT logs, retry, check API credentials if auth error |
| 18 | **Ingestion** | Data Migrator Failure | `Data Migrator Load Failure - X` | P2 | **No** — custom migration | Check migrator logs, identify failing table, escalate to DE |
| 19 | **Ingestion** | Reconciliation Error | `Row diffs detected` / `Hard deletes detected` | P2 | **No** — needs investigation | Compare row counts, identify missing/extra rows, check source system |
| 20 | **Reporting** | Tableau Refresh Failure | `refresh_tableau_workbook__X failed` | P3 | **Yes** — retry usually works | Retry refresh, check Tableau Server status, check extract size |
| 21 | **Reporting** | Hex Notebook Failure | `Hex project run failed` | P3 | **Partially** — retry may work | Check Hex run logs, retry, check if data shape changed |
| 22 | **Monitoring** | Cascading Failure | Multiple alerts from same DAG domain within 30 min | P1 | **No** — needs root cause analysis | Correlate alerts, identify common upstream cause, present unified analysis |

### Severity Definitions

| Level | Definition | Response Time | Example |
|-------|-----------|---------------|---------|
| **P1** | Critical — data pipeline broken, downstream dashboards stale, executive reporting impacted | < 30 min | Warehouse queue blocking all transforms; SLA breach on key dataset |
| **P2** | High — specific model or domain broken, workaround may exist | < 2 hours | Single dbt model failure; source freshness warning |
| **P3** | Medium — non-critical reporting affected, auto-retry likely to resolve | < 4 hours | Tableau refresh failure; Hex notebook error |
| **P4** | Low — informational, no immediate impact | Next business day | Test warning threshold; non-production environment issue |
| **P5** | Noise — auto-resolved, known transient issue | Suppress or batch | Firing/Resolved pair within 30 min |

---

## 5. DCAG On-Call Workflow Design

### 5.1 High-Level Architecture

```
[Slack Alert]                          [DCAG Engine]
     |                                      |
     v                                      |
[Shift Listener] ----webhook/event----> [Alert Parser]
                                            |
                                            v
                                      [Alert Normalizer]
                                            |
                                            v
                                      [Dedup Check]
                                            |
                                            v
                                      [Classifier] -- knowledge YAML for alert types
                                            |
                                            v
                                      [Conditional Walker] -- branch by alert type
                                            |
                        +---+---+---+---+---+---+
                        |   |   |   |   |   |   |
                        v   v   v   v   v   v   v
                    [Model] [Fresh] [Test] [Infra] [Ingest] [Report] [Cascade]
                    [Fail]  [ness]  [Fail] [ure]   [ion]    [ing]    [ing]
                        |   |   |   |   |   |   |
                        v   v   v   v   v   v   v
                      [Enrichment] -- Snowflake MCP, dbt lineage, past incidents
                            |
                            v
                      [Triage Report]
                            |
                            v
                      [Human Gate] -- post findings to Slack thread, propose action
                            |
                        +---+---+
                        |       |
                        v       v
                  [Approve]  [Escalate]
                      |          |
                      v          v
                  [Execute]  [Notify Team]
                      |
                      v
                  [Post-Action Verify]
                      |
                      v
                  [Close / Document]
```

### 5.2 Top 10 Alert Types — Manual vs. DCAG Automated

#### Alert Type 1: dbt Model Failure — SQL Compilation Error

**What AE currently does manually (~30-45 min):**
1. Read error message in Slack alert
2. SSH or open Airflow UI, find the failed task log
3. Read the compiled SQL in the error output
4. Open the dbt model file in GitHub/IDE
5. Compare compiled SQL with source table schema (manually query `INFORMATION_SCHEMA`)
6. Identify the mismatched column or syntax issue
7. Fix the SQL, test locally, push to branch, merge, rerun DAG

**What DCAG would do automatically (~2-3 min):**
1. Parse alert: extract model name, error message, DAG name, task name
2. Classify: SQL_COMPILATION_ERROR
3. Query Snowflake MCP: `INFORMATION_SCHEMA.COLUMNS` for the source table
4. Query dbt MCP: get compiled SQL for the failing model
5. Compare: identify which column/reference in the SQL doesn't exist in the schema
6. Check: was there a recent schema change? (query `TABLE_STORAGE_METRICS` or schema change history)
7. Present to on-call: "Model `X` references column `Y` which doesn't exist in `TABLE_Z`. The column was renamed to `Y_NEW` on 2026-03-10. Suggested fix: update model SQL line 42."

**What still needs human judgment:** Deciding whether to rename the column reference, add a COALESCE with the old and new names, or refactor the model.

**Estimated time saved:** 25-40 minutes per incident.

---

#### Alert Type 2: Source Freshness Breach

**What AE currently does manually (~20-30 min):**
1. Note which source is stale from the alert
2. Query Snowflake: `SELECT MAX(loaded_at) FROM source_table`
3. Check Airflow: is the ingestion DAG running? Failed? Delayed?
4. Check upstream: is the source system (API, database) operational?
5. Determine impact: which downstream models depend on this source?
6. Notify stakeholders if SLA will be missed

**What DCAG would do automatically (~1-2 min):**
1. Parse alert: extract source name, expected freshness, actual freshness
2. Classify: SOURCE_FRESHNESS_BREACH
3. Query Snowflake MCP: `MAX(loaded_at)` from the source table
4. Query Airflow MCP: get latest DAG run status for the ingestion DAG
5. Query dbt MCP: get downstream lineage (all models depending on this source)
6. Check: is this a recurring issue? (query past alerts for same source)
7. Present to on-call: "Source `X` is 4 hours stale. Ingestion DAG `load_X` last ran at 02:00 and succeeded, but source table hasn't been updated since 22:00 yesterday. Downstream impact: 5 models, including `revenue_daily` (P1 SLA). This same source was stale 3 times last week."

**What still needs human judgment:** Whether to escalate to the upstream team, suppress the alert, or notify stakeholders.

**Estimated time saved:** 15-25 minutes per incident.

---

#### Alert Type 3: dbt Test Failure — Unique Violation

**What AE currently does manually (~45-60 min):**
1. Identify failing test and model from alert
2. Query the test result audit table to see failing rows
3. Investigate: where are the duplicates coming from?
4. Check upstream: was there a backfill, replay, or source system issue?
5. Determine: is this a data issue or a model logic issue?
6. Coordinate with DE or domain team if upstream
7. Fix: dedup query, model logic change, or wait for upstream fix

**What DCAG would do automatically (~3-5 min):**
1. Parse alert: extract test name, model name, number of failures
2. Classify: TEST_FAILURE_UNIQUE
3. Query Snowflake MCP: run the test query to get duplicate rows (dbt stores the test SQL)
4. Analyze duplicates: which key(s) are duplicated? When were they loaded? What source?
5. Query dbt lineage: trace the duplicated key back to the source model
6. Check: was there a recent backfill or replay in the source? (check Airflow task history for backfill runs)
7. Present to on-call: "Model `X` has 147 duplicate rows on key `transaction_id`. All duplicates have `loaded_at` between 01:00-02:00 today. Upstream model `stg_transactions` also shows duplicates. A backfill was running in DAG `load_transactions` from 00:30-01:30. Likely cause: backfill replay without dedup. Suggested action: run dedup on stg_transactions and full-refresh downstream."

**What still needs human judgment:** Whether to run the dedup, whether it's safe to full-refresh the downstream model, whether to wait for the backfill to complete.

**Estimated time saved:** 35-50 minutes per incident.

---

#### Alert Type 4: Warehouse Queue Timeout

**What AE currently does manually (~15-30 min):**
1. Check which warehouse is queued from the alert
2. Query `WAREHOUSE_LOAD_HISTORY` to see utilization
3. Identify what queries are running and causing congestion
4. Determine if scaling up or out is needed
5. Check if any unexpected large queries (Metaflow, ad-hoc) are consuming resources
6. Resize warehouse or kill problematic queries

**What DCAG would do automatically (~1-2 min):**
1. Parse alert: extract warehouse name, queue duration
2. Classify: INFRA_WAREHOUSE_QUEUE
3. Query Snowflake MCP: `WAREHOUSE_LOAD_HISTORY` for the affected warehouse
4. Query Snowflake MCP: active queries on the warehouse with runtime, bytes scanned, user
5. Identify: top 3 resource-consuming queries, whether any are ad-hoc or unexpected
6. Check: is this warehouse single-cluster? Is auto-scaling configured?
7. Present to on-call: "Warehouse `TRANSFORM_L` queued for 16 minutes. 3 active queries: (1) Metaflow listing_fact 45 min, 2.1TB scanned (2) dbt transform_cx 12 min (3) dbt transform_pricing 8 min. Metaflow job is 3x larger than usual. Warehouse is single-cluster. Suggested: scale to multi-cluster or contact ML team about the Metaflow job."

**What still needs human judgment:** Whether to resize (cost implication), kill a query, or coordinate with the ML team.

**Estimated time saved:** 10-25 minutes per incident.

---

#### Alert Type 5: dbt Model Failure — Timeout

**What AE currently does manually (~30-60 min):**
1. Identify the timed-out model from alert
2. Check Snowflake query history for the actual query
3. Review query profile: bytes scanned, partitions scanned vs pruned
4. Check if the model has grown (data volume increase)
5. Check warehouse sizing
6. Optimize: add clustering keys, filter pushdown, query refactoring
7. Retry with larger warehouse or optimized query

**What DCAG would do automatically (~2-3 min):**
1. Parse alert: extract model name, timeout duration
2. Classify: MODEL_FAILURE_TIMEOUT
3. Query Snowflake MCP: find the query in `QUERY_HISTORY`, get query ID
4. Query Snowflake MCP: query profile — bytes scanned, partitions scanned vs pruned, spillage
5. Compare with historical: has this model's scan size grown recently?
6. Check: is this an incremental model that should be doing less work?
7. Present to on-call: "Model `large_fact_daily` timed out after 2 hours. Query scanned 850GB (up from 200GB last week). Partition pruning: only 12% pruned (should be >90%). The model is incremental but is scanning the full table due to a missing `WHERE` clause on the incremental filter. Last successful run: 2 days ago. Suggested: fix incremental logic, add clustering key on `event_date`."

**What still needs human judgment:** Whether the incremental logic needs a full rewrite, whether to run a full-refresh first.

**Estimated time saved:** 25-50 minutes per incident.

---

#### Alert Type 6: Cascading Failures (Multiple Alerts from Same Domain)

**What AE currently does manually (~30-60 min):**
1. Notice multiple alerts firing from the same domain
2. Mentally correlate: are these related?
3. Trace common upstream dependency
4. Identify the root cause alert vs. cascading effects
5. Triage: fix root cause first, then downstream will resolve

**What DCAG would do automatically (~2-3 min):**
1. Detect pattern: 3+ alerts from related DAGs within 30 minutes
2. Classify: CASCADE_FAILURE
3. Correlate: use dbt lineage graph to identify common upstream model/source
4. Identify root cause: the earliest-firing alert that is upstream of all others
5. Suppress downstream alerts: mark as "likely cascade, root cause: [X]"
6. Present to on-call: "5 alerts detected in CX domain in the last 20 minutes. Root cause analysis: all 5 models depend on `stg_user_events`, which failed at 02:15 with a timeout error. The other 4 failures are cascading effects. Fixing `stg_user_events` should resolve all 5. [Detailed analysis of root cause follows]"

**What still needs human judgment:** Confirming the root cause analysis is correct before acting.

**Estimated time saved:** 20-40 minutes per incident (plus reduced cognitive overhead from alert noise).

---

#### Alert Type 7: Duplicate Row Error (MERGE)

**What AE currently does manually (~45-90 min):**
1. Read error: "Duplicate row detected during DML action"
2. Identify the MERGE model and its natural key
3. Query for duplicates in the source
4. Trace: why are there duplicates? Backfill? Replay? Source system bug?
5. Coordinate with DE team (this is usually upstream)
6. Wait for upstream fix, then retry

**What DCAG would do automatically (~3-5 min):**
1. Parse alert: extract model name, error details
2. Classify: MODEL_FAILURE_DUPLICATE_MERGE
3. Query Snowflake MCP: find duplicates on the merge key
4. Analyze: when were duplicates introduced (by `loaded_at` or `_etl_loaded_at`)?
5. Trace upstream: check Airflow for recent backfill runs on the source DAG
6. Check history: has this model had merge failures before?
7. Present to on-call: "Model `gpm_repeat_buyer_dim` failed with duplicate row on MERGE. 23 duplicate `participant_id` values found, all loaded between 01:00-02:00 today. Upstream DAG `load_participations` ran a backfill from 00:30. This same error occurred 3 days ago and was resolved by DE running a dedup backfill. Escalation recommended to @tao on DE team."

**What still needs human judgment:** Coordinating the dedup with DE, deciding whether to block downstream until fixed.

**Estimated time saved:** 35-70 minutes per incident.

---

#### Alert Type 8: SLA Breach

**What AE currently does manually (~20-40 min):**
1. Identify which datasets missed SLA
2. For each: check pipeline status, freshness, last successful run
3. Determine impact: which dashboards/reports/stakeholders are affected?
4. Notify affected teams
5. Determine fix timeline

**What DCAG would do automatically (~2-3 min):**
1. Parse alert: extract dataset list, SLA expectations, actual times
2. Classify: SLA_BREACH
3. For each dataset: query pipeline status, freshness, last run
4. Map impact: use dbt exposures or a configured impact map to identify affected dashboards/teams
5. Check: is the delay expected (planned maintenance, known issue)?
6. Present to on-call: "3 datasets missed SLA: `revenue_daily` (expected 06:00, still stale), `customer_metrics` (expected 07:00, last run failed at 05:30), `market_share_daily` (expected 08:00, upstream source stale). Impact: Executive dashboard (P1), CX daily report (P2), Market share tracker (P2). Root causes: `revenue_daily` blocked by warehouse queue; `customer_metrics` has a test failure; `market_share_daily` waiting on external source."

**What still needs human judgment:** Prioritizing which SLA breach to address first, stakeholder communication.

**Estimated time saved:** 15-30 minutes per incident.

---

#### Alert Type 9: Tableau/Hex Reporting Failure

**What AE currently does manually (~10-20 min):**
1. Check which workbook/notebook failed
2. Check Airflow task logs for error details
3. Usually: retry the task
4. If retry fails: check Tableau Server or Hex status, check credentials
5. If persistent: investigate data shape changes that broke the report

**What DCAG would do automatically (~1-2 min):**
1. Parse alert: extract workbook/notebook name, error
2. Classify: REPORTING_FAILURE
3. Check: has this report succeeded in the last 24 hours? (query Airflow task history)
4. If intermittent: auto-retry the Airflow task
5. If persistent: check for upstream data issues, credential expiry, or extract size limits
6. Present to on-call: "Tableau workbook `acquisition_performance_overview` failed. Error: extract timeout. Last successful refresh: yesterday at 06:00. This workbook's extract has grown 40% in the last month. Suggested: optimize the extract query or increase timeout threshold. Auto-retry initiated."

**What still needs human judgment:** Whether to optimize the extract or accept longer refresh times.

**Estimated time saved:** 8-15 minutes per incident.

---

#### Alert Type 10: Snowflake Task Failure

**What AE currently does manually (~15-30 min):**
1. Identify failed Snowflake task from alert
2. Check task history: `TASK_HISTORY` view
3. Read error message
4. Determine if transient (retry) or persistent (needs fix)
5. Check if the task is part of a larger DAG
6. Retry or escalate to DE

**What DCAG would do automatically (~1-2 min):**
1. Parse alert: extract task name, database, schema, error
2. Classify: INFRA_SNOWFLAKE_TASK
3. Query Snowflake MCP: `TASK_HISTORY` for the task — last N runs, success rate
4. Analyze: is this a new failure or recurring? What's the error pattern?
5. If transient (single failure in otherwise healthy task): auto-retry
6. If persistent: gather diagnostics and present
7. Present to on-call: "Snowflake task `TASK_EXEC_LOAD_SELLER_EVENT_DAY_LISTING_AGG` failed. Error: timeout. This task has failed 3 of the last 5 runs. Average runtime has increased from 10 min to 45 min over the past week. Likely cause: table growth without updated clustering. Recommended: add clustering key on `event_date`, or escalate to DE for optimization."

**What still needs human judgment:** Whether to modify the task definition, add clustering, or redesign the load.

**Estimated time saved:** 10-25 minutes per incident.

---

### 5.3 Estimated Total Impact

| Metric | Current (Manual) | With DCAG | Improvement |
|--------|------------------|-----------|-------------|
| Average triage time per alert | 30-45 min | 2-5 min | **85-90% reduction** |
| Time to root cause identification | 20-60 min | 1-3 min | **90-95% reduction** |
| Alerts requiring full manual investigation | ~80% | ~20% | **75% reduction** |
| On-call cognitive load | High (context switching, manual queries) | Low (findings presented, actions proposed) | **Significant reduction** |
| Cross-team escalation time | 30-60 min (finding right person, explaining context) | 5-10 min (pre-built context package) | **80% reduction** |

---

## 6. Integration Architecture

### 6.1 System Integration Map

```
                         +-----------------+
                         |   Slack (#ae-   |
                         |   alerts, #dw-  |
                         |   alerts)       |
                         +--------+--------+
                                  |
                      webhook / Shift listener
                                  |
                                  v
                    +-------------+--------------+
                    |       DCAG Engine           |
                    |  (Workflow Orchestrator)     |
                    |                             |
                    |  +--------+  +-----------+  |
                    |  | Parser |  | Classifier|  |
                    |  +--------+  +-----------+  |
                    |  +--------+  +-----------+  |
                    |  | Walker |  | Knowledge |  |
                    |  | (cond) |  | Graph     |  |
                    |  +--------+  +-----------+  |
                    +---+---+---+---+---+---+-----+
                        |   |   |   |   |   |
              +---------+   |   |   |   |   +---------+
              |             |   |   |   |             |
              v             v   |   v   v             v
        +-----------+ +------+  | +-------+ +----------+
        | Snowflake | | dbt  |  | | Air-  | | incident |
        | MCP       | | MCP  |  | | flow  | | .io API  |
        +-----------+ +------+  | | MCP   | +----------+
                                | +-------+
                                v
                          +-----------+
                          | GitHub    |
                          | CLI / API |
                          +-----------+
```

### 6.2 Integration Details

#### Slack -> DCAG (Trigger)

**Mechanism:** Shift listener monitors #ae-alerts and #dw-alerts channels for new messages from the incident.io bot.

**Alert message parsing:**
```
Input:  "Firing - HTTP - Failed Task in DAG transform_pricing__daily: dbt_build.gpm_repeat_buyer_dim_run"
Output: {
  status: "firing",
  protocol: "HTTP",
  dag: "transform_pricing__daily",
  task: "dbt_build.gpm_repeat_buyer_dim_run",
  model: "gpm_repeat_buyer_dim",
  task_type: "dbt_run",
  domain: "pricing",
  cadence: "daily"
}
```

**Dedup logic:** If a "Resolved" message arrives for the same DAG+task within 30 minutes of a "Firing" message, mark as self-resolved and skip investigation.

**Channel routing:**
- #ae-alerts: transformation-layer alerts (dbt models, tests, SLAs)
- #dw-alerts: ingestion/infrastructure-layer alerts (freshness, loads, warehouse)

---

#### DCAG -> Snowflake MCP (Diagnostics)

**Already exists.** The Snowflake MCP server can execute read-only queries for diagnostic purposes.

**Key queries the on-call workflow would execute:**

| Query | Purpose | Alert Types |
|-------|---------|-------------|
| `SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?` | Schema validation for invalid identifier errors | Model Failure — SQL Compilation |
| `SELECT MAX(loaded_at) FROM source_table` | Check actual freshness | Freshness Breach |
| `SELECT * FROM QUERY_HISTORY WHERE QUERY_TAG LIKE ?` | Find failed query details | Model Failure — Timeout, Runtime |
| `SELECT * FROM WAREHOUSE_LOAD_HISTORY WHERE WAREHOUSE_NAME = ?` | Check warehouse utilization | Infrastructure — Queue |
| `SELECT key_column, COUNT(*) FROM table GROUP BY key_column HAVING COUNT(*) > 1` | Find duplicate rows | Test Failure — Unique, Merge Error |
| `SELECT * FROM TABLE_STORAGE_METRICS WHERE TABLE_NAME = ?` | Check table growth trends | Model Failure — Timeout |
| `SELECT * FROM TASK_HISTORY WHERE NAME = ?` | Check task run history | Snowflake Task Failure |

---

#### DCAG -> dbt MCP (Lineage & Compilation)

**Exists but needs verification of capabilities.** The dbt MCP server should support:

| Operation | Purpose | Alert Types |
|-----------|---------|-------------|
| `dbt ls --select +model_name+` | Get upstream/downstream lineage | All model/test failures |
| `dbt compile --select model_name` | Get compiled SQL for analysis | Model Failure — SQL Compilation |
| `dbt test --select model_name` | Rerun specific test | Test Failures |
| `dbt source freshness --select source_name` | Check freshness | Freshness Breach |
| Parse `manifest.json` | Model metadata, dependencies, tests | All |

---

#### DCAG -> GitHub (Code Analysis & Fix PRs)

**Exists as CLI.** For the on-call assistant:

| Operation | Purpose | Risk Level |
|-----------|---------|------------|
| Read model SQL file | Understand model logic for debugging | Low (read-only) |
| Read recent commits to model | Check for recent changes that may have caused failure | Low (read-only) |
| Create fix PR | Auto-generate fix for simple issues (column rename, TRY_CAST) | **High** — requires human approval |

---

#### DCAG -> Airflow/Astronomer MCP (NEW — Needs Building)

**Does not exist yet.** This is the highest-priority new integration.

| Operation | Purpose | Priority |
|-----------|---------|----------|
| Get DAG run status | Check if DAG is running, failed, or succeeded | P0 |
| Get task instance logs | Read error details from failed task | P0 |
| Trigger DAG run | Rerun a failed DAG (with human approval) | P1 |
| Get task instance history | Check task success rate over time | P1 |
| List upstream task dependencies | Understand task ordering within DAG | P2 |

**Implementation options:**
1. **Astronomer API** — REST API for Airflow metadata and operations
2. **Airflow REST API** — Native Airflow 2.x REST API
3. **Direct Snowflake query** — If Airflow metadata is synced to Snowflake (check if this exists)

---

#### DCAG -> incident.io API (Alert Lifecycle)

**Does not exist as MCP.** incident.io is the alert source at StubHub.

| Operation | Purpose | Priority |
|-----------|---------|----------|
| Acknowledge alert | Mark alert as being investigated | P1 |
| Resolve alert | Close alert after fix confirmed | P1 |
| Get alert details | Full alert payload for parsing | P1 |
| Add note to incident | Document findings in incident record | P2 |
| Get past incidents | Find similar historical incidents | P2 |

---

#### DCAG -> Slack (Response & Coordination)

**Exists via Shift native integration.** The on-call assistant should:

| Operation | Purpose |
|-----------|---------|
| Reply in alert thread | Post investigation findings in the alert's Slack thread |
| Tag on-call engineer | Mention the current on-call AE for human-gate decisions |
| Post to domain channel | Notify affected domain team if escalation needed |
| Create summary | Post end-of-shift summary of alerts handled |

---

### 6.3 Data Flow for a Complete Alert Lifecycle

```
T+0s    incident.io posts "Firing" alert to #ae-alerts
T+1s    Shift listener detects new message, triggers DCAG workflow
T+2s    Alert Parser extracts: model=gpm_repeat_buyer_dim, type=dbt_run, dag=transform_pricing__daily
T+3s    Dedup Check: no recent duplicate, proceed
T+4s    Classifier: MODEL_FAILURE (sub-type TBD, need error details)
T+5s    Walker: query Airflow MCP for task logs -> error = "Duplicate row detected during DML"
T+8s    Reclassify: MODEL_FAILURE_DUPLICATE_MERGE (P1)
T+10s   Enrichment (parallel):
          - Snowflake MCP: find duplicates on merge key (3s)
          - dbt MCP: get upstream lineage (2s)
          - Airflow MCP: check for recent backfill runs (2s)
          - Knowledge Graph: find past incidents for this model (1s)
T+15s   Synthesis: "23 duplicates on participant_id, loaded 01:00-02:00, backfill detected"
T+16s   Triage Report generated
T+17s   Post to Slack thread: findings + recommended action + tag on-call
T+20s   On-call AE reviews, approves escalation to DE
T+21s   DCAG posts escalation to #datawarehouse with full context package
T+25s   DE acknowledges, begins dedup
T+2hr   DE completes dedup, reruns DAG
T+2h5m  "Resolved" alert fires in Slack
T+2h6m  DCAG detects resolution, closes incident, logs resolution pattern
```

**Total DCAG time: ~20 seconds** (vs. 45-90 minutes manual investigation).

---

### 6.4 Phase Plan

| Phase | Scope | Dependencies | Effort |
|-------|-------|-------------|--------|
| **Phase 1: Listen & Classify** | Slack listener, alert parser, classifier, post findings to thread | Shift listener, alert taxonomy YAML | 2 weeks |
| **Phase 2: Diagnose** | Snowflake MCP queries, dbt lineage lookup, enrichment | Snowflake MCP (exists), dbt MCP (exists) | 2 weeks |
| **Phase 3: Correlate** | Dedup, cascade detection, past incident matching | Knowledge graph, alert history store | 2 weeks |
| **Phase 4: Airflow Integration** | Airflow MCP server, task logs, DAG status, rerun capability | New MCP server build | 3 weeks |
| **Phase 5: Remediate** | Auto-retry for safe actions, human gate for risky actions, incident.io integration | incident.io API, approval workflow | 3 weeks |
| **Phase 6: Learn** | Store resolution patterns, improve classification, feedback loop | Alert history database, ML pipeline | Ongoing |

---

## 7. Sources

### AI On-Call Products
- [PagerDuty AIOps](https://www.pagerduty.com/platform/aiops/)
- [PagerDuty Spring 2026 Release](https://www.businesswire.com/news/home/20260312121276/en/PagerDuty-Unveils-Next-Generation-of-the-Operations-Cloud-Platform-with-the-Spring-2026-Release)
- [PagerDuty AIOps Quickstart Guide](https://support.pagerduty.com/main/docs/pagerduty-aiops-quickstart-guide)
- [Datadog Bits AI SRE](https://www.datadoghq.com/product/ai/bits-ai-sre/)
- [Datadog Bits AI SRE Deeper Reasoning](https://www.datadoghq.com/blog/bits-ai-sre-deeper-reasoning/)
- [Datadog Watchdog RCA](https://www.datadoghq.com/blog/datadog-watchdog-automated-root-cause-analysis/)
- [BigPanda Core Platform](https://www.bigpanda.io/our-product/bigpanda-core/)
- [BigPanda 98% Noise Reduction](https://www.bigpanda.io/blog/fast-track-video-series-slash-it-noise-by-up-to-98-with-alert-correlation-with-bigpanda/)
- [Moogsoft Probable Root Cause](https://docs.moogsoft.com/moogsoft-cloud/en/probable-root-cause-overview.html)
- [Moogsoft Alert Correlation](https://docs.moogsoft.com/moogsoft-cloud/en/correlate-alerts-into-incidents.html)
- [Shoreline Runbook Automation Guide](https://www.shoreline.io/blog/the-guide-to-automating-runbook-execution)
- [Shoreline Incident Automation](https://www.shoreline.io/blog/what-is-incident-automation)
- [Rootly Platform](https://rootly.com/)
- [Rootly AI-Agent-First API](https://www.businesswire.com/news/home/20250312871641/en/Rootly-Makes-Its-API-AI-Agent-First-to-Elevate-Incident-Management)
- [incident.io AI SRE](https://incident.io/ai-sre)
- [incident.io Multi-Agent Investigation (ZenML)](https://www.zenml.io/llmops-database/ai-powered-incident-response-system-with-multi-agent-investigation)
- [Opsgenie Alert Priority](https://support.atlassian.com/opsgenie/docs/what-are-incident-priority-levels/)
- [Opsgenie Consolidation into JSM](https://www.infoq.com/news/2025/03/atlassian-opsgenie-consolidation/)
- [Monte Carlo Observability Agents](https://www.montecarlodata.com/blog-monte-carlo-observability-agents)
- [Monte Carlo Data Quality Statistics 2025](https://www.montecarlodata.com/blog-data-quality-statistics/)
- [Anomalo Platform](https://www.anomalo.com/)
- [Anomalo 6 Pillars of Data Quality](https://www.globenewswire.com/news-release/2025/09/09/3147209/0/en/Anomalo-Unveils-the-6-Pillars-of-Data-Quality-Critical-for-AI-Success.html)
- [Elementary GitHub](https://github.com/elementary-data/elementary)
- [Elementary Documentation](https://docs.elementary-data.com/home)
- [Harness AI SRE Runbooks](https://developer.harness.io/docs/ai-sre/runbooks/)
- [Harness AI SRE Overview](https://developer.harness.io/docs/ai-sre/get-started/overview/)

### Architecture Patterns
- [Multi-Agent LLM Orchestration for Incident Response (arXiv)](https://arxiv.org/abs/2511.15755)
- [AWS Multi-Agent SRE with Bedrock AgentCore](https://aws.amazon.com/blogs/machine-learning/build-multi-agent-site-reliability-engineering-assistants-with-amazon-bedrock-agentcore/)
- [Human-in-the-Loop Architecture Patterns](https://www.agentpatterns.tech/en/architecture/human-in-the-loop-architecture)
- [OpenAI Agents SDK HITL](https://openai.github.io/openai-agents-js/guides/human-in-the-loop/)
- [Komodor: War Room of AI Agents for SRE](https://komodor.com/blog/the-war-room-of-ai-agents-why-the-future-of-ai-sre-is-multi-agent-orchestration/)
- [InfoQ: Human-Centred AI for SRE](https://www.infoq.com/news/2026/01/opsworker-ai-sre/)
- [Azure AI Agent Design Patterns](https://learn.microsoft.com/en-us/azure/architecture/ai-ml/guide/ai-agent-design-patterns)
- [incident.io Automated Runbook Guide](https://incident.io/blog/automated-runbook-guide)
- [Autonomous Incident Remediation via GenAI-Assisted Runbooks (Journal)](https://jisem-journal.com/index.php/journal/article/download/13519/6363/22894)
- [Incident Response Automation Best Practices 2025 (GetDX)](https://getdx.com/blog/incident-response-automation/)
- [5 AI-Powered Incident Management Platforms 2026 (incident.io)](https://incident.io/blog/5-best-ai-powered-incident-management-platforms-2026)
- [7 Best AI SRE Tools 2026 (Dash0)](https://www.dash0.com/comparisons/best-ai-sre-tools)

### Data Engineering On-Call
- [On-Call Rotation Best Practices (DevOps.com)](https://devops.com/on-call-rotation-best-practices-reducing-burnout-and-improving-response/)
- [Google SRE: Being On-Call](https://sre.google/sre-book/being-on-call/)
- [dbt Best Practices (dbt Labs)](https://www.getdbt.com/blog/data-engineering)
- [Handling dbt Failures in Production (Medium)](https://medium.com/the-data-movement/handling-dbt-failures-in-production-debugging-common-errors-and-failed-runs-471389486e32)
- [My dbt Test Failed — Now What? (Elementary)](https://www.elementary-data.com/post/my-dbt-test-failed-now-what)
- [dbt Debug Errors (dbt Docs)](https://docs.getdbt.com/guides/debug-errors)
- [Snowflake Task Error Notifications](https://docs.snowflake.com/en/user-guide/tasks-errors)
- [Snowflake Alerts and Notifications](https://docs.snowflake.com/en/guides-overview-alerts)
- [Snowflake Task Event Monitoring](https://docs.snowflake.com/en/user-guide/tasks-events)

### Alert Classification & ML Triage
- [Automated Incident Triage with ML (Algomox)](https://www.algomox.com/resources/blog/automated_incident_triage_categorizing_alerts_using_ml/)
- [AACT: Automated Alert Classification and Triage (arXiv)](https://arxiv.org/html/2505.09843v1)
- [Snowflake SQL Compilation Error: Invalid Identifier (DrDroid)](https://drdroid.io/stack-diagnosis/snowflake-sql-compilation-error--invalid-identifier)

---

*Research compiled 2026-03-12. This document serves as the foundation for building the DCAG-powered AE on-call assistant at StubHub.*
