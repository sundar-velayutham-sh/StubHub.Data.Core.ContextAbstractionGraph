# Analytics Engineering Alert Channels - Slack Analysis

**Date:** 2026-03-12
**Methodology:** Paginated through 800+ Slack channels using `slack_list_channels` API with cursor pagination. Pulled 50 most recent messages from each identified alert channel.

---

## Summary of Channels Found

### Primary AE Alert Channels

| Channel Name | Channel ID | Members | Status | Notes |
|---|---|---|---|---|
| **#ae-alerts** | `C0590MFQN1W` | 51 | Active (high volume) | Primary AE on-call alert channel. Automated Airflow/DAG alerts + manual triage. On-call rotation tracked via topic. |
| **#dw-alerts** | `C040SRYF9HS` | 47 | Active (high volume) | Data warehouse ingestion + freshness alerts. Heavily automated. |
| **#dw-alerts-dev** | `C04SCBU172L` | 2 | Dormant | Dev environment alerts for DW team. |
| **#ask-ae** | `C04B34RN0BV` | 183 | Active | AE support channel (previously named `ae-biz`, then `dw-adhoc-data-requests`). Not an alert channel -- catch-all for data questions. |
| **#ae-catalog** | `C05ETRR4EGL` | 45 | Active | AE x Catalog cross-team channel. Not alerts -- communication/coordination. |
| **#supply-data** | `C05CL75V5K4` | 85 | Active | Previously named `ae-supply`. Supply-focused data questions and support. |
| **#datawarehouse** | `C03KF2GC04A` | 282 | Active | General DW team channel with on-call rotation. |
| **#datawarehouse-announcements** | `C051MR9H2J2` | 147 | Active | Read-only announcements for DW changes. |

### Specific Channels Referenced in dbt META (Search Results)

| Channel Name | Found? | Notes |
|---|---|---|
| `#ae-supply-alerts` | **NOT FOUND** | Does not exist as a public channel. May be a planned channel, or the META config references a private channel or future channel. |
| `#ae-core-alerts` | **NOT FOUND** | Does not exist as a public channel. |
| `#ae-pricing-alerts` | **NOT FOUND** | Does not exist as a public channel. |
| `#ae-ops-alerts` | **NOT FOUND** | Does not exist as a public channel. |
| `#ae-fintech-alerts` | **NOT FOUND** | Does not exist as a public channel. |
| `#ae-experimentation-alerts` | **NOT FOUND** | Does not exist as a public channel. |
| Any "elementary" channel | **NOT FOUND** | No channel with "elementary" in the name exists. Elementary (dbt observability tool) is not sending to a dedicated channel. |
| Any "freshness" channel | **NOT FOUND** | No dedicated freshness channel. Freshness alerts go to `#dw-alerts`. |

### Other Alert Channels (Not AE but Relevant)

| Channel Name | Channel ID | Members | Notes |
|---|---|---|---|
| **#database-alerts** | `C03L51PP98R` | 55 | SQL Server / database team alerts |
| **#database-supply-alerts** | `C03UKKX03U0` | 6 | Database-level supply service alerts (dormant) |
| **#database-failover-alerts** | `C03TVPVQJDR` | 8 | Database failover alerts |
| **#finance-subledger-alerts** | `C04BLJ9L0MN` | 29 | Fintech subledger alerts |
| **#experimentation-platform-alerts** | `C055PLTBK0W` | 4 | Experiment platform alerts |
| **#catalog-team-alerts** | `C058WP6AMKL` | 18 | Catalog service alerts |
| **#acq-operational-excellence-alerts** | `C05EZNK3LHE` | 14 | Acquisition operational alerts |
| **#onsale-alerts** | `C04MHKJK27R` | 26 | On-sale event alerts |
| **#customer-success-tech-alerts** | `C03F5T2MPAB` | 19 | CS tech alerts |
| **#crm-alerts** | `C03F6SFV0AF` | 19 | CRM alerts |
| **#ppc-alerts** | `C03ET4MKUA2` | 18 | Paid search alerts |
| **#native-alerts** | `C03FNH5FX5X` | 21 | Native mobile app alerts |
| **#platform-alerts** | `C03FA0NFVDF` | 24 | Platform/infra PagerDuty alerts |
| **#supply-alerts** | `C03FZRD5CH3` | 8 | Supply service alerts |
| **#cx-compliance-alerts** | `C03SL7BL857` | 26 | CX web compliance/monitoring alerts |
| **#cx-web-alerts-minor** | `C03F9TBQN2E` | 42 | Website minor alerts |
| **#payment-fraud-alerts** | `C03HXE3N2SC` | 2 | Payment fraud alerts |
| **#it-infrastructure-alerts** | `C03FPLNPGHJ` | 10 | IT infra alerts |
| **#supply-alerts-pos** | `C059CKUJLJ2` | 17 | Point of Sale service alerts |
| **#supply-alerts-pos-db** | `C054DNP44BC` | 17 | POS database alerts |

---

## Channel Deep Dive: #ae-alerts (C0590MFQN1W)

### Overview
- **Members:** 51
- **On-call rotation:** Tracked via channel topic (currently: `@U077TGDL8TH`)
- **Alert source:** Primarily automated via `incident.io` (user `U03EA12AEDP`) -- this is a bot that posts Airflow DAG failure firing/resolved alerts
- **Volume:** ~50 messages in the last 7 days (March 6-12, 2026)

### Sample Messages (10 Most Recent)

1. **2026-03-12 15:21** -- `Resolved - HTTP - Failed Task in DAG transform_optimal_external__hourly: transform_optimal`
2. **2026-03-12 14:59** -- `Resolved - HTTP - Failed Task in DAG transform_optimal_external__hourly: transform_optimal`
3. **2026-03-12 09:38** -- (Human) Swap of on-call weeks between team members
4. **2026-03-12 07:09** -- (Human) Thank you for covering while sick
5. **2026-03-12 06:43** -- `Resolved - HTTP - Failed Task in DAG refresh_tableau_workbook__acquisition_performance_overview: dbt_test`
6. **2026-03-12 06:14** -- `Resolved - HTTP - Failed Task in DAG transform_cx__hourly: dbt_build.search_fact_run`
7. **2026-03-12 02:53** -- `Resolved - HTTP - Failed Task in DAG transform_cx__daily: cx_pipeline_build.exp_web_vitals.test`
8. **2026-03-12 02:35** -- `Resolved - HTTP - Failed Task in DAG monitor__acquisition_sources_tests: test_acquisition_sources`
9. **2026-03-12 00:52** -- `Firing - HTTP - Failed Task in DAG transform_pricing__daily: dbt_build.gpm_repeat_buyer_dim_run`
10. **2026-03-11 21:54** -- (Human) FYI about Google report download problems, potential impact on Acq exec reporting and VMC

### Alert Type Classification (from 50 messages, March 6-12)

| Alert Type | Count | Example DAGs/Tasks |
|---|---|---|
| **Airflow DAG task failure (Firing)** | ~12 | `transform_pricing__daily`, `transform_cx__hourly`, `transform_marketshare__daily`, `transform_acquisition__hourly`, `publish_metaflow_event__*`, `monitor__severe_tests__hourly`, `transform_pos__hourly` |
| **Airflow DAG task failure (Resolved)** | ~30 | Same DAGs as above -- most alerts auto-resolve on retry |
| **dbt test failure** | ~8 | `cx_pipeline_build.exp_web_vitals.test`, `cx_pipeline_build.user_lifetime_metrics.test`, `cx_pipeline_build.notification_center_fact.test`, `cx_pipeline_build.int_user_lifetime_attributes.test` |
| **Snowflake task failure** | ~2 | `DW.CORE.TASK_EXEC_LOAD_SELLER_EVENT_DAY_LISTING_AGG` |
| **SLA breach** | ~2 | `IMPORTANT DATASET SLAS MISSED` |
| **Hex app notification** | ~1 | Scheduled Hex app reporting unresolved alert count |
| **Tableau refresh failure** | ~2 | `refresh_tableau_workbook__acquisition_performance_overview` |
| **Human triage/discussion** | ~5 | On-call swaps, incident investigation, Metaflow job issues |

### Key DAG Domains in ae-alerts

- **transform_cx__hourly / daily** -- CX (customer experience) data transforms
- **transform_pricing__hourly / daily** -- Pricing data transforms
- **transform_acquisition__hourly** -- Acquisition/marketing data transforms
- **transform_ops__daily** -- Operations data (CS dashboards, NPS, contact rate)
- **transform_marketshare__daily / hourly** -- Market share analysis
- **transform_strategic_finance** -- Strategic finance reporting
- **transform_pos__hourly** -- Point of Sale data
- **transform_external_rpt__subhourly** -- External reporting
- **transform_optimal_external__hourly** -- Optimal/external data
- **monitor__acquisition_sources_tests** -- Acquisition source testing
- **monitor__severe_tests__hourly** -- Severe test monitoring
- **publish_metaflow_event__*** -- Metaflow event publishing (listing_fact, transaction_fact, event, cs_chat_insights)
- **refresh_tableau_workbook__*** -- Tableau workbook refresh

---

## Channel Deep Dive: #dw-alerts (C040SRYF9HS)

### Overview
- **Members:** 47
- **Alert sources:** Automated via incident.io bot + dbt freshness checks
- **Volume:** ~50 messages in 2 days (March 11-12, 2026) -- very high volume
- **Primary alert type:** Source freshness warnings (dominant)

### Sample Messages (10 Most Recent)

1. **2026-03-12 22:17** -- `Warning: Freshness exceeded the acceptable times on source "ecomm.application.experimentation__participation"`
2. **2026-03-12 21:46** -- Same freshness warning (repeated every ~30 min)
3. **2026-03-12 21:17** -- Same freshness warning
4. **2026-03-12 18:45** -- Same freshness warning
5. **2026-03-12 17:16** -- Same freshness warning
6. **2026-03-12 11:16** -- Same freshness warning
7. **2026-03-12 10:17** -- `Resolved - HTTP - Flow CitibikeInferenceFlow has succeeded.`
8. **2026-03-12 09:29** -- `Firing - HTTP - ANALYTICS_XL has been queued for at least 985 seconds.`
9. **2026-03-12 08:46** -- Same freshness warning
10. **2026-03-12 08:34** -- `Firing - HTTP - Data Migrator Load Failure - PdtsAdjustmentMappingDailyOutputMigration`

### Alert Type Classification (from 50 messages, March 11-12)

| Alert Type | Count | Notes |
|---|---|---|
| **Source freshness warning** | ~30 | Dominated by `experimentation__participation` source -- firing every 30 min. dbt `source freshness` check. |
| **Warehouse queue alert** | ~2 | `ANALYTICS_XL has been queued for at least 985 seconds`, `TRANSFORM_L has been queued for at least 953 seconds` |
| **Data Migrator load failure** | ~2 | `PdtsAdjustmentMappingDailyOutputMigration` |
| **Reconciliation error** | ~2 | `Row diffs detected`, `Hard deletes detected` |
| **Snowflake task failure** | ~2 | `DW.DATAOPS.TASK_EXEC_UNIFIED_ALERT_ON_TASK_FAILURE` |
| **Snowflake deployment failure** | ~1 | `Snowflake Deployment Failure` |
| **DLT load failure** | ~2 | `load_dlt__salesforce__email_messages`, `load_app_store_connect__daily` |
| **Metaflow flow status** | ~1 | `CitibikeInferenceFlow has succeeded` |
| **DAG task failure (Firing/Resolved)** | ~5 | Various ingestion DAGs |

---

## Key Findings

### 1. Centralized Alert Routing -- All AE Alerts Go to #ae-alerts
There are **NO domain-specific ae- alert channels** (e.g., `#ae-supply-alerts`, `#ae-core-alerts`, `#ae-pricing-alerts`). All analytics engineering alerts from all domains (CX, pricing, acquisition, ops, supply, finance, etc.) are routed to a single `#ae-alerts` channel. If dbt META configs reference `#ae-supply-alerts` or similar, those channels do not currently exist as public Slack channels -- they may be:
- Planned but not yet created
- Private channels not visible to the API
- Aspirational configurations that haven't been implemented

### 2. Two-Tier Alert Architecture
- **#ae-alerts** -- Airflow DAG failures, dbt test failures, Snowflake task failures, SLA breaches, Tableau refresh failures. These are **transformation-layer** alerts.
- **#dw-alerts** -- Source freshness warnings, data migrator failures, reconciliation errors, warehouse queue alerts, Snowflake deployment failures, DLT load failures. These are **ingestion/infrastructure-layer** alerts.

### 3. Alert Noise Problem
- `#dw-alerts` is extremely noisy -- the `experimentation__participation` source freshness warning fires every ~30 minutes and accounts for 60%+ of all messages
- `#ae-alerts` has better signal-to-noise ratio but still has many auto-resolved alerts (Firing then Resolved pairs)

### 4. On-Call Rotation
- `#ae-alerts` has an active on-call rotation tracked via the channel topic
- On-call engineers actively triage alerts and communicate via the channel
- There is a Hex app that runs scheduled checks and reports unresolved alert counts

### 5. No Elementary Integration Found
- No channel named "elementary" exists
- Elementary (dbt observability tool) does not appear to have a dedicated Slack integration
- dbt test failures are routed through Airflow alerting, not Elementary directly

### 6. Alert Sources
- **incident.io** bot (`U03EA12AEDP`) -- primary automated alert source for both channels
- **Hex scheduled apps** -- supplementary monitoring
- **Snowflake native task alerting** -- some alerts come directly from Snowflake tasks
- **dbt source freshness** -- freshness checks fire directly to `#dw-alerts`

---

## Recommendations for DCAG Context

For the ContextAbstractionGraph alert routing configuration:

1. **Current state:** Route all AE alerts to `#ae-alerts` (C0590MFQN1W)
2. **DW ingestion alerts:** Route to `#dw-alerts` (C040SRYF9HS)
3. **Domain-specific channels (ae-supply-alerts, ae-core-alerts, etc.):** These do NOT exist yet. If the DCAG system needs domain-specific routing, the channels would need to be created first.
4. **Alert deduplication:** Consider the Firing/Resolved pattern -- most alerts auto-resolve on retry, creating noise.

---

*Analysis performed by scanning all 800+ public Slack channels via API pagination and pulling 50 messages from each identified alert channel.*
