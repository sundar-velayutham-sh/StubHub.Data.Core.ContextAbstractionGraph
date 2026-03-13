# AE On-Call Snowflake Analysis

**Date**: 2026-03-12
**Period**: Last 30 days (2026-02-10 to 2026-03-12)
**Source**: `DW.DATAOPS_DBT.DBT_RUN_RESULTS`, `DW.DATAOPS_DBT.ELEMENTARY_TEST_RESULTS`, `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`

---

## Executive Summary

Over the past 30 days, **650 dbt errors** occurred across **180 distinct models**. SQL compilation errors dominate (46%), followed by null/unclassified errors from source ingestion pipelines (35%), Snowflake internal errors (13%), and warehouse timeouts (6%). The top 5 models account for 220 (34%) of all errors. Additionally, Elementary detected **276 test failures** across 17+ tables, with uniqueness violations being the most common data quality issue.

Long-running query failures are dominated by two patterns: ML model training jobs (BPR event model) hitting 5-hour timeouts, and experiment analysis stored procedures (`apex_sponsored_listing_conversion_rate_agged`) consistently timing out at 3 hours. Schema changes are exclusively datamigrator housekeeping (DROP TABLE on temporary staging tables).

---

## 1. Alert Classification by Frequency

### 1.1 Error Category Distribution (dbt run results)

| Rank | Error Category | Occurrences | Distinct Models | % of Total |
|------|---------------|-------------|-----------------|------------|
| 1 | SQL_COMPILATION | 299 | 95 | 46.0% |
| 2 | OTHER (null messages, cast errors, duplicates, cancellations) | 229 | 10 | 35.2% |
| 3 | INTERNAL_ERROR (Snowflake platform) | 82 | 66 | 12.6% |
| 4 | TIMEOUT (warehouse/statement timeout) | 40 | 12 | 6.2% |

### 1.2 SQL Compilation Sub-Classification

| Sub-Category | Occurrences | Distinct Models |
|-------------|-------------|-----------------|
| INVALID_IDENTIFIER (column renamed/removed) | 127 | 28 |
| OBJECT_NOT_FOUND_OR_UNAUTHORIZED | 121 | 48 |
| SYNTAX_ERROR | 25 | 12 |
| AMBIGUOUS_COLUMN | 12 | 4 |
| COLUMN_COUNT_MISMATCH (UNION branches) | 5 | 1 |
| OTHER_SQL_COMPILATION | 9 | 3 |

### 1.3 Elementary Test Failures by Type

| Test Type | Failures | Distinct Tables |
|-----------|----------|-----------------|
| unique | 116 | 17 |
| apex_participation_matches (custom) | 35 | 1 |
| expect_row_values_to_have_data_for_every_n_datepart | 32 | 1 |
| not_null | 24 | 5 |
| expect_table_aggregation_to_equal_other_table | 14 | 2 |
| unique_combination_of_columns | 13 | 4 |
| scd_type2_timeline_continuity | 13 | 3 |
| recency | 4 | 2 |
| expect_column_sum_to_be_between | 4 | 1 |
| scd_type2_current_flag_accuracy | 3 | 1 |
| session_window_no_overlap | 3 | 1 |
| session_iterator_continuity | 3 | 1 |

### 1.4 Top Failing Models (Repeat Offenders)

These models fail across multiple distinct days, indicating persistent/systemic issues rather than one-off incidents:

| Model | Distinct Failure Days | Total Failures | Error Type |
|-------|----------------------|----------------|------------|
| `int_holdout_post_data_daily` (+ its test) | 10 | 15 | OBJECT_NOT_FOUND |
| `deployed_experiments_primary_metrics` | 7 | 8 | TIMEOUT (7,200s) |
| `transaction_activity` | 6 | 6 | TIMEOUT (7,200s) |
| `pos_pre_migration_seller_agg` | 3 | 3 | OBJECT_NOT_FOUND |

### 1.5 Batch Failure Clusters

The top 5 models by raw count (`dbo__generalledgerlinetoviagogoentitymapping`, `dbo__metroareatext`, `dbo__generalledgerline`, `archive__journal_archive`, `pdts__pdtsfirstprinciples`) each had **44 failures** concentrated in a single 11-hour window (Mar 8-9). These have **null error messages**, suggesting a source system or ingestion infrastructure outage rather than individual model issues. This is a single incident producing 220 errors.

---

## 2. Root Cause Patterns

### 2.1 OBJECT_NOT_FOUND_OR_UNAUTHORIZED (121 occurrences, 48 models)

**Root Cause**: Upstream tables were dropped, renamed, or permissions were revoked without updating downstream dbt models.

**Specific instances**:
- `DW.EXPERIMENTATION.INT_HOLDOUT_POST_DATA_DAILY` -- referenced by tests, object missing for 10+ days
- `DW.ACQUISITION.US_ZIPCODE_DIM` -- blocked `marketshare_reports__marketshare_insights` (12 failures)
- `DW.SUPPLY.PROJECT_ASH_DEAL_COMPLIANCE_FACT` and siblings -- 4 Project Ash tables missing (16 test failures)
- `DW.SUPPLY.CONSIGNEE_LISTING_DAY_AGG` -- blocked `pos_pre_migration_seller_agg`
- `DW.SUPPLY.AVAILABLE_FLIGHT_SALE_AGG` -- blocked 6 tests
- `DW.EXPERIMENT.EXP_INT_SESSION_AGG` -- blocked `exp_session_agg`

**Pattern**: These are almost always caused by schema changes (table drops/renames) in upstream schemas that downstream dbt models still reference. The affected models continue to retry on every dbt run, generating repeated alerts.

### 2.2 INVALID_IDENTIFIER (127 occurrences, 28 models)

**Root Cause**: Column-level schema changes (renames, removals) that dbt models still reference.

**Specific instances**:
- `CONVERSATIONID` removed from CS chatbot source -- broke `cs_chatbot_conversation_agg` (10 failures)
- `E.IS_TSWIFT` -- removed from marketshare external source, broke 4 marketshare models (16 failures)
- `ELIGIBLE_REVENUE`, `FLIGHT_REVENUE`, `FLIGHT_SALES`, `ELIGIBLE_SALES` -- all removed from `ad_campaign_flight_agg` source, breaking the model AND 8 associated tests (64 failures)

**Pattern**: A single column rename/drop cascades to break the model and every associated dbt test (not_null, expression_is_true, etc.), producing a multiplier effect on alert volume.

### 2.3 TIMEOUT (40 occurrences, 12 models)

**Root Cause**: Queries exceeding the 7,200-second (2-hour) warehouse timeout, typically due to expensive joins, full-table scans, or growing data volumes.

**Specific instances**:
- `deployed_experiments_primary_metrics` -- 8 failures across 7 days (chronically slow)
- `transaction_activity` -- 6 failures across 6 days (chronically slow)
- `int_holdout_post_data_daily` -- 4 timeouts

**Pattern**: These are not transient. The same models time out repeatedly, suggesting they need query optimization, materialization strategy changes, or warehouse sizing adjustments.

### 2.4 INTERNAL_ERROR (82 occurrences, 66 models)

**Root Cause**: Snowflake platform-level errors (`000603 XX000: SQL execution internal error: Processing aborted due to error 300002`).

**Pattern**: These affect a wide spread of models (66 distinct) but with low per-model counts (mostly 1-2 each). The `int_union_pos__audit__*` models (POS shard union views) are slightly more affected. These are platform incidents -- nothing the AE can fix, but they need to be identified as such to avoid wasted triage time.

### 2.5 Batch Ingestion Failures (229 occurrences, ~10 models)

**Root Cause**: The 5 `dbo__*` and `archive__*`/`pdts__*` models have null error messages and failed 44 times each in a concentrated window. This indicates a source system connectivity failure (likely SQL Server or Viago integration) rather than model-level bugs.

### 2.6 Data Quality Failures (via Elementary)

- **Uniqueness violations** (116 failures, 17 tables): Most common data quality issue. Likely caused by duplicate records in source data or incorrect incremental merge logic.
- **Recency/freshness** (4 failures, 2 tables): Source data going stale.
- **SCD Type 2 timeline continuity** (13 failures, 3 tables): Gaps in slowly-changing dimension history -- usually caused by missed loads or out-of-order processing.

### 2.7 Long-Running Query Failures

Two dominant patterns from `QUERY_HISTORY`:

| Pattern | User | Warehouse | Duration | Count |
|---------|------|-----------|----------|-------|
| ML BPR Model Training (`TrainUserToEventModel210`) | SYSTEM | SNOWPARK_OPT_WH_L | 5 hours (timeout) | 2 |
| Cosine Similarity computation | SYSTEM | SNOWPARK_OPT_WH_L | 4 hours (timeout) | 2 |
| Experiment analysis (`apex_sponsored_listing_conversion_rate_agged`) | STUBHUB_EXPERIMENTANALYSIS_PROD | EXPERIMENTANALYSIS_WH | 3 hours (timeout) | 15+ |

The experiment analysis stored procedure is by far the biggest offender, with **15+ timeout failures** in a single week, each consuming 3 hours of warehouse compute before failing.

### 2.8 Schema Changes (Last 7 Days)

All 30 schema changes were **DROP TABLE** operations by `STUBHUB_DATAMIGRATOR_PROD` in the `DW.DATAMIGRATOR` schema, dropping temporary staging tables (e.g., `POS__SALE_<uuid>`, `POS__LISTING_<uuid>`). These are routine datamigrator cleanup operations and are NOT causing downstream failures -- the temporary tables use UUID suffixes and are not referenced by dbt models.

---

## 3. Triage Steps by Error Type

### 3.1 OBJECT_NOT_FOUND / UNAUTHORIZED

1. Identify the missing object from the error message (e.g., `DW.EXPERIMENTATION.INT_HOLDOUT_POST_DATA_DAILY`)
2. Check if the object was renamed: `SHOW TABLES HISTORY IN SCHEMA <schema>` or check `QUERY_HISTORY` for recent DDL
3. Check if it's a permissions issue: `SHOW GRANTS ON <object>` vs. the dbt service account
4. If object was intentionally dropped: disable the downstream model in dbt (comment out or remove from `dbt_project.yml`)
5. If unintentional: restore from Time Travel or contact the owner who dropped it
6. **Suppress continued alerts**: Add model to a dbt `on-run-end` skip list or tag as `disabled: true`

### 3.2 INVALID_IDENTIFIER

1. Identify the missing column from the error message
2. Check if it was renamed: `SELECT * FROM <table> LIMIT 1` to see current columns
3. If renamed: update the dbt model SQL to use the new column name
4. If removed: check with the upstream owner whether it's coming back; if not, remove from model
5. **Note the cascade**: A single column change can break the model + N tests. Fix the model first, then all tests pass.

### 3.3 TIMEOUT

1. Check the query plan: `EXPLAIN <compiled SQL>` or find the `QUERY_ID` in `QUERY_HISTORY`
2. Check for partition pruning: `SELECT system$explain_plan_json(...)` -- look for full table scans
3. Common fixes:
   - Add/adjust clustering keys on large tables
   - Switch to incremental materialization
   - Add date range filters to limit scan scope
   - Upsize warehouse (temporary) while fixing the root cause
4. For chronic timeouts (`deployed_experiments_primary_metrics`, `transaction_activity`): these need engineering tickets, not just reruns

### 3.4 INTERNAL_ERROR

1. Note the Snowflake incident ID from the error message (e.g., `300002:639321540`)
2. Check [Snowflake status page](https://status.snowflake.com/) for known incidents
3. **Do not retry immediately** -- wait 15-30 minutes for the platform issue to resolve
4. If persistent: open a Snowflake support case with the incident ID
5. If affecting many models simultaneously: it's a platform issue, not a model issue

### 3.5 Batch Ingestion Failures (null messages)

1. Check the source system status (SQL Server, Viago, etc.)
2. Check Airflow DAG logs for the ingestion task
3. If source is down: wait and rerun once restored
4. If source is up but dbt gets null errors: check network connectivity, Snowflake external stage access

### 3.6 Data Quality (Elementary test failures)

1. For **uniqueness violations**: Check source data for duplicates; review incremental merge keys
2. For **not_null violations**: Check if upstream ETL started sending NULLs in a previously non-null field
3. For **recency failures**: Check if the source pipeline is running on schedule (Airflow DAG status)
4. For **SCD2 continuity failures**: Check for gaps in `valid_from`/`valid_to` ranges; often caused by out-of-order processing

---

## 4. Time Patterns

### 4.1 Peak Failure Hours (UTC)

| Hour (UTC) | Day of Week | Failures | Likely Cause |
|------------|-------------|----------|-------------|
| 21:00 | Monday (1) | 54 | Evening batch runs after weekday loads |
| 18:00 | Thursday (4) | 52 | Late-afternoon scheduled runs |
| 01:00-02:00 | Thursday (4) | 64 | Overnight batch window |
| 15:00 | Tuesday (2) | 28 | Mid-day scheduled runs |
| 18:00 | Tuesday (2) | 25 | Evening batch runs |
| 20:00-23:00 | Sunday (0) | 80 | Weekend overnight batch processing |

### 4.2 Key Observations

- **Sunday evenings/nights (UTC 20:00-23:00)** have a large failure cluster (80 failures). This correlates with the `dbo__*` batch ingestion incident on Mar 8-9 (Saturday night/Sunday in UTC).
- **Thursday** shows the heaviest non-incident failure load, concentrated in overnight (01:00-02:00) and late afternoon (18:00-19:00) windows.
- **Monday evening (21:00 UTC)** is the single highest hour -- likely where weekly refresh jobs run.
- The pattern suggests dbt runs are scheduled at roughly 6-hour intervals (overnight, morning, afternoon, evening), with failures peaking in the overnight and evening windows.

### 4.3 On-Call Impact

An AE on-call would most likely be paged during:
1. **Sunday night / Monday early morning** -- batch ingestion failures
2. **Overnight (01:00-06:00 UTC)** -- timeout failures for heavy compute models
3. **Thursday afternoon/evening** -- peak scheduled run failures

---

## 5. Owner Distribution

### 5.1 dbt Model Owners (by run result failures)

| Owner | Failing Models | Total Failures | Top Error Type |
|-------|---------------|----------------|----------------|
| @oliver.tosky | 30 | 70 | Mixed (internal errors, object not found) |
| @connor.sempek | 13 | 43 | Invalid identifier, object not found |
| @kavya.jain | 12 | 27 | Object not found, timeout |
| @alan.peters | 8 | 23 | Timeout, internal error |
| @kendall.mccormick | 5 | 13 | Invalid identifier |
| @cole.romano | 4 | 12 | Mixed |
| @corinne.smallwood | 3 | 12 | SQL compilation |
| @dustin.thomas | 6 | 8 | Mixed |
| @bennie.lopez | 5 | 7 | Mixed |
| @shruti.narula | 1 | 6 | Timeout |

### 5.2 Elementary Test Failure Owners

| Owner | Test Failures | Distinct Tables |
|-------|--------------|-----------------|
| @cole.romano | 52 | 6 |
| [] (unowned) | 36 | 2 |
| @kavya.jain | 35 | 1 |
| @dan.wedig | 34 | 5 |
| @paige.xu | 31 | 6 |
| @dustin.thomas | 22 | 4 |
| @albert.hu | 18 | 6 |
| @sara.lu | 17 | 2 |
| @shruti.narula | 15 | 1 |

### 5.3 Observations

- **@oliver.tosky** carries the heaviest model failure burden (30 models, 70 failures). Many are POS shard union views hitting internal errors and the `dbo__*` ingestion models.
- **@connor.sempek** has concentrated failures in the ad platform domain (`ad_campaign_flight_agg` and its tests).
- **@kavya.jain** has persistent timeout issues (`deployed_experiments_primary_metrics`) and object-not-found errors in experimentation.
- **36 unowned test failures** represent a governance gap -- models without owners have no one to route alerts to.
- The ad platform domain shows a pattern where a single schema change (column renames) cascaded into 60+ failures across multiple tests, all owned by one person.

---

## 6. Recommendations for DCAG Automation

### 6.1 Highest-Value Automation Targets

| Priority | Alert Type | Volume | Automation Approach | DCAG Suitability |
|----------|-----------|--------|--------------------|--------------------|
| **1** | OBJECT_NOT_FOUND | 121/month | Auto-detect missing object, check Time Travel availability, suggest restore or model disable. Cross-reference `QUERY_HISTORY` for DDL that dropped it. | **HIGH** -- deterministic triage, clear decision tree |
| **2** | INVALID_IDENTIFIER | 127/month | Compare model's expected columns vs. actual table columns (`INFORMATION_SCHEMA.COLUMNS`). Identify renames via column similarity matching. Suggest SQL fix. | **HIGH** -- column diff is deterministic |
| **3** | Cascade Deduplication | ~60% of volume | When a single root cause (e.g., missing table) triggers N test failures, group them into one alert with root cause and suppress duplicates. | **HIGHEST** -- biggest noise reduction |
| **4** | TIMEOUT (chronic) | 40/month | Track per-model execution time trends. When a model starts timing out repeatedly, auto-generate an optimization report (clustering, partitioning, materialization strategy). | **MEDIUM** -- detection is easy, remediation requires judgment |
| **5** | INTERNAL_ERROR | 82/month | Auto-classify as platform incident. Check Snowflake status. Suppress alert if known incident. Auto-rerun after 30 minutes. | **HIGH** -- pure automation, no human judgment needed |
| **6** | Batch Ingestion Failures | 220/incident | Detect null-message cluster, check source system health, create a single incident instead of N alerts. | **HIGH** -- pattern detection + alert grouping |
| **7** | Uniqueness Violations | 116/month | Auto-query for duplicate rows, identify the duplicate keys, trace to source ingestion job. | **MEDIUM** -- detection is easy, root cause varies |
| **8** | Stale Data / Recency | Low (4/month) | Auto-check Airflow DAG status for the source pipeline. If DAG failed, link to Airflow logs. | **HIGH** -- simple cross-system correlation |

### 6.2 Alert Noise Reduction Opportunity

The single biggest win is **cascade deduplication**. Currently, when `ad_campaign_flight_agg` fails because columns were renamed:
- The model error fires: 1 alert
- 8 associated tests fire: 8 alerts
- Each dbt run retries: multiplied by N runs

A DCAG agent that understands the dbt DAG can collapse these 64+ alerts into **1 root cause alert**: "Column `ELIGIBLE_REVENUE` removed from source. Affects `ad_campaign_flight_agg` + 8 tests. Owner: @connor.sempek."

This single capability would reduce alert volume by an estimated **40-50%**.

### 6.3 Runbook Knowledge to Encode

For each error type, DCAG should have encoded knowledge of:

1. **Diagnostic queries** -- what to run in Snowflake to triage (e.g., `SHOW TABLES HISTORY`, `INFORMATION_SCHEMA.COLUMNS`)
2. **Cross-system checks** -- Airflow DAG status, Snowflake status page, source system health
3. **Owner routing** -- who owns the model (from `DBT_MODELS.OWNER`), who to escalate to
4. **Resolution patterns** -- for each error type, the 2-3 most common resolutions
5. **Suppression rules** -- when an alert is a duplicate, a known platform issue, or a non-actionable retry

### 6.4 Data Sources DCAG Should Integrate

| Source | Table/API | Use |
|--------|-----------|-----|
| dbt run results | `DW.DATAOPS_DBT.DBT_RUN_RESULTS` | Error detection, model status |
| dbt model metadata | `DW.DATAOPS_DBT.DBT_MODELS` | Owner lookup, dependency graph |
| Elementary test results | `DW.DATAOPS_DBT.ELEMENTARY_TEST_RESULTS` | Data quality monitoring |
| Snowflake query history | `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` | DDL tracking, timeout analysis |
| Snowflake schema | `INFORMATION_SCHEMA.COLUMNS` / `TABLES` | Schema drift detection |
| Airflow metadata | `DW.STAGE.STG_AIRFLOW_METADATA__DAG_RUNS` | Pipeline run status |
| Airflow task instances | `DW.STAGE.STG_AIRFLOW_METADATA__TASK_INSTANCES` | Task-level failure details |

### 6.5 Experiment Analysis Stored Procedures

The `apex_sponsored_listing_conversion_rate_agged` stored procedure is a standout problem -- **15+ timeout failures in 7 days**, each burning 3 hours of L-sized warehouse compute. This is not a dbt model issue but a Snowpark/stored procedure optimization issue. DCAG could:
- Detect repeated stored procedure timeouts
- Auto-create a ticket with execution stats
- Suggest warehouse scaling or query plan analysis

### 6.6 Estimated Impact

| Metric | Current State | With DCAG |
|--------|--------------|-----------|
| Monthly dbt errors | 650 | 650 (same occurrence) |
| Alerts requiring human triage | ~650 | ~80-120 (after dedup + auto-classify) |
| Mean time to root cause | 15-30 min | 2-5 min (pre-diagnosed) |
| Repeat offender resolution | Days/weeks | Hours (auto-tracked, auto-escalated) |
| Unowned failures | 36+ | 0 (auto-route to team lead) |

---

## Appendix A: Long-Running Query Failure Details

| Query Type | User | Warehouse | Duration (sec) | Description |
|------------|------|-----------|----------------|-------------|
| CALL | SYSTEM | SNOWPARK_OPT_WH_L | 18,000 | ML BPR Event Model Training (2x) |
| CTAS | SYSTEM | SNOWPARK_OPT_WH_L | 14,127 | Cosine similarity computation |
| CALL | SYSTEM | SNOWPARK_OPT_WH_L | 14,401 | SimilarUsersFromEmbeddings |
| CALL | EXPERIMENTANALYSIS_PROD | EXPERIMENTANALYSIS_WH | 10,801 | apex_sponsored_listing_conversion_rate_agged (15x) |

## Appendix B: Schema Change Activity (Last 7 Days)

All 30 operations were `DROP TABLE` by `STUBHUB_DATAMIGRATOR_PROD` on temporary tables in `DW.DATAMIGRATOR`. Tables dropped follow the pattern `<ENTITY>__<TABLE>_<uuid>` (e.g., `POS__SALE_d254574be6134bffa01b874b86be653f`). These are routine cleanup of completed migration batches and do not cause downstream failures.

## Appendix C: Elementary Observability

Only one Elementary table exists in `DATAOPS_DBT`: `ELEMENTARY_TEST_RESULTS`. This table includes:
- Test ownership (`OWNERS` field with @-handles)
- Tag-based categorization (e.g., `["dataops", "subhourly"]`)
- Detailed test result queries for debugging
- Model-level unique IDs for DAG tracing
