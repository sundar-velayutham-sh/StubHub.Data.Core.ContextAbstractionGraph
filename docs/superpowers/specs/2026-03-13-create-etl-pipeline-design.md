# Create ETL Pipeline — Design Spec

**Date:** 2026-03-13
**Status:** Approved
**Workflow:** `create-etl-pipeline`
**Persona:** `data_engineer`

## Summary

A single DCAG workflow that takes a one-sentence request from a data practitioner and builds a complete, validated ETL pipeline — from source discovery through PR creation. The user interacts at 3 checkpoints; everything else is autonomous.

## Motivation

Building an ETL pipeline at StubHub today involves: discovering source schema, checking what dbt models already exist, copying a similar pipeline's structure, adapting SQL, writing tests, adding schema.yml, configuring tags for the DAG, and creating a PR. This takes hours of manual work across Snowflake, GitHub, and dbt.

DCAG automates this by acting like a senior DE who knows the entire codebase — every existing pipeline, every convention, every test pattern. It discovers metadata from Snowflake (`DBT_MODELS`, `DBT_MODEL_TABLE_LINEAGE`, `INFORMATION_SCHEMA`, `DBT_TESTS`), reads real SQL from the Astronomer repo as reference, profiles source data, generates models, validates them, and delivers a PR.

### Real Pipeline Patterns (from production analysis)

| Pattern | Example | Structure |
|---------|---------|-----------|
| **hourly_rollup** | campaign_day_agg | src → stg_hour → stg_day → mart (view) |
| **multi_source_union** | marketing_spend_day_country_agg | src (multi-channel) → int → country_agg → rollup |
| **cdc_staging** | transaction_fact → rpt.transaction | src → stg → int → core_fact → rpt (denormalized) |
| **scd_star** | ad_day_fact → ad_day_agg | src → dim → status_history → fact → agg |

These were traced from real StubHub production using `DBT_MODEL_TABLE_LINEAGE` (573M rows of recursive lineage).

## Design

### Inputs — Minimal by Design

```yaml
inputs:
  request_text:
    type: string
    required: true
    description: "Natural language request from user (the Slack message)"

  sql_text:
    type: string
    required: false
    description: "Raw SQL to productionize (pasted from Hex/worksheet)"

  reference_model:
    type: string
    required: false
    description: "Existing model name to use as template (can also be inferred from request_text)"

  channel_id:
    type: string
    required: false
  thread_ts:
    type: string
    required: false
```

One required field: the user's message. Everything else is discovered.

### UX Flow — 3 Checkpoints

```
┌──────────────────────────────────────────────────────┐
│  USER INPUT (1 sentence in Slack)                    │
│  "Build a pipeline for tiktok_ads from Fivetran"     │
└──────────────────────┬───────────────────────────────┘
                       ▼
┌──────────────────────────────────────────────────────┐
│  PHASE 1: SILENT DISCOVERY (no user interaction)     │
│  classify_intent → discovery branch → profile source │
│  → discover reference patterns → design pipeline     │
└──────────────────────┬───────────────────────────────┘
                       ▼
│  CHECKPOINT 1: "Here's my plan — approve or adjust?" │
                       ▼
┌──────────────────────────────────────────────────────┐
│  PHASE 2: SILENT GENERATION (no user interaction)    │
│  generate models (loop) → validate → recommend tests │
└──────────────────────┬───────────────────────────────┘
                       ▼
│  CHECKPOINT 2: "Here's the SQL — review or edit?"    │
│  ↺ apply_changes loop if edits requested             │
                       ▼
┌──────────────────────────────────────────────────────┐
│  PHASE 3: DELIVER                                    │
│  create PR → recommend orchestration                 │
└──────────────────────┬───────────────────────────────┘
                       ▼
│  CHECKPOINT 3: "PR created — anything else?"         │
└──────────────────────────────────────────────────────┘
```

---

### Step 0: `setup_environment`

**Mode:** `execute`
**Type:** `script`

Validates tool availability before starting. Sets capability flags for degraded mode.

```yaml
- id: setup_environment
  mode: execute
  type: script
  script: |
    # Validate GitHub CLI access
    github_available=$(gh auth status 2>&1 && echo "true" || echo "false")
    # Validate Snowflake MCP access
    snowflake_available=$(echo "SELECT 1" | snowflake_mcp 2>&1 && echo "true" || echo "false")
  output_schema:
    type: object
    required: [github_available, snowflake_available]
    properties:
      github_available: { type: boolean }
      snowflake_available: { type: boolean }
```

**Degraded mode:** If `github_available` is false, `discover_reference_patterns` falls back to reading compiled SQL from `DBT_RUN_RESULTS.compiled_code` instead of raw Jinja from GitHub. This loses the `ref()` and config block information but still provides working SQL patterns. Steps that use `github_cli` include `DEGRADED MODE` instructions for this fallback.

---

### Step 1: `classify_intent`

**Mode:** `reason`
**Tools:** none (pure LLM parsing)
**Knowledge:** `naming_conventions`

Parses the user's request into one of 4 entry points:

| Entry Point | Trigger | Next Step |
|---|---|---|
| `new_source` | "Build a pipeline for fivetran_database.tiktok_ads.campaign_report" | `discover_source_schema` |
| `similar_to` | "Build something like campaign_day_agg but for affiliate traffic" | `trace_reference_pipeline` |
| `sql_to_pipeline` | "Make this a proper pipeline: [SQL]" | `parse_sql_sources` |
| `extend_existing` | "Add TikTok as a new channel to marketing_spend" | `analyze_target_pipeline` |

**Output schema:**

```yaml
type: object
required: [entry_point, source_hints, domain_hint]
properties:
  entry_point:
    type: string
    enum: [new_source, similar_to, sql_to_pipeline, extend_existing]
  source_hints:
    type: array
    items: { type: string }
  reference_model_hint:
    type: string
  domain_hint:
    type: string
  sql_text:
    type: string
```

**Transitions:**

```yaml
transitions:
  - when: "output.entry_point == 'new_source'"
    goto: discover_source_schema
  - when: "output.entry_point == 'similar_to'"
    goto: trace_reference_pipeline
  - when: "output.entry_point == 'sql_to_pipeline'"
    goto: parse_sql_sources
  - when: "output.entry_point == 'extend_existing'"
    goto: analyze_target_pipeline
```

**Budget:** `max_llm_turns: 2`, `max_tokens: 4000`

---

### Steps 2a-2d: Discovery Branches

All 4 branches produce the same output shape so `profile_source_data` and `design_pipeline` can consume any of them uniformly.

**Common output schema:**

```yaml
type: object
required: [source_tables, existing_models, reference_pipeline, conventions]
properties:
  source_tables:
    type: array
    items:
      type: object
      properties:
        table_fqn: { type: string }
        columns: { type: array }
        row_count: { type: integer }
        daily_volume: { type: integer }
        freshness_hours: { type: number }
        pk_candidates: { type: array }
  existing_models:
    type: array
    items: { type: string }
  reference_pipeline:
    type: object
    properties:
      root_model: { type: string }
      layers: { type: array }
      materializations: { type: array }
      tests: { type: array }
      grain: { type: string }
  conventions:
    type: object
    properties:
      naming_pattern: { type: string }
      schema: { type: string }
      directory: { type: string }
```

#### Step 2a: `discover_source_schema` (for `new_source`)

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`, `snowflake_mcp.describe_table`

1. Query `INFORMATION_SCHEMA.COLUMNS` for source table schema
2. Query `DBT_MODELS` to check if staging models already exist for this source
3. Query `DBT_MODEL_TABLE_LINEAGE` to find similar pipelines in the same domain
4. Query `DBT_MODELS` for naming/materialization conventions in the target schema

**Budget:** `max_llm_turns: 6`, `max_tokens: 10000`
**Transitions:** `default: profile_source_data`

#### Step 2b: `trace_reference_pipeline` (for `similar_to`)

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`

1. Trace reference model's full lineage via `DBT_MODEL_TABLE_LINEAGE` (level, nodes, paths)
2. Get model details for each node from `DBT_MODELS`: materialization, tests, tags, depends_on_nodes
3. Get test patterns from `DBT_TESTS` for each model in the lineage
4. Identify the new source the user wants to use (from `source_hints`)

**Budget:** `max_llm_turns: 6`, `max_tokens: 10000`
**Transitions:** `default: profile_source_data`

#### Step 2c: `parse_sql_sources` (for `sql_to_pipeline`)

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`, `snowflake_mcp.describe_table`

1. Parse the user's SQL to extract FROM/JOIN table references
2. For each referenced table: check if a dbt model exists in `DBT_MODELS`
3. Describe tables that don't have dbt models yet (these need staging)
4. Identify CTE structure → map to proposed layer decomposition
5. Check domain conventions for models that DO exist

**Budget:** `max_llm_turns: 6`, `max_tokens: 10000`
**Transitions:** `default: profile_source_data`

#### Step 2d: `analyze_target_pipeline` (for `extend_existing`)

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`, `github_cli.read_file`

1. Read the target model's SQL from GitHub to understand its structure
2. Trace its lineage to understand the full pipeline
3. Identify where the new source fits (UNION ALL block, new JOIN, new CTE)
4. Get the target model's test patterns from `DBT_TESTS`

**Budget:** `max_llm_turns: 6`, `max_tokens: 10000`
**Transitions:** `default: profile_source_data`

---

### Step 3: `profile_source_data`

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`

Profiles every source table identified in discovery. Runs 6 checks per table:

| Check | What It Tells Us |
|---|---|
| **Shape** — row count, column count, date range | Volume → drives materialization choice |
| **PK candidates** — columns with unique or near-unique cardinality | Grain → drives unique key for incremental |
| **Null rates** — per column, flagging >5% | Quality issues → surfaces warnings before building |
| **Daily volume** — rows per day over last 7 days | Cadence → hourly vs daily materialization |
| **Freshness** — MAX(loaded_at) or MAX(created_at) vs now | Health → don't build on a dead source |
| **Duplicates** — check candidate PKs for dupes | Integrity → need dedup in staging? |

**Output schema:**

```yaml
type: object
required: [profiles, warnings]
properties:
  profiles:
    type: array
    items:
      type: object
      required: [table_fqn, row_count, date_range, daily_avg_rows, freshness_hours, pk_candidate, null_flags]
      properties:
        table_fqn: { type: string }
        row_count: { type: integer }
        date_range: { type: object, properties: { min: { type: string }, max: { type: string } } }
        daily_avg_rows: { type: integer }
        freshness_hours: { type: number }
        pk_candidate: { type: object, properties: { columns: { type: array }, is_unique: { type: boolean } } }
        null_flags: { type: array, items: { column: { type: string }, null_pct: { type: number } } }
        has_duplicates: { type: boolean }
  warnings:
    type: array
    items:
      type: object
      properties:
        table_fqn: { type: string }
        warning_type: { type: string, enum: [stale_source, high_nulls, no_pk_candidate, duplicates_detected, low_volume] }
        detail: { type: string }
```

**Budget:** `max_llm_turns: 6`, `max_tokens: 10000`
**Transitions:** `default: discover_reference_patterns`

---

### Step 4: `discover_reference_patterns`

**Mode:** `reason`
**Tools:** `github_cli.read_file`, `github_cli.search_code`, `snowflake_mcp.execute_query`

Reads **real SQL from the Astronomer repo** — no hardcoded patterns. The LLM sees actual production code as the template.

**What it does:**

1. From discovery, identify the reference pipeline (explicit or inferred from domain)
2. For each model in the reference pipeline's lineage:
   - `github_cli.read_file` → raw model SQL (Jinja, ref(), config block)
   - `github_cli.read_file` → schema.yml (tests, descriptions)
   - `snowflake_mcp.execute_query` → `compiled_code` from `DBT_RUN_RESULTS` (resolved SQL)
3. If no explicit reference: find 2-3 models in same domain from `DBT_MODELS`, read their SQL

**Output schema:**

```yaml
type: object
required: [reference_examples]
properties:
  reference_examples:
    type: array
    items:
      type: object
      required: [model_name, layer, raw_sql, config_block, original_path]
      properties:
        model_name: { type: string }
        layer: { type: string }
        raw_sql: { type: string }
        compiled_sql: { type: string }
        config_block: { type: string }
        schema_yml: { type: string }
        tests: { type: array }
        original_path: { type: string }
  domain_conventions:
    type: object
    properties:
      common_config_blocks: { type: array }
      naming_pattern: { type: string }
      typical_materialization: { type: string }
      directory_structure: { type: string }
```

**Budget:** `max_llm_turns: 8`, `max_tokens: 15000`
**Transitions:** `default: design_pipeline`

---

### Step 5: `design_pipeline`

**Mode:** `reason`
**Tools:** none (pure reasoning)
**Knowledge:** `naming_conventions`, `load_frequency_heuristics`, `dbt_project_structure`

Convergence point — takes all discovery, profiling, and reference patterns to produce a concrete pipeline architecture.

**Context (dynamic):**
- `classify_intent` → entry_point, source_hints, domain_hint
- Discovery step (whichever ran) → source_tables, existing_models, reference_pipeline, conventions
- `profile_source_data` → profiles, warnings
- `discover_reference_patterns` → reference_examples, domain_conventions

**What it decides:**

| Decision | Informed By |
|---|---|
| Pipeline pattern | Reference pipeline structure, source count, domain conventions |
| Number of layers | Complexity of transforms, whether intermediate is needed |
| Model names | Domain naming conventions from reference examples |
| Materialization per layer | Daily volume from profiling, reference pipeline configs |
| Incremental strategy | PK candidates from profiling, reference config blocks |
| Schema placement | Domain conventions from existing models |
| Which DAG / tags | `DBT_INVOCATIONS.job_name` patterns for the domain |
| What already exists | existing_models from discovery — skip layers already built |

**Output schema:**

```yaml
type: object
required: [pipeline_pattern, models, dag_recommendation]
properties:
  pipeline_pattern:
    type: string
    enum: [hourly_rollup, scd_star, multi_source_union, cdc_staging, standard]
  models:
    type: array
    items:
      type: object
      required: [name, layer, materialization, depends_on, schema, is_new]
      properties:
        name: { type: string }
        layer: { type: string, enum: [source, staging, intermediate, dimension, fact, mart, rpt] }
        materialization: { type: string, enum: [incremental, table, view, dynamic_table, ephemeral] }
        depends_on: { type: array, items: { type: string } }
        schema: { type: string }
        is_new: { type: boolean }
        incremental_config:
          type: object
          properties:
            unique_key: { type: string }
            strategy: { type: string, enum: [merge, "delete+insert", append] }
            on_schema_change: { type: string }
            lookback: { type: string }
        description: { type: string }
  dag_recommendation:
    type: object
    properties:
      existing_dag: { type: string }
      tags: { type: array }
      schedule: { type: string }
      needs_new_dag: { type: boolean }
  skipped_models:
    type: array
    items: { name: { type: string }, reason: { type: string } }
```

**Budget:** `max_llm_turns: 3`, `max_tokens: 8000`
**Transitions:** `default: confirm_plan`

---

### Step 6: `confirm_plan` — Checkpoint 1

**Mode:** `execute`
**Type:** `delegate`
**Delegate:** `shift.show_plan`
**Requires approval:** `true`

Posts pipeline architecture summary to Slack:
- Source profile summary (rows, columns, freshness, warnings)
- Pipeline pattern identified + reference model used
- Models to create (NEW), modify (MODIFY), or skip (EXISTS)
- DAG and tags recommendation
- Profiling warnings inline

**Output schema** (returned by Shift driver after user responds):

```yaml
output_schema:
  type: object
  required: [user_decision]
  properties:
    user_decision:
      type: string
      enum: [approve, revise]
    feedback:
      type: string
```

**Transitions:**

```yaml
transitions:
  - when: "output.user_decision == 'revise'"
    goto: design_pipeline
  - default: generate_models
```

> **Revision scope:** The revision loop supports design adjustments (model count, materialization, naming, schema, tags). If the user wants a completely different source, the workflow should be restarted. The `design_pipeline` step re-runs with the same discovery/profiling context plus the user's feedback as additional dynamic context.

---

### Step 7: `generate_models` (loop)

**Mode:** `reason`
**Loop:** `over: design_pipeline.models` / `as: current_model`
**Tools:** `snowflake_mcp.execute_query`, `github_cli.read_file`
**Knowledge:** `naming_conventions`, `testing_standards`

For each model, generates 3 artifacts using reference examples as templates:

1. **Model SQL** — with labeled CTE blocks (`[SOURCE]`, `[RENAME]`, `[DEDUP]`, `[FINAL]`) for easy editing
2. **schema.yml entry** — column names, types, descriptions, PK markers
3. **sources.yml entry** — only for staging models with new sources

**SQL generation principles:**
- Config block copied from reference example, adapted for new model
- CTE structure mirrors reference pipeline's layer pattern
- Comments on non-obvious logic (unit conversions, business rules)
- Incremental window as explicit value (easy to adjust)
- For `extend_existing`: generates only the delta (new CTE, new source entry)

**Output schema (per iteration):**

```yaml
type: object
required: [model_name, sql_content, schema_yml_content, file_path, change_points]
properties:
  model_name: { type: string }
  sql_content: { type: string }
  schema_yml_content: { type: string }
  sources_yml_content: { type: string }
  file_path: { type: string }
  schema_yml_path: { type: string }
  is_modification: { type: boolean }
  modification_description: { type: string }
  change_points:
    type: array
    items:
      type: object
      required: [id, section, current, alternatives]
      properties:
        id: { type: string }
        section: { type: string }
        current: { type: string }
        alternatives: { type: array, items: { type: string } }
```

**Budget:** `max_llm_turns: 5`, `max_tokens: 12000` (per loop iteration)

> **Loop output accumulation:** The Shift driver must accumulate outputs from each loop iteration into a list (keyed by `model_name`). The `validate_pipeline` and `show_results` steps reference the full accumulated list, not just the last iteration's output. This matches the pattern used by `thread-field-through-pipeline.yml` (Steps 3-4).

> **Modification outputs:** For `is_modification: true` (extend_existing), the output is the COMPLETE modified file content — not a diff or partial snippet. This matches the convention from `add-column-to-model.yml`.

**Transitions:** `default: validate_pipeline`

---

### Step 8: `validate_pipeline`

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`

Validates generated SQL before showing to user:

| Check | How | Fail Action |
|---|---|---|
| SQL compiles | Dry-run as `CREATE TABLE ... AS SELECT ... LIMIT 0` | Flag error, attempt auto-fix |
| Join fanout | Compare COUNT before/after each JOIN | Warn if rows increase >5% |
| Row count sanity | Compare output vs source from profiling | Warn if >2x or <50% |
| Null propagation | Check PK and metric columns for nulls | Warn if PK has nulls |
| Date continuity | Check for date gaps in output | Warn if incremental missed dates |
| Grain verification | `SELECT pk, COUNT(*) HAVING COUNT(*) > 1` | Fail if duplicates on declared PK |

**Output schema:**

```yaml
type: object
required: [compiles, warnings, errors, sample_output]
properties:
  compiles: { type: boolean }
  warnings:
    type: array
    items: { model_name: { type: string }, check: { type: string }, detail: { type: string }, severity: { type: string } }
  errors:
    type: array
    items: { model_name: { type: string }, check: { type: string }, detail: { type: string } }
  sample_output:
    type: object
    properties:
      columns: { type: array }
      rows: { type: array }
      row_count: { type: integer }
```

**Budget:** `max_llm_turns: 6`, `max_tokens: 10000`

**Transitions:**

```yaml
transitions:
  - when: "output.errors != []"
    goto: fix_validation_errors
  - default: recommend_tests
```

---

### Step 8b: `fix_validation_errors` (auto-repair, max 2 attempts)

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`, `github_cli.read_file`

Attempts to auto-fix SQL compilation errors or grain violations found by `validate_pipeline`. Reads the error details, identifies the cause (missing column, wrong join key, type mismatch), and regenerates the affected model's SQL.

**Context (dynamic):**
- `validate_pipeline` → errors array
- `generate_models` → accumulated model outputs
- `discover_reference_patterns` → reference SQL for comparison

The Shift driver tracks fix attempts. After 2 failed attempts, the step returns with `auto_fix_exhausted: true` and the workflow proceeds to `recommend_tests` → `show_results` with errors flagged for the user to see at Checkpoint 2.

**Budget:** `max_llm_turns: 5`, `max_tokens: 10000`
**Transitions:** `default: validate_pipeline` (loops back for re-validation)

---

### Step 9: `recommend_tests`

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`

Infers tests from 3 sources:

1. **Column types + naming** — `*_id` → `not_null`, PK → `unique`, low cardinality → `accepted_values`
2. **Reference pipeline tests** — mirror the reference model's test patterns (from `DBT_TESTS`)
3. **Profiling results** — 0% nulls in source → `not_null`, cardinality = row count → `unique`

**Output schema:**

```yaml
type: object
required: [recommended_tests]
properties:
  recommended_tests:
    type: array
    items:
      type: object
      required: [model_name, test_name, column, reasoning]
      properties:
        model_name: { type: string }
        test_name: { type: string }
        column: { type: string }
        config: { type: object }
        reasoning: { type: string }
        from_reference: { type: boolean }
```

**Budget:** `max_llm_turns: 4`, `max_tokens: 8000`
**Transitions:** `default: show_results`

---

### Step 10: `show_results` — Checkpoint 2

**Mode:** `execute`
**Type:** `delegate`
**Delegate:** `shift.show_plan`
**Requires approval:** `true`

Posts full generation results to Slack:
- Validation results (compiles, warnings, errors)
- Generated SQL for each model (with CTE labels)
- Data preview (first 5 rows of final output)
- Recommended tests with reasoning
- Change points with alternatives
- DAG recommendation

**Output schema** (returned by Shift driver):

```yaml
output_schema:
  type: object
  required: [user_decision]
  properties:
    user_decision:
      type: string
      enum: [approve, edit]
    edit_request:
      type: string
    edit_count:
      type: integer   # Shift driver tracks iteration count, increments each round
```

**Transitions:**

```yaml
transitions:
  - when: "output.user_decision == 'edit'"
    goto: apply_changes
  - default: create_pr
```

---

### Step 11: `apply_changes` (edit loop — max 3 iterations)

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`, `github_cli.read_file`

Takes user's change request + generated models, applies the modification to affected model(s) only, re-validates. The Shift driver tracks `edit_count` and after 3 rounds returns `user_decision: "approve"` with a message: "Reached edit limit — creating PR. You can make further changes in the PR itself."

**Context (dynamic):**
- `show_results` → `edit_request` (what the user wants changed)
- `generate_models` → accumulated model outputs
- `validate_pipeline` → current validation state

**Output:** Updated model files (complete file content, not diffs) + re-validation results.

**Budget:** `max_llm_turns: 5`, `max_tokens: 10000`
**Transitions:** `default: show_results` (loops back to Checkpoint 2)

> **Bounded exit:** The 3-iteration cap is enforced by the Shift driver, not the engine. The driver increments `edit_count` on each round and force-approves after 3. This avoids needing an engine change for `max_transition_cycles`.

---

### Step 12: `create_pr` — Checkpoint 3

**Mode:** `execute`
**Type:** `delegate`
**Delegate:** `shift.create_pr`
**Requires approval:** `true`

Creates PR with all artifacts:
- New/modified model SQL files (correct directory placement)
- schema.yml entries
- sources.yml entries (if new sources)

PR description includes: architecture summary, source profiling results, validation results, test plan, DAG recommendation.

**Transitions:** `default: recommend_orchestration`

---

### Step 13: `recommend_orchestration`

**Mode:** `reason`
**Tools:** `snowflake_mcp.execute_query`

Queries `DBT_INVOCATIONS` to confirm DAG exists and tags/selectors align. Posts final summary:
- Which DAG picks up the new models
- Tags that match existing selectors
- Downstream models that will auto-rebuild
- Monitoring recommendation (watch first 3 runs)

**Output schema:**

```yaml
type: object
required: [dag_name, schedule, selector_match, downstream_impact, monitoring_recommendation]
properties:
  dag_name: { type: string }
  schedule: { type: string }
  selector_match: { type: boolean }
  tags: { type: array }
  downstream_impact:
    type: array
    items: { model_name: { type: string }, relationship: { type: string } }
  monitoring_recommendation: { type: string }
```

**Budget:** `max_llm_turns: 3`, `max_tokens: 5000`

---

## Full Step Flow

```
setup_environment
       ▼
classify_intent
  ├─ [new_source]      → discover_source_schema ─────┐
  ├─ [similar_to]      → trace_reference_pipeline ────┤
  ├─ [sql_to_pipeline] → parse_sql_sources ───────────┤
  └─ [extend_existing] → analyze_target_pipeline ─────┘
                                                       ▼
                                              profile_source_data
                                                       ▼
                                          discover_reference_patterns
                                                       ▼
                                              design_pipeline
                                                       ▼
                                    ↺ confirm_plan (Checkpoint 1)
                                          approve → ▼  revise → design_pipeline
                                          generate_models (loop)
                                                       ▼
                                            validate_pipeline
                                         errors → ↺ fix_validation_errors (max 2)
                                       no errors → ▼
                                            recommend_tests
                                                       ▼
                                    ↺ show_results (Checkpoint 2)
                                      approve → ▼  edit → apply_changes (max 3)
                                            create_pr (Checkpoint 3)
                                                       ▼
                                        recommend_orchestration
```

**16 steps, 3 user checkpoints, 4 entry point branches, 1 design revision loop, 1 validation auto-fix loop (max 2), 1 edit loop (max 3).**

## Metadata Sources Used

| Source | Table | Purpose |
|--------|-------|---------|
| Snowflake | `INFORMATION_SCHEMA.COLUMNS` | Source table schema discovery |
| Snowflake | `DW.DATAOPS_DBT.DBT_MODELS` | Find existing dbt models, materializations, dependencies |
| Snowflake | `DW.DATAOPS_DBT.DBT_MODEL_TABLE_LINEAGE` | Trace full recursive lineage for reference pipelines |
| Snowflake | `DW.DATAOPS_DBT.DBT_TESTS` | Get test patterns from reference models |
| Snowflake | `DW.DATAOPS_DBT.DBT_SOURCES` | Check existing source definitions |
| Snowflake | `DW.DATAOPS_DBT.DBT_RUN_RESULTS` | Get compiled SQL for reference models |
| Snowflake | `DW.DATAOPS_DBT.DBT_INVOCATIONS` | DAG/job name patterns for orchestration |
| Snowflake | `DW.DATAOPS_DBT.DBT_COLUMNS` | Column-level metadata and descriptions |
| GitHub | Astronomer repo model SQL files | Real Jinja SQL as generation templates |
| GitHub | Astronomer repo schema.yml files | Real test/description patterns |

## MCP Tools Required

| Tool | Steps Used In |
|------|---------------|
| `snowflake_mcp.execute_query` | All discovery, profiling, validation, orchestration steps |
| `snowflake_mcp.describe_table` | `discover_source_schema`, `parse_sql_sources` |
| `github_cli.read_file` | `analyze_target_pipeline`, `discover_reference_patterns`, `generate_models` |
| `github_cli.search_code` | `discover_reference_patterns` |

## Knowledge Files (via `context.static`)

All knowledge references use `context.static` in the YAML (not `context.knowledge`), matching the convention from existing workflows.

| File | Steps Used In |
|------|---------------|
| `naming_conventions` | `classify_intent`, `design_pipeline`, `generate_models` |
| `load_frequency_heuristics` | `design_pipeline` |
| `dbt_project_structure` | `design_pipeline` |
| `testing_standards` | `generate_models`, `recommend_tests` |

## Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Max models per pipeline | 10 | Prevents runaway generation for overly complex requests |
| Source profiling checks | 6 per table | Covers shape, PK, nulls, volume, freshness, duplicates |
| Validation checks | 6 per model | Compile, fanout, row count, nulls, date gaps, grain |
| Edit loop max iterations | 3 | Prevents infinite loops — surface to human after 3 rounds |
| Reference examples max | 5 models | Enough to establish pattern without token overload |

## New Files

| File | Purpose |
|------|---------|
| `content/workflows/create-etl-pipeline.yml` | Workflow definition (16 steps incl. setup + fix_validation_errors) |
| `content/workflows/manifest.yml` | Update: add entry with trigger keywords `[create pipeline, build pipeline, new pipeline, etl pipeline, productionize sql, new source, similar to]` |
| `content/personas/data_engineer.yml` | May need updates for new tool access (Slack MCP not needed for v1) |
| `tests/test_e2e_create_etl_pipeline.py` | E2E tests for each entry point branch |
| `tests/cassettes/create-etl-pipeline-*/` | 4 cassette directories (one per entry point) |

## Non-Goals (v1)

- **No Airflow MCP** — orchestration is recommendation only (tags + DAG name). Models are picked up by Cosmos via dbt selectors.
- **No Slack MCP** — this workflow is triggered via Shift, not directly from Slack search.
- **No persistent pipeline index** — uses live Snowflake metadata every time.
- **No auto-merge** — PR requires human approval.
- **No multi-repo** — generates files for the Astronomer repo only.

## Future Enhancements (v2+)

- **Airflow MCP** — verify DAG picked up models after merge, trigger test runs
- **Pipeline templates** — frequently used patterns as first-class templates (reduces LLM reasoning)
- **Hex notebook integration** — directly import SQL from Hex API instead of paste
- **Cost estimation** — use `COST_PER_QUERY` and warehouse sizing to estimate pipeline run cost
- **Auto-documentation** — generate Confluence page for the new pipeline
