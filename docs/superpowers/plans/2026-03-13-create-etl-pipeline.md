# Create ETL Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a DCAG workflow that takes a one-sentence request and generates a complete, validated ETL pipeline — from source discovery through PR creation — with 3 user checkpoints.

**Architecture:** A 16-step YAML workflow with 4 entry point branches (new_source, similar_to, sql_to_pipeline, extend_existing) that converge at a design step, loop through model generation, validate, and deliver a PR. All metadata discovered from Snowflake DataOps tables and real SQL read from the Astronomer repo via GitHub CLI.

**Tech Stack:** YAML workflow definition, JSON cassettes, pytest, Snowflake MCP, GitHub CLI MCP

**Spec:** `docs/superpowers/specs/2026-03-13-create-etl-pipeline-design.md`

---

## File Map

| File | Purpose |
|------|---------|
| `content/workflows/create-etl-pipeline.yml` | Workflow definition (16 steps) |
| `content/workflows/create-etl-pipeline.test.yml` | Conformance spec |
| `content/workflows/manifest.yml` | Add entry with trigger keywords |
| `tests/test_e2e_create_etl_pipeline.py` | E2E tests (4 test classes, one per entry point) |
| `tests/test_conformance_create_etl_pipeline.py` | Conformance tests |
| `tests/cassettes/create-etl-pipeline-new-source/` | Cassettes for new_source path |
| `tests/cassettes/create-etl-pipeline-similar-to/` | Cassettes for similar_to path |
| `tests/cassettes/create-etl-pipeline-sql-to-pipeline/` | Cassettes for sql_to_pipeline path |
| `tests/cassettes/create-etl-pipeline-extend-existing/` | Cassettes for extend_existing path |

---

## Chunk 1: Workflow YAML — Setup, Intent, and Discovery Steps (0-4)

### Task 1: Create the workflow YAML with inputs and first 5 steps

**Files:**
- Create: `content/workflows/create-etl-pipeline.yml`

- [ ] **Step 1: Create workflow header with inputs**

```yaml
workflow:
  id: create-etl-pipeline
  name: Create ETL Pipeline
  persona: data_engineer

  inputs:
    request_text:
      type: string
      required: true
    sql_text:
      type: string
      required: false
    reference_model:
      type: string
      required: false
    channel_id:
      type: string
      required: false
    thread_ts:
      type: string
      required: false

  steps:
```

Write to: `content/workflows/create-etl-pipeline.yml`

- [ ] **Step 2: Add `setup_environment` step (Step 0)**

Append to the `steps:` array:

```yaml
    # Step 0: Validate tool availability
    - id: setup_environment
      mode: reason
      instruction: |
        Verify that the required tools are available for this workflow.
        Test Snowflake connectivity by running a simple query.
        Test GitHub CLI by attempting to list files.
        Report which tools are available so downstream steps can use
        degraded mode if GitHub is unavailable.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Test connectivity with SELECT 1"
        - name: github_cli.read_file
          instruction: "Test connectivity by reading a known file"
      context:
        static: [snowflake_environment]
        dynamic: []
      output_schema:
        type: object
        required: [snowflake_available, github_available]
        properties:
          snowflake_available:
            type: boolean
          github_available:
            type: boolean
      budget:
        max_llm_turns: 3
        max_tokens: 4000
```

- [ ] **Step 3: Add `classify_intent` step (Step 1)**

```yaml
    # Step 1: Classify user intent into entry point
    - id: classify_intent
      mode: reason
      instruction: |
        Parse the user's request to determine the entry point for pipeline creation.
        Extract structured fields from the natural language request:

        ENTRY POINTS:
        - new_source: User names a specific source table to build a pipeline for.
          Example: "Build a pipeline for fivetran_database.tiktok_ads.campaign_report"
        - similar_to: User references an existing model and wants something like it.
          Example: "Build something like campaign_day_agg but for affiliate traffic"
        - sql_to_pipeline: User provides raw SQL (in sql_text input or pasted in request).
          Example: "Make this a proper pipeline: SELECT ... FROM ..."
        - extend_existing: User wants to add a source/channel to an existing pipeline.
          Example: "Add TikTok as a new channel to marketing_spend"

        Extract:
        - source_hints: table names, system names, or identifiers mentioned
        - reference_model_hint: model name if similar_to or extend_existing
        - domain_hint: business domain (acquisition, core, pricing, cx, etc.)
        - sql_text: pass through if provided in input
      tools: []
      context:
        static: [naming_conventions]
        dynamic: []
      output_schema:
        type: object
        required: [entry_point, source_hints, domain_hint]
        properties:
          entry_point:
            type: string
            enum: [new_source, similar_to, sql_to_pipeline, extend_existing]
          source_hints:
            type: array
            items:
              type: string
          reference_model_hint:
            type: string
          domain_hint:
            type: string
          sql_text:
            type: string
      budget:
        max_llm_turns: 2
        max_tokens: 4000
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

- [ ] **Step 4: Add the 4 discovery branches (Steps 2a-2d)**

```yaml
    # Step 2a: Discover source schema (for new_source)
    - id: discover_source_schema
      mode: reason
      instruction: |
        Discover the source table(s) mentioned in the user's request.

        1. Query INFORMATION_SCHEMA.COLUMNS for the source table schema
           (column names, types, nullability, ordinal position)
        2. Query DW.DATAOPS_DBT.DBT_MODELS to check if staging models
           already exist for this source (WHERE depends_on_nodes ILIKE '%{source}%')
        3. Query DW.DATAOPS_DBT.DBT_MODEL_TABLE_LINEAGE to find similar
           pipelines in the same schema/domain (WHERE root_table_name ILIKE '%{domain}%' LIMIT 5)
        4. Query DW.DATAOPS_DBT.DBT_MODELS for naming and materialization
           conventions used by existing models in the target schema

        Output all findings in the common discovery format.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query INFORMATION_SCHEMA, DBT_MODELS, and DBT_MODEL_TABLE_LINEAGE"
          usage_pattern: |
            1. SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
               FROM {db}.INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            2. SELECT name, schema_name, materialization, depends_on_nodes, original_path
               FROM DW.DATAOPS_DBT.DBT_MODELS
               WHERE depends_on_nodes ILIKE '%{source_table}%'
            3. SELECT DISTINCT root_table_name, level, node
               FROM DW.DATAOPS_DBT.DBT_MODEL_TABLE_LINEAGE
               WHERE root_table_name ILIKE '%{domain}%' AND level <= 4
               LIMIT 50
            4. SELECT name, materialization, tags, original_path
               FROM DW.DATAOPS_DBT.DBT_MODELS
               WHERE schema_name = '{target_schema}' LIMIT 20
        - name: snowflake_mcp.describe_table
          instruction: "Get detailed column metadata for the source table"
      context:
        static: [snowflake_environment]
        dynamic:
          - from: classify_intent
            select: [source_hints, domain_hint]
      output_schema:
        type: object
        required: [source_tables, existing_models, reference_pipeline, conventions]
        properties:
          source_tables:
            type: array
            items:
              type: object
              required: [table_fqn, columns]
          existing_models:
            type: array
          reference_pipeline:
            type: object
          conventions:
            type: object
      budget:
        max_llm_turns: 6
        max_tokens: 10000
      transitions:
        - default: profile_source_data

    # Step 2b: Trace reference pipeline lineage (for similar_to)
    - id: trace_reference_pipeline
      mode: reason
      instruction: |
        Trace the full lineage of the reference model to understand the
        pipeline structure the user wants to replicate.

        1. Find the reference model in DBT_MODELS to get its unique_id,
           schema, materialization, depends_on_nodes, and original_path
        2. Trace its FULL recursive lineage via DBT_MODEL_TABLE_LINEAGE:
           query WHERE root_table_name matches the model's table FQN,
           get all levels (depth) and node names
        3. For each model in the lineage, get details from DBT_MODELS:
           materialization, tags, depends_on_nodes
        4. Get test patterns from DBT_TESTS for models in the lineage:
           parent_model_unique_id, short_name, test_column_name
        5. Identify what new source the user wants to use (from source_hints)

        This gives us the complete template to replicate.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query DBT_MODELS, DBT_MODEL_TABLE_LINEAGE, and DBT_TESTS"
          usage_pattern: |
            1. SELECT * FROM DW.DATAOPS_DBT.DBT_MODELS WHERE name = '{reference_model}'
            2. SELECT level, node, lineage_path FROM DW.DATAOPS_DBT.DBT_MODEL_TABLE_LINEAGE
               WHERE root_table_name ILIKE '%{reference_table}%' ORDER BY level
            3. SELECT parent_model_unique_id, short_name, test_column_name, severity
               FROM DW.DATAOPS_DBT.DBT_TESTS
               WHERE parent_model_unique_id ILIKE '%{model}%'
      context:
        static: [snowflake_environment]
        dynamic:
          - from: classify_intent
            select: [reference_model_hint, source_hints, domain_hint]
      output_schema:
        type: object
        required: [source_tables, existing_models, reference_pipeline, conventions]
        properties:
          source_tables:
            type: array
            items:
              type: object
              required: [table_fqn, columns]
          existing_models:
            type: array
          reference_pipeline:
            type: object
            properties:
              root_model:
                type: string
              layers:
                type: array
              materializations:
                type: array
              tests:
                type: array
          conventions:
            type: object
      budget:
        max_llm_turns: 6
        max_tokens: 10000
      transitions:
        - default: profile_source_data

    # Step 2c: Parse SQL to identify sources (for sql_to_pipeline)
    - id: parse_sql_sources
      mode: reason
      instruction: |
        Parse the user's SQL to identify source tables and plan the pipeline.

        1. Extract all FROM and JOIN table references from the SQL
        2. For each referenced table, check if a dbt model exists in DBT_MODELS
           (query WHERE name ILIKE '%{table}%' or depends_on_nodes references it)
        3. For tables without dbt models, query INFORMATION_SCHEMA to get their
           schema (these will need new staging models)
        4. Identify CTE structure in the SQL — map each CTE to a potential
           pipeline layer (staging, intermediate, final)
        5. Check domain conventions for models that DO exist

        The goal is to decompose the user's ad-hoc SQL into proper dbt layers.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query DBT_MODELS and INFORMATION_SCHEMA for source table metadata"
        - name: snowflake_mcp.describe_table
          instruction: "Describe tables that don't have dbt models yet"
      context:
        static: [snowflake_environment, dbt_project_structure]
        dynamic:
          - from: classify_intent
            select: [sql_text, source_hints, domain_hint]
      output_schema:
        type: object
        required: [source_tables, existing_models, reference_pipeline, conventions]
        properties:
          source_tables:
            type: array
            items:
              type: object
              required: [table_fqn, columns]
          existing_models:
            type: array
          reference_pipeline:
            type: object
          conventions:
            type: object
      budget:
        max_llm_turns: 6
        max_tokens: 10000
      transitions:
        - default: profile_source_data

    # Step 2d: Analyze target pipeline for extension (for extend_existing)
    - id: analyze_target_pipeline
      mode: reason
      instruction: |
        Analyze the existing pipeline that the user wants to extend with
        a new source or channel.

        1. Find the target model in DBT_MODELS and get its full metadata
        2. Read the target model's SQL from GitHub to understand its structure
           (UNION ALL blocks, CTEs, JOIN patterns)
           DEGRADED MODE: If GitHub unavailable, use compiled_code from
           DBT_RUN_RESULTS instead (loses Jinja but shows resolved SQL)
        3. Trace its lineage via DBT_MODEL_TABLE_LINEAGE to understand
           the full pipeline structure
        4. Get test patterns from DBT_TESTS to replicate for the new source
        5. Identify where the new source fits: new CTE in UNION ALL,
           new JOIN, new intermediate model, etc.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query DBT_MODELS, DBT_MODEL_TABLE_LINEAGE, DBT_TESTS, DBT_RUN_RESULTS"
        - name: github_cli.read_file
          instruction: "Read the target model SQL file from the Astronomer repo"
      context:
        static: [snowflake_environment]
        dynamic:
          - from: setup_environment
            select: github_available
          - from: classify_intent
            select: [reference_model_hint, source_hints, domain_hint]
      output_schema:
        type: object
        required: [source_tables, existing_models, reference_pipeline, conventions]
        properties:
          source_tables:
            type: array
            items:
              type: object
              required: [table_fqn, columns]
          existing_models:
            type: array
          reference_pipeline:
            type: object
          conventions:
            type: object
      budget:
        max_llm_turns: 6
        max_tokens: 10000
      transitions:
        - default: profile_source_data
```

- [ ] **Step 5: Add `profile_source_data` step (Step 3)**

```yaml
    # Step 3: Profile source data for all identified tables
    - id: profile_source_data
      mode: reason
      instruction: |
        Profile every source table identified in the discovery phase.
        For each table, run these 6 checks:

        1. SHAPE: SELECT COUNT(*) as rows, MIN(date_col), MAX(date_col)
        2. PK CANDIDATES: For columns likely to be keys (*_id, *_key),
           SELECT column, COUNT(DISTINCT column), COUNT(*) to check uniqueness
        3. NULL RATES: For each column,
           SUM(CASE WHEN col IS NULL THEN 1 END)::FLOAT / COUNT(*) as null_pct
           Flag columns with >5% nulls
        4. DAILY VOLUME: GROUP BY DATE_TRUNC('day', date_col) for last 7 days
           to determine if hourly or daily materialization is needed
        5. FRESHNESS: SELECT MAX(loaded_at_col),
           DATEDIFF('hour', MAX(loaded_at_col), CURRENT_TIMESTAMP()) as hours_stale
        6. DUPLICATES: SELECT pk_candidates, COUNT(*) HAVING COUNT(*) > 1 LIMIT 5

        Generate warnings for: stale sources (>48h), high nulls (>5% on non-nullable),
        no PK candidate, duplicates on candidate PK, low volume (<100 rows/day).
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Run profiling queries against each source table"
          usage_pattern: |
            For each source table:
            1. SELECT COUNT(*) as row_count FROM {table_fqn}
            2. SELECT {pk_col}, COUNT(*) as cnt FROM {table_fqn} GROUP BY 1 HAVING cnt > 1 LIMIT 5
            3. SELECT column_name, SUM(CASE WHEN col IS NULL THEN 1 END)::FLOAT/COUNT(*) FROM {table}
      context:
        static: []
        dynamic:
          - from: discover_source_schema
            select: source_tables
          - from: trace_reference_pipeline
            select: source_tables
          - from: parse_sql_sources
            select: source_tables
          - from: analyze_target_pipeline
            select: source_tables
      output_schema:
        type: object
        required: [profiles, warnings]
        properties:
          profiles:
            type: array
            items:
              type: object
              required: [table_fqn, row_count, date_range, daily_avg_rows, freshness_hours, pk_candidate, null_flags]
          warnings:
            type: array
            items:
              type: object
              required: [table_fqn, warning_type, detail]
              properties:
                warning_type:
                  type: string
                  enum: [stale_source, high_nulls, no_pk_candidate, duplicates_detected, low_volume]
      budget:
        max_llm_turns: 6
        max_tokens: 10000
      transitions:
        - default: discover_reference_patterns
```

- [ ] **Step 6: Add `discover_reference_patterns` step (Step 4)**

```yaml
    # Step 4: Read real SQL from Astronomer repo as generation templates
    - id: discover_reference_patterns
      mode: reason
      instruction: |
        Read REAL SQL from the Astronomer repo to use as templates for
        generating the new pipeline. Do NOT invent patterns — always
        base generation on actual production code.

        1. From the discovery step, identify the reference pipeline
           (explicit from user, or best match from same domain)
        2. For each model in the reference pipeline's lineage:
           - Read the raw model SQL from GitHub (Jinja, ref(), config block)
           - Read its schema.yml (tests, descriptions)
           DEGRADED MODE: If GitHub unavailable, query DBT_RUN_RESULTS
           for compiled_code (resolved SQL without Jinja)
        3. If no explicit reference: find 2-3 models in the same domain
           from DBT_MODELS, read their SQL for pattern extraction
        4. Extract: config blocks, CTE structure, naming patterns,
           materialization choices, incremental strategies

        This step ensures generate_models has REAL examples to follow,
        not hallucinated patterns.
      tools:
        - name: github_cli.read_file
          instruction: "Read model SQL and schema.yml files from the Astronomer repo"
        - name: github_cli.search_code
          instruction: "Search for config blocks and patterns in the domain directory"
        - name: snowflake_mcp.execute_query
          instruction: "DEGRADED MODE: Get compiled_code from DBT_RUN_RESULTS if GitHub unavailable"
          usage_pattern: |
            SELECT compiled_code FROM DW.DATAOPS_DBT.DBT_RUN_RESULTS
            WHERE unique_id ILIKE '%{model}%' AND status = 'success'
            ORDER BY execution_time DESC LIMIT 1
      context:
        static: []
        dynamic:
          - from: setup_environment
            select: github_available
          - from: classify_intent
            select: [reference_model_hint, domain_hint]
          - from: discover_source_schema
            select: [reference_pipeline, conventions]
          - from: trace_reference_pipeline
            select: [reference_pipeline, conventions]
          - from: parse_sql_sources
            select: [reference_pipeline, conventions]
          - from: analyze_target_pipeline
            select: [reference_pipeline, conventions]
      output_schema:
        type: object
        required: [reference_examples]
        properties:
          reference_examples:
            type: array
            items:
              type: object
              required: [model_name, layer, raw_sql, config_block, original_path]
              properties:
                model_name:
                  type: string
                layer:
                  type: string
                raw_sql:
                  type: string
                compiled_sql:
                  type: string
                config_block:
                  type: string
                schema_yml:
                  type: string
                tests:
                  type: array
                original_path:
                  type: string
          domain_conventions:
            type: object
            properties:
              common_config_blocks:
                type: array
              naming_pattern:
                type: string
              typical_materialization:
                type: string
              directory_structure:
                type: string
      budget:
        max_llm_turns: 8
        max_tokens: 15000
      transitions:
        - default: design_pipeline
```

- [ ] **Step 7: Verify YAML syntax**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -c "
import yaml
from pathlib import Path
with open('content/workflows/create-etl-pipeline.yml') as f:
    wf = yaml.safe_load(f)
print(f'Workflow ID: {wf[\"workflow\"][\"id\"]}')
print(f'Steps so far: {len(wf[\"workflow\"][\"steps\"])}')
for s in wf['workflow']['steps']:
    print(f'  - {s[\"id\"]}')
"
```

Expected: 7 steps listed (setup_environment, classify_intent, discover_source_schema, trace_reference_pipeline, parse_sql_sources, analyze_target_pipeline, profile_source_data, discover_reference_patterns). Actually 8 steps.

- [ ] **Step 8: Commit**

```bash
git add content/workflows/create-etl-pipeline.yml
git commit -m "feat: create-etl-pipeline workflow — setup, intent, discovery, profiling steps"
```

---

## Chunk 2: Workflow YAML — Design, Checkpoint 1, Generation, Validation (Steps 5-9)

### Task 2: Add design_pipeline and confirm_plan steps

**Files:**
- Modify: `content/workflows/create-etl-pipeline.yml`

- [ ] **Step 1: Add `design_pipeline` step (Step 5)**

Append to the `steps:` array:

```yaml
    # Step 5: Design the pipeline architecture
    - id: design_pipeline
      mode: reason
      instruction: |
        Based on ALL discovery, profiling, and reference pattern data,
        design the complete pipeline architecture.

        DECISIONS TO MAKE:
        1. Pipeline pattern: hourly_rollup, scd_star, multi_source_union,
           cdc_staging, or standard — based on reference and source characteristics
        2. Number of layers: source → staging → intermediate? → fact/mart
        3. Model names: follow domain naming conventions from reference examples
        4. Materialization per layer: based on daily volume from profiling
           and reference pipeline's config blocks
        5. Incremental strategy: based on PK candidates from profiling
           and reference config blocks (merge, delete+insert, append)
        6. Schema placement: match existing models in the domain
        7. DAG and tags: query DBT_INVOCATIONS for job_name patterns

        OUTPUT: A list of models to create, each with name, layer,
        materialization, dependencies, and config. Mark models that
        already exist (is_new: false) so they are skipped in generation.

        For extend_existing: output only the delta — new models to add
        plus the existing model to modify.
      tools: []
      context:
        static: [naming_conventions, load_frequency_heuristics, dbt_project_structure]
        dynamic:
          - from: classify_intent
            select: [entry_point, source_hints, domain_hint]
          - from: discover_source_schema
            select: [source_tables, existing_models, reference_pipeline, conventions]
          - from: trace_reference_pipeline
            select: [source_tables, existing_models, reference_pipeline, conventions]
          - from: parse_sql_sources
            select: [source_tables, existing_models, reference_pipeline, conventions]
          - from: analyze_target_pipeline
            select: [source_tables, existing_models, reference_pipeline, conventions]
          - from: profile_source_data
            select: [profiles, warnings]
          - from: discover_reference_patterns
            select: [reference_examples, domain_conventions]
      output_schema:
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
                name:
                  type: string
                layer:
                  type: string
                  enum: [source, staging, intermediate, dimension, fact, mart, rpt]
                materialization:
                  type: string
                  enum: [incremental, table, view, dynamic_table, ephemeral]
                depends_on:
                  type: array
                  items:
                    type: string
                schema:
                  type: string
                is_new:
                  type: boolean
                incremental_config:
                  type: object
                  properties:
                    unique_key:
                      type: string
                    strategy:
                      type: string
                    on_schema_change:
                      type: string
                    lookback:
                      type: string
                description:
                  type: string
          dag_recommendation:
            type: object
            properties:
              existing_dag:
                type: string
              tags:
                type: array
              schedule:
                type: string
              needs_new_dag:
                type: boolean
          skipped_models:
            type: array
      budget:
        max_llm_turns: 3
        max_tokens: 8000
      transitions:
        - default: confirm_plan
```

- [ ] **Step 2: Add `confirm_plan` checkpoint (Step 6)**

```yaml
    # Step 6: Checkpoint 1 — Show plan to user for approval
    - id: confirm_plan
      mode: execute
      type: delegate
      delegate: shift.show_plan
      requires_approval: true
      context:
        dynamic:
          - from: profile_source_data
            select: [profiles, warnings]
          - from: design_pipeline
      output_schema:
        type: object
        required: [user_decision]
        properties:
          user_decision:
            type: string
            enum: [approve, revise]
          feedback:
            type: string
      transitions:
        - when: "output.user_decision == 'revise'"
          goto: design_pipeline
        - default: generate_models
```

- [ ] **Step 3: Commit**

```bash
git add content/workflows/create-etl-pipeline.yml
git commit -m "feat: add design_pipeline and confirm_plan (checkpoint 1) steps"
```

---

### Task 3: Add generate_models, validate_pipeline, and recommend_tests steps

**Files:**
- Modify: `content/workflows/create-etl-pipeline.yml`

- [ ] **Step 1: Add `generate_models` loop step (Step 7)**

```yaml
    # Step 7: Generate model SQL, schema.yml, sources.yml for each model
    - id: generate_models
      mode: reason
      loop:
        over: design_pipeline.models
        as: current_model
      instruction: |
        Generate all artifacts for {{current_model.name}}.

        Use the reference examples from discover_reference_patterns as your
        TEMPLATE. Do not invent patterns — adapt the reference SQL to the
        new source and requirements.

        GENERATE 3 ARTIFACTS:

        1. MODEL SQL FILE — structured with labeled CTE blocks:
           -- [SOURCE] Raw data pull with incremental filter
           -- [RENAME] Column standardization and type casting
           -- [DEDUP] Deduplication if profiling found duplicates
           -- [ENRICH] Joins to dimension/lookup tables (if needed)
           -- [FINAL] Final SELECT with surrogate key generation

           Config block: copy from reference example, adapt unique_key
           and materialization for this model.

           For is_modification=true (extend_existing): output the COMPLETE
           modified file, not a diff. Read the existing file first.

        2. SCHEMA.YML ENTRY — column names, types from source profiling,
           descriptions inferred from column names + reference descriptions.
           Mark primary key columns.

        3. SOURCES.YML ENTRY (only for staging models with new sources) —
           source_name, table identifier, loaded_at_field from profiling.

        Also output change_points: decisions you made with alternatives,
        so the user can easily request changes at Checkpoint 2.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query source data for column verification if needed"
        - name: github_cli.read_file
          instruction: "Read existing model file for extend_existing modifications"
      context:
        static: [naming_conventions, testing_standards]
        dynamic:
          - from: design_pipeline
            select: models
          - from: discover_reference_patterns
            select: [reference_examples, domain_conventions]
          - from: profile_source_data
            select: [profiles, warnings]
          - from: classify_intent
            select: [entry_point, sql_text]
      output_schema:
        type: object
        required: [model_name, sql_content, schema_yml_content, file_path, change_points]
        properties:
          model_name:
            type: string
          sql_content:
            type: string
          schema_yml_content:
            type: string
          sources_yml_content:
            type: string
          file_path:
            type: string
          schema_yml_path:
            type: string
          is_modification:
            type: boolean
          modification_description:
            type: string
          change_points:
            type: array
            items:
              type: object
              required: [id, section, current, alternatives]
      budget:
        max_llm_turns: 5
        max_tokens: 12000
```

- [ ] **Step 2: Add `validate_pipeline` step (Step 8)**

```yaml
    # Step 8: Validate generated SQL
    - id: validate_pipeline
      mode: reason
      instruction: |
        Validate the generated SQL for all models. Run these checks:

        1. SQL COMPILES: For each model, attempt to dry-run the compiled SQL
           as CREATE TABLE ... AS SELECT ... LIMIT 0 (or EXPLAIN)
        2. JOIN FANOUT: Compare COUNT(*) before and after each JOIN.
           Warn if rows increase >5% (likely missing dedup or wrong join key)
        3. ROW COUNT SANITY: Compare output row count vs source row count
           from profiling. Warn if >2x (fanout) or <50% (over-filtering)
        4. NULL PROPAGATION: Check PK and metric columns for unexpected nulls
           in the output. Warn if PK has any nulls.
        5. DATE CONTINUITY: Check for date gaps in output where source has
           continuous dates. Warn if incremental logic missed dates.
        6. GRAIN VERIFICATION: SELECT pk_columns, COUNT(*) FROM output
           GROUP BY pk_columns HAVING COUNT(*) > 1 LIMIT 5.
           FAIL if duplicates found on declared primary key.

        Report all findings as warnings or errors.
        Generate a sample_output with first 5 rows of the final model.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Run validation queries against Snowflake"
      context:
        static: []
        dynamic:
          - from: generate_models
          - from: profile_source_data
            select: profiles
      output_schema:
        type: object
        required: [compiles, warnings, errors, sample_output]
        properties:
          compiles:
            type: boolean
          warnings:
            type: array
            items:
              type: object
              required: [model_name, check, detail]
          errors:
            type: array
            items:
              type: object
              required: [model_name, check, detail]
          sample_output:
            type: object
            properties:
              columns:
                type: array
              rows:
                type: array
              row_count:
                type: integer
      budget:
        max_llm_turns: 6
        max_tokens: 10000
      transitions:
        - when: "output.errors != []"
          goto: fix_validation_errors
        - default: recommend_tests
```

- [ ] **Step 3: Add `fix_validation_errors` step (Step 8b)**

```yaml
    # Step 8b: Auto-fix validation errors (max 2 attempts)
    - id: fix_validation_errors
      mode: reason
      instruction: |
        Attempt to auto-fix the validation errors found by validate_pipeline.

        For each error:
        - SQL compilation error: read the error message, identify the cause
          (missing column, wrong type, bad join), regenerate the affected
          model's SQL
        - Grain violation: add or fix the dedup logic in the affected model
        - Join fanout: add DISTINCT or fix the join key

        Output the corrected model files. The Shift driver tracks fix
        attempts — after 2 failed attempts, proceed to show_results
        with errors flagged for the user.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Re-validate corrected SQL"
        - name: github_cli.read_file
          instruction: "Re-read reference SQL if needed for correction"
      context:
        static: []
        dynamic:
          - from: validate_pipeline
            select: errors
          - from: generate_models
          - from: discover_reference_patterns
            select: reference_examples
      output_schema:
        type: object
        required: [fixed_models, auto_fix_exhausted]
        properties:
          fixed_models:
            type: array
            items:
              type: object
              required: [model_name, sql_content, fix_description]
          auto_fix_exhausted:
            type: boolean
      budget:
        max_llm_turns: 5
        max_tokens: 10000
      transitions:
        - default: validate_pipeline
```

- [ ] **Step 4: Add `recommend_tests` step (Step 9)**

```yaml
    # Step 9: Recommend tests based on profiling, reference, and conventions
    - id: recommend_tests
      mode: reason
      instruction: |
        Recommend dbt tests for each generated model. Use 3 sources:

        1. COLUMN TYPES + NAMING:
           - *_id columns → not_null
           - Primary key → unique + not_null
           - Low cardinality columns (<20 distinct values) → accepted_values
           - Boolean columns → accepted_values [true, false]

        2. REFERENCE PIPELINE TESTS:
           - Mirror test patterns from the reference model's DBT_TESTS
           - If reference has reconciliation tests, generate equivalent
           - If reference has row_count_between tests, generate equivalent

        3. PROFILING RESULTS:
           - Column with 0% nulls in source → not_null
           - Column with cardinality == row_count → unique
           - PK candidate from profiling → unique + not_null

        For each test, include reasoning so the user understands why.
        Mark tests that come from the reference (from_reference: true).
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query DBT_TESTS for reference model test patterns"
      context:
        static: [testing_standards]
        dynamic:
          - from: generate_models
          - from: profile_source_data
            select: profiles
          - from: discover_reference_patterns
            select: reference_examples
      output_schema:
        type: object
        required: [recommended_tests]
        properties:
          recommended_tests:
            type: array
            items:
              type: object
              required: [model_name, test_name, column, reasoning]
              properties:
                model_name:
                  type: string
                test_name:
                  type: string
                column:
                  type: string
                config:
                  type: object
                reasoning:
                  type: string
                from_reference:
                  type: boolean
      budget:
        max_llm_turns: 4
        max_tokens: 8000
      transitions:
        - default: show_results
```

- [ ] **Step 5: Verify YAML syntax**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -c "
import yaml
from pathlib import Path
with open('content/workflows/create-etl-pipeline.yml') as f:
    wf = yaml.safe_load(f)
steps = wf['workflow']['steps']
print(f'Total steps: {len(steps)}')
for s in steps:
    print(f'  - {s[\"id\"]} (mode={s[\"mode\"]})')
"
```

Expected: 12 steps listed.

- [ ] **Step 6: Commit**

```bash
git add content/workflows/create-etl-pipeline.yml
git commit -m "feat: add generate_models, validate_pipeline, fix_validation_errors, recommend_tests steps"
```

---

## Chunk 3: Workflow YAML — Checkpoint 2, Edit Loop, PR, Orchestration (Steps 10-13)

### Task 4: Add remaining steps and manifest entry

**Files:**
- Modify: `content/workflows/create-etl-pipeline.yml`
- Modify: `content/workflows/manifest.yml`

- [ ] **Step 1: Add `show_results` checkpoint (Step 10)**

```yaml
    # Step 10: Checkpoint 2 — Show generated SQL, tests, validation for review
    - id: show_results
      mode: execute
      type: delegate
      delegate: shift.show_plan
      requires_approval: true
      context:
        dynamic:
          - from: generate_models
          - from: validate_pipeline
          - from: recommend_tests
          - from: design_pipeline
            select: dag_recommendation
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
            type: integer
      transitions:
        - when: "output.user_decision == 'edit'"
          goto: apply_changes
        - default: create_pr
```

- [ ] **Step 2: Add `apply_changes` edit loop step (Step 11)**

```yaml
    # Step 11: Apply user-requested changes (max 3 iterations, enforced by driver)
    - id: apply_changes
      mode: reason
      instruction: |
        Apply the user's requested change to the generated models.

        Read the edit_request from show_results. Common changes:
        - "Change dedup to no dedup" → remove the [DEDUP] CTE block
        - "Use LEFT JOIN instead of INNER" → modify join type
        - "Add a filter for active only" → add WHERE clause
        - "The conversion should be / 100" → fix calculation
        - "Add an accepted_values test" → add to test list

        Re-generate ONLY the affected model(s). Output the complete
        modified file content. Then re-validate the affected model.

        The Shift driver tracks edit_count. After 3 rounds, the driver
        returns user_decision="approve" and the workflow proceeds to PR.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Re-validate modified SQL"
        - name: github_cli.read_file
          instruction: "Re-read reference if needed"
      context:
        static: []
        dynamic:
          - from: show_results
            select: [edit_request, edit_count]
          - from: generate_models
          - from: validate_pipeline
          - from: discover_reference_patterns
            select: reference_examples
      output_schema:
        type: object
        required: [updated_models, validation_passed]
        properties:
          updated_models:
            type: array
            items:
              type: object
              required: [model_name, sql_content, change_description]
          validation_passed:
            type: boolean
      budget:
        max_llm_turns: 5
        max_tokens: 10000
      transitions:
        - default: show_results
```

- [ ] **Step 3: Add `create_pr` checkpoint (Step 12)**

```yaml
    # Step 12: Checkpoint 3 — Create PR with all artifacts
    - id: create_pr
      mode: execute
      type: delegate
      delegate: shift.create_pr
      requires_approval: true
      context:
        dynamic:
          - from: generate_models
          - from: recommend_tests
          - from: design_pipeline
          - from: validate_pipeline
            select: sample_output
          - from: profile_source_data
            select: [profiles, warnings]
```

- [ ] **Step 4: Add `recommend_orchestration` step (Step 13)**

```yaml
    # Step 13: Recommend DAG configuration and monitoring
    - id: recommend_orchestration
      mode: reason
      instruction: |
        Provide orchestration guidance for the new pipeline.

        1. Query DBT_INVOCATIONS for the recommended DAG's recent runs
           to confirm it exists and runs on the expected schedule
        2. Verify that the tags assigned to new models match the DAG's
           selector pattern
        3. Identify downstream models that will auto-rebuild after the
           new models are added (query DBT_MODEL_TABLE_LINEAGE)
        4. Recommend monitoring: watch first 3 runs in #ae-alerts

        Post a final summary to the Slack thread.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query DBT_INVOCATIONS and DBT_MODEL_TABLE_LINEAGE"
          usage_pattern: |
            1. SELECT job_name, COUNT(*), MAX(run_started_at)
               FROM DW.DATAOPS_DBT.DBT_INVOCATIONS
               WHERE job_name = '{dag_name}'
               GROUP BY 1
            2. SELECT DISTINCT node FROM DW.DATAOPS_DBT.DBT_MODEL_TABLE_LINEAGE
               WHERE root_table_name ILIKE '%{new_model}%' AND level = 1
      context:
        static: []
        dynamic:
          - from: design_pipeline
            select: dag_recommendation
          - from: generate_models
      output_schema:
        type: object
        required: [dag_name, schedule, selector_match, downstream_impact, monitoring_recommendation]
        properties:
          dag_name:
            type: string
          schedule:
            type: string
          selector_match:
            type: boolean
          tags:
            type: array
          downstream_impact:
            type: array
            items:
              type: object
              properties:
                model_name:
                  type: string
                relationship:
                  type: string
          monitoring_recommendation:
            type: string
      budget:
        max_llm_turns: 3
        max_tokens: 5000
```

- [ ] **Step 5: Add manifest entry**

Add to `content/workflows/manifest.yml` after the last entry:

```yaml
  - id: create-etl-pipeline
    name: Create ETL Pipeline
    persona: data_engineer
    triggers:
      keywords: [create pipeline, build pipeline, new pipeline, etl pipeline, productionize sql, new source, similar to, like this but for]
      input_pattern: "{request_text}"
```

- [ ] **Step 6: Verify complete workflow YAML**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -c "
import yaml
with open('content/workflows/create-etl-pipeline.yml') as f:
    wf = yaml.safe_load(f)
steps = wf['workflow']['steps']
print(f'Total steps: {len(steps)}')
for s in steps:
    tid = s.get('transitions', [])
    loop = 'LOOP' if 'loop' in s else ''
    delegate = s.get('delegate', '')
    print(f'  {s[\"id\"]:40s} mode={s[\"mode\"]:8s} {loop} {delegate} transitions={len(tid)}')
print(f'Inputs: {list(wf[\"workflow\"][\"inputs\"].keys())}')
print(f'Persona: {wf[\"workflow\"][\"persona\"]}')
"
```

Expected: 16 steps total.

- [ ] **Step 7: Try loading the workflow with the engine**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -c "
from pathlib import Path
from dcag import DCAGEngine
engine = DCAGEngine(content_dir=Path('content'))
wf = engine._registry.get_workflow('create-etl-pipeline')
print(f'Loaded: {wf.id}')
print(f'Steps: {len(wf.steps)}')
print(f'Step IDs: {[s.id for s in wf.steps]}')
print('OK — engine loads the workflow')
"
```

Expected: Workflow loads with 16 steps, no errors.

- [ ] **Step 8: Commit**

```bash
git add content/workflows/create-etl-pipeline.yml content/workflows/manifest.yml
git commit -m "feat: complete create-etl-pipeline workflow — all 16 steps + manifest entry"
```

---

## Chunk 4: Cassettes for `new_source` Path (Primary E2E Test)

### Task 5: Create cassettes for the `new_source` entry point

This is the primary test path: user says "Build a pipeline for fivetran_database.tiktok_ads.campaign_report"

**Files:**
- Create: `tests/cassettes/create-etl-pipeline-new-source/setup_environment.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/classify_intent.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/discover_source_schema.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/profile_source_data.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/discover_reference_patterns.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/design_pipeline.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/generate_models.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/validate_pipeline.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/recommend_tests.json`
- Create: `tests/cassettes/create-etl-pipeline-new-source/recommend_orchestration.json`

Each cassette follows the established format: `{ "output": {...}, "decision_log": {...}, "tool_calls": [...], "token_usage": {...} }`

- [ ] **Step 1: Create `setup_environment.json`**

```json
{
  "output": {
    "snowflake_available": true,
    "github_available": true
  },
  "decision_log": {
    "decision": "Both Snowflake and GitHub tools available",
    "rationale": "SELECT 1 succeeded on Snowflake, gh auth status succeeded",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["snowflake_mcp.execute_query", "github_cli.read_file"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT 1"},
    {"tool": "github_cli.read_file", "input": "models/mart/acquisition/stage/README.md"}
  ],
  "token_usage": {"prompt": 500, "completion": 100}
}
```

- [ ] **Step 2: Create `classify_intent.json`**

```json
{
  "output": {
    "entry_point": "new_source",
    "source_hints": ["fivetran_database.tiktok_ads.campaign_report"],
    "reference_model_hint": null,
    "domain_hint": "acquisition",
    "sql_text": null
  },
  "decision_log": {
    "decision": "Classified as new_source — user wants a pipeline for a specific Fivetran table",
    "rationale": "Request mentions a fully-qualified table name from Fivetran, no reference model or SQL",
    "alternatives_considered": ["extend_existing — but user didn't mention adding to an existing pipeline"],
    "confidence": "high",
    "informed_by": []
  },
  "tool_calls": [],
  "token_usage": {"prompt": 1000, "completion": 200}
}
```

- [ ] **Step 3: Create `discover_source_schema.json`**

```json
{
  "output": {
    "source_tables": [
      {
        "table_fqn": "FIVETRAN_DATABASE.TIKTOK_ADS.CAMPAIGN_REPORT",
        "columns": [
          {"name": "CAMPAIGN_ID", "type": "NUMBER", "nullable": false, "ordinal_position": 1},
          {"name": "STAT_TIME_DAY", "type": "DATE", "nullable": false, "ordinal_position": 2},
          {"name": "CAMPAIGN_NAME", "type": "VARCHAR", "nullable": true, "ordinal_position": 3},
          {"name": "IMPRESSIONS", "type": "NUMBER", "nullable": true, "ordinal_position": 4},
          {"name": "CLICKS", "type": "NUMBER", "nullable": true, "ordinal_position": 5},
          {"name": "SPEND", "type": "NUMBER", "nullable": true, "ordinal_position": 6},
          {"name": "CONVERSIONS", "type": "NUMBER", "nullable": true, "ordinal_position": 7},
          {"name": "COUNTRY_CODE", "type": "VARCHAR", "nullable": true, "ordinal_position": 8},
          {"name": "_FIVETRAN_SYNCED", "type": "TIMESTAMP_NTZ", "nullable": false, "ordinal_position": 9}
        ],
        "row_count": 12400000,
        "daily_volume": 15000,
        "freshness_hours": 2.1,
        "pk_candidates": ["CAMPAIGN_ID", "STAT_TIME_DAY"]
      }
    ],
    "existing_models": [],
    "reference_pipeline": {
      "root_model": "marketing_spend_day_country_agg",
      "layers": ["source", "intermediate", "mart", "mart_rollup"],
      "materializations": ["table", "table"],
      "tests": ["unique", "not_null", "row_count_between"],
      "grain": "campaign_id + date + country"
    },
    "conventions": {
      "naming_pattern": "stg_acquisition__{source}__{entity}",
      "schema": "ACQUISITION",
      "directory": "models/mart/acquisition/stage/"
    }
  },
  "decision_log": {
    "decision": "Found source table with 9 columns, identified marketing_spend as reference pipeline",
    "rationale": "DBT_MODEL_TABLE_LINEAGE shows marketing_spend_day_country_agg already aggregates similar channel sources (Meta, Apple, Google) in the acquisition schema",
    "alternatives_considered": ["campaign_day_agg — but that's for paid search, not multi-channel spend"],
    "confidence": "high",
    "informed_by": ["INFORMATION_SCHEMA", "DBT_MODELS", "DBT_MODEL_TABLE_LINEAGE"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM FIVETRAN_DATABASE.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'CAMPAIGN_REPORT'"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT name, depends_on_nodes FROM DW.DATAOPS_DBT.DBT_MODELS WHERE depends_on_nodes ILIKE '%tiktok%'"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT DISTINCT root_table_name, level, node FROM DW.DATAOPS_DBT.DBT_MODEL_TABLE_LINEAGE WHERE root_table_name ILIKE '%acquisition%' AND level <= 4 LIMIT 50"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT name, materialization, tags FROM DW.DATAOPS_DBT.DBT_MODELS WHERE schema_name = 'ACQUISITION' LIMIT 20"}
  ],
  "token_usage": {"prompt": 4000, "completion": 1500}
}
```

- [ ] **Step 4: Create `profile_source_data.json`**

```json
{
  "output": {
    "profiles": [
      {
        "table_fqn": "FIVETRAN_DATABASE.TIKTOK_ADS.CAMPAIGN_REPORT",
        "row_count": 12400000,
        "date_range": {"min": "2024-01-15", "max": "2026-03-12"},
        "daily_avg_rows": 15234,
        "freshness_hours": 2.1,
        "pk_candidate": {"columns": ["CAMPAIGN_ID", "STAT_TIME_DAY"], "is_unique": true},
        "null_flags": [
          {"column": "CAMPAIGN_NAME", "null_pct": 0.002},
          {"column": "COUNTRY_CODE", "null_pct": 0.0}
        ],
        "has_duplicates": false
      }
    ],
    "warnings": []
  },
  "decision_log": {
    "decision": "Source is healthy — no warnings",
    "rationale": "12.4M rows, 15K/day, 2h fresh, PK is unique, no significant nulls, no duplicates",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["snowflake_mcp.execute_query"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT COUNT(*), MIN(STAT_TIME_DAY), MAX(STAT_TIME_DAY) FROM FIVETRAN_DATABASE.TIKTOK_ADS.CAMPAIGN_REPORT"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT CAMPAIGN_ID, STAT_TIME_DAY, COUNT(*) FROM ... GROUP BY 1,2 HAVING COUNT(*) > 1 LIMIT 5"},
    {"tool": "snowflake_mcp.execute_query", "input": "NULL rate analysis per column"}
  ],
  "token_usage": {"prompt": 3000, "completion": 800}
}
```

- [ ] **Step 5: Create `discover_reference_patterns.json`**

```json
{
  "output": {
    "reference_examples": [
      {
        "model_name": "marketing_spend_day_country_agg",
        "layer": "mart",
        "raw_sql": "{{ config(materialized='table', schema='acquisition', tags=['acquisition', 'daily']) }}\n\nWITH google AS (\n  SELECT 'google' AS channel, ...\n),\nmeta AS (\n  SELECT 'meta' AS channel, ...\n),\nunioned AS (\n  SELECT * FROM google\n  UNION ALL\n  SELECT * FROM meta\n  UNION ALL ...\n)\nSELECT\n  {{ dbt_utils.generate_surrogate_key(['channel','date','country']) }} AS md5_key,\n  *\nFROM unioned",
        "compiled_sql": "CREATE TABLE ... AS WITH google AS ( SELECT 'google' AS channel, ... )",
        "config_block": "{{ config(materialized='table', schema='acquisition', tags=['acquisition', 'daily']) }}",
        "schema_yml": "models:\n  - name: marketing_spend_day_country_agg\n    columns:\n      - name: md5_key\n        tests: [unique, not_null]",
        "tests": ["unique(md5_key)", "not_null(md5_key)", "row_count_google_last_7d", "spend_usd_meta_daily_last_7d"],
        "original_path": "models/mart/acquisition/marketing_spend_day_country_agg.sql"
      },
      {
        "model_name": "marketing_spend_day_agg",
        "layer": "mart_rollup",
        "raw_sql": "{{ config(materialized='table', schema='acquisition', tags=['acquisition', 'daily']) }}\n\nSELECT\n  date,\n  channel,\n  SUM(spend_usd) AS spend_usd,\n  SUM(clicks) AS clicks,\n  SUM(impressions) AS impressions\nFROM {{ ref('marketing_spend_day_country_agg') }}\nGROUP BY 1, 2",
        "compiled_sql": "SELECT date, channel, SUM(spend_usd)... FROM DW.ACQUISITION.MARKETING_SPEND_DAY_COUNTRY_AGG GROUP BY 1,2",
        "config_block": "{{ config(materialized='table', schema='acquisition', tags=['acquisition', 'daily']) }}",
        "schema_yml": "",
        "tests": [],
        "original_path": "models/mart/acquisition/marketing_spend_day_agg.sql"
      }
    ],
    "domain_conventions": {
      "common_config_blocks": ["{{ config(materialized='table', schema='acquisition', tags=['acquisition', 'daily']) }}"],
      "naming_pattern": "stg_acquisition__{source} or {entity}_day_agg",
      "typical_materialization": "table",
      "directory_structure": "models/mart/acquisition/ and models/mart/acquisition/stage/"
    }
  },
  "decision_log": {
    "decision": "Read marketing_spend pipeline as reference — multi_source_union pattern with UNION ALL",
    "rationale": "Best match for adding a new channel source to existing multi-channel pipeline",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["github_cli.read_file"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/mart/acquisition/marketing_spend_day_country_agg.sql"},
    {"tool": "github_cli.read_file", "input": "models/mart/acquisition/marketing_spend_day_agg.sql"},
    {"tool": "github_cli.search_code", "input": "config(materialized in models/mart/acquisition/"}
  ],
  "token_usage": {"prompt": 8000, "completion": 3000}
}
```

- [ ] **Step 6: Create `design_pipeline.json`**

```json
{
  "output": {
    "pipeline_pattern": "multi_source_union",
    "models": [
      {
        "name": "stg_acquisition__tiktok_campaign_report",
        "layer": "staging",
        "materialization": "table",
        "depends_on": [],
        "schema": "ACQUISITION",
        "is_new": true,
        "incremental_config": null,
        "description": "Staging model for TikTok Ads campaign report from Fivetran — standardizes columns to match marketing_spend schema"
      },
      {
        "name": "marketing_spend_day_country_agg",
        "layer": "mart",
        "materialization": "table",
        "depends_on": ["stg_acquisition__tiktok_campaign_report"],
        "schema": "ACQUISITION",
        "is_new": false,
        "incremental_config": null,
        "description": "MODIFY: Add TikTok CTE block to existing UNION ALL (channel 7 of 7)"
      }
    ],
    "dag_recommendation": {
      "existing_dag": "transform_acquisition__daily",
      "tags": ["acquisition", "daily"],
      "schedule": "daily",
      "needs_new_dag": false
    },
    "skipped_models": [
      {"name": "marketing_spend_day_agg", "reason": "Downstream rollup — auto-rebuilds from country_agg, no changes needed"}
    ]
  },
  "decision_log": {
    "decision": "multi_source_union pattern — 1 new staging model + 1 modification to existing agg",
    "rationale": "marketing_spend already aggregates 6 channels via UNION ALL. Adding TikTok as channel 7 requires: (1) new staging model to standardize TikTok columns, (2) add TikTok CTE to marketing_spend_day_country_agg",
    "alternatives_considered": ["Standalone TikTok pipeline — rejected because marketing_spend is the canonical cross-channel table"],
    "confidence": "high",
    "informed_by": ["reference_examples", "profiling", "domain_conventions"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 5000, "completion": 1500}
}
```

- [ ] **Step 7: Create `generate_models.json`**

> Note: This cassette represents the accumulated output from the loop (both models).

```json
{
  "output": {
    "model_name": "stg_acquisition__tiktok_campaign_report",
    "sql_content": "{{ config(materialized='table', schema='acquisition', tags=['acquisition', 'daily']) }}\n\n-- [SOURCE] TikTok Ads campaign data from Fivetran\nWITH source AS (\n    SELECT * FROM {{ source('tiktok_ads', 'campaign_report') }}\n),\n\n-- [RENAME] Standardize to marketing_spend schema\nrenamed AS (\n    SELECT\n        campaign_id,\n        stat_time_day AS date,\n        campaign_name,\n        country_code AS country,\n        spend AS spend_usd,  -- TikTok reports in USD directly\n        clicks,\n        impressions,\n        conversions,\n        _fivetran_synced\n    FROM source\n),\n\n-- [FINAL]\nSELECT\n    {{ dbt_utils.generate_surrogate_key(['campaign_id', 'date', 'country']) }} AS md5_key,\n    'tiktok' AS channel,\n    *\nFROM renamed",
    "schema_yml_content": "models:\n  - name: stg_acquisition__tiktok_campaign_report\n    description: Staging model for TikTok Ads campaign report\n    columns:\n      - name: md5_key\n        description: Surrogate key (campaign_id + date + country)\n        tests:\n          - unique\n          - not_null\n      - name: campaign_id\n        tests:\n          - not_null\n      - name: date\n        tests:\n          - not_null",
    "sources_yml_content": "sources:\n  - name: tiktok_ads\n    database: FIVETRAN_DATABASE\n    schema: TIKTOK_ADS\n    tables:\n      - name: campaign_report\n        loaded_at_field: _fivetran_synced\n        freshness:\n          warn_after: {count: 24, period: hour}\n          error_after: {count: 48, period: hour}",
    "file_path": "models/mart/acquisition/stage/stg_acquisition__tiktok_campaign_report.sql",
    "schema_yml_path": "models/mart/acquisition/stage/schema.yml",
    "is_modification": false,
    "modification_description": null,
    "change_points": [
      {"id": "stg_tiktok__spend", "section": "spend conversion", "current": "No conversion — TikTok reports in USD", "alternatives": ["Divide by 100 if in cents", "Divide by 1M if in micros"]},
      {"id": "stg_tiktok__dedup", "section": "deduplication", "current": "No dedup — profiling showed 0 duplicates on PK", "alternatives": ["Add ROW_NUMBER dedup on campaign_id + date"]},
      {"id": "stg_tiktok__materialization", "section": "materialization", "current": "table (full refresh daily)", "alternatives": ["incremental with merge on md5_key"]}
    ]
  },
  "decision_log": {
    "decision": "Generated staging model + UNION ALL modification following marketing_spend pattern",
    "rationale": "Adapted marketing_spend_day_country_agg's channel CTE pattern for TikTok",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["reference_examples", "profiling"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/mart/acquisition/marketing_spend_day_country_agg.sql"}
  ],
  "token_usage": {"prompt": 8000, "completion": 3000}
}
```

- [ ] **Step 8: Create `validate_pipeline.json`**

```json
{
  "output": {
    "compiles": true,
    "warnings": [],
    "errors": [],
    "sample_output": {
      "columns": ["md5_key", "channel", "campaign_id", "date", "country", "spend_usd", "clicks", "impressions"],
      "rows": [
        ["a3f2e1...", "tiktok", 12345, "2026-03-12", "US", 1247.50, 3891, 89012],
        ["b7c4d2...", "tiktok", 12345, "2026-03-12", "GB", 523.80, 1456, 34567],
        ["c8e5f3...", "tiktok", 67890, "2026-03-12", "US", 892.10, 2734, 67890]
      ],
      "row_count": 15234
    }
  },
  "decision_log": {
    "decision": "All validation checks passed",
    "rationale": "SQL compiles, grain verified (0 dupes on md5_key), row count matches source daily avg (15K), no null PKs",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["snowflake_mcp.execute_query"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "CREATE TEMP TABLE ... AS SELECT ... LIMIT 0"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT md5_key, COUNT(*) HAVING COUNT(*) > 1"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM output LIMIT 5"}
  ],
  "token_usage": {"prompt": 4000, "completion": 1000}
}
```

- [ ] **Step 9: Create `recommend_tests.json`**

```json
{
  "output": {
    "recommended_tests": [
      {"model_name": "stg_acquisition__tiktok_campaign_report", "test_name": "unique", "column": "md5_key", "config": {}, "reasoning": "PK from profiling — 0 duplicates on campaign_id + date + country", "from_reference": false},
      {"model_name": "stg_acquisition__tiktok_campaign_report", "test_name": "not_null", "column": "md5_key", "config": {}, "reasoning": "PK should never be null", "from_reference": false},
      {"model_name": "stg_acquisition__tiktok_campaign_report", "test_name": "not_null", "column": "campaign_id", "config": {}, "reasoning": "0% nulls in source profiling", "from_reference": false},
      {"model_name": "stg_acquisition__tiktok_campaign_report", "test_name": "not_null", "column": "date", "config": {}, "reasoning": "0% nulls in source profiling", "from_reference": false},
      {"model_name": "marketing_spend_day_country_agg", "test_name": "row_count_tiktok_last_7d", "column": null, "config": {"min_value": 1000}, "reasoning": "Mirrored from Google/Meta channel tests — ensures TikTok data flows daily", "from_reference": true},
      {"model_name": "marketing_spend_day_country_agg", "test_name": "spend_usd_tiktok_daily_last_7d", "column": null, "config": {"min_value": 100}, "reasoning": "Mirrored from Meta/Partnerize spend validation — ensures non-zero TikTok spend", "from_reference": true}
    ]
  },
  "decision_log": {
    "decision": "6 tests recommended — 4 from profiling, 2 mirrored from reference",
    "rationale": "Grain tests from profiling + channel-specific tests from marketing_spend's existing test pattern",
    "alternatives_considered": ["accepted_values on country_code — skipped, too many countries"],
    "confidence": "high",
    "informed_by": ["profiling", "DBT_TESTS"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT short_name, test_column_name FROM DW.DATAOPS_DBT.DBT_TESTS WHERE parent_model_unique_id ILIKE '%marketing_spend%'"}
  ],
  "token_usage": {"prompt": 3000, "completion": 800}
}
```

- [ ] **Step 10: Create `recommend_orchestration.json`**

```json
{
  "output": {
    "dag_name": "transform_acquisition__daily",
    "schedule": "daily at 06:00 UTC",
    "selector_match": true,
    "tags": ["acquisition", "daily"],
    "downstream_impact": [
      {"model_name": "marketing_spend_day_agg", "relationship": "direct downstream — auto-rebuilds from country_agg"}
    ],
    "monitoring_recommendation": "Monitor first 3 runs in #ae-alerts. Watch for: TikTok row count dropping to 0 (source freshness issue), spend_usd anomalies (currency conversion), grain violations (duplicate campaign_id+date+country). The triage workflow will auto-investigate if tests fail."
  },
  "decision_log": {
    "decision": "transform_acquisition__daily is the correct DAG — tags match, schedule aligns",
    "rationale": "DBT_INVOCATIONS shows 24 runs of this DAG in last 30 days, all with acquisition+daily selector",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["DBT_INVOCATIONS"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT job_name, COUNT(*) FROM DW.DATAOPS_DBT.DBT_INVOCATIONS WHERE job_name ILIKE '%acquisition%daily%' GROUP BY 1"}
  ],
  "token_usage": {"prompt": 2000, "completion": 500}
}
```

- [ ] **Step 11: Commit cassettes**

```bash
git add tests/cassettes/create-etl-pipeline-new-source/
git commit -m "test: add cassettes for create-etl-pipeline new_source path (TikTok Ads scenario)"
```

---

## Chunk 5: E2E Tests for `new_source` Path

### Task 6: Create E2E test file with new_source test class

**Files:**
- Create: `tests/test_e2e_create_etl_pipeline.py`

- [ ] **Step 1: Create test file with step constants and helper**

```python
"""
End-to-end test for the create-etl-pipeline workflow.

Tests the full 16-step workflow with 4 ENTRY POINT BRANCHES at classify_intent.
Each test class exercises a different entry point:
  - TestNewSource: classify -> discover_source_schema -> profile -> design -> generate -> validate -> orchestration
  - TestSimilarTo: classify -> trace_reference_pipeline -> profile -> design -> generate -> validate -> orchestration
  - TestSqlToPipeline: classify -> parse_sql_sources -> profile -> design -> generate -> validate -> orchestration
  - TestExtendExisting: classify -> analyze_target_pipeline -> profile -> design -> generate -> validate -> orchestration
"""
import json
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.types import (
    ReasonRequest,
    DelegateRequest,
    StepSuccess,
)


CONTENT_DIR = Path(__file__).parent.parent / "content"

# Steps for new_source path (discovery via discover_source_schema)
NEW_SOURCE_STEPS = [
    "setup_environment",
    "classify_intent",
    "discover_source_schema",
    "profile_source_data",
    "discover_reference_patterns",
    "design_pipeline",
    "confirm_plan",
    "generate_models",
    "validate_pipeline",
    "recommend_tests",
    "show_results",
    "create_pr",
    "recommend_orchestration",
]

# Steps for similar_to path (discovery via trace_reference_pipeline)
SIMILAR_TO_STEPS = [
    "setup_environment",
    "classify_intent",
    "trace_reference_pipeline",
    "profile_source_data",
    "discover_reference_patterns",
    "design_pipeline",
    "confirm_plan",
    "generate_models",
    "validate_pipeline",
    "recommend_tests",
    "show_results",
    "create_pr",
    "recommend_orchestration",
]

# Steps for sql_to_pipeline path (discovery via parse_sql_sources)
SQL_TO_PIPELINE_STEPS = [
    "setup_environment",
    "classify_intent",
    "parse_sql_sources",
    "profile_source_data",
    "discover_reference_patterns",
    "design_pipeline",
    "confirm_plan",
    "generate_models",
    "validate_pipeline",
    "recommend_tests",
    "show_results",
    "create_pr",
    "recommend_orchestration",
]

# Steps for extend_existing path (discovery via analyze_target_pipeline)
EXTEND_EXISTING_STEPS = [
    "setup_environment",
    "classify_intent",
    "analyze_target_pipeline",
    "profile_source_data",
    "discover_reference_patterns",
    "design_pipeline",
    "confirm_plan",
    "generate_models",
    "validate_pipeline",
    "recommend_tests",
    "show_results",
    "create_pr",
    "recommend_orchestration",
]


def load_cassettes(cassette_dir: Path, reason_steps: list[str]) -> dict[str, dict]:
    """Load cassettes for the steps that will actually execute."""
    cassettes = {}
    for step_id in reason_steps:
        path = cassette_dir / f"{step_id}.json"
        if path.exists():
            with open(path) as f:
                cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(
    cassette_dir: Path,
    inputs: dict,
    expected_steps: list[str],
) -> tuple:
    """Drive the workflow with cassette responses, handling branching and delegation."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    reason_steps = [s for s in expected_steps if s not in ("confirm_plan", "show_results", "create_pr")]
    cassettes = load_cassettes(cassette_dir, reason_steps)

    run = engine.start("create-etl-pipeline", inputs)
    assert run.status == "running"

    steps_executed = []
    reason_outputs = {}

    while run.status == "running":
        request = run.next_step()
        if request is None:
            break

        steps_executed.append(request.step_id)

        if isinstance(request, ReasonRequest):
            cassette = cassettes[request.step_id]
            reason_outputs[request.step_id] = cassette["output"]
            run.record_result(
                request.step_id,
                StepSuccess(output=cassette["output"]),
            )

        elif isinstance(request, DelegateRequest):
            if request.step_id == "confirm_plan":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"user_decision": "approve", "feedback": ""}),
                )
            elif request.step_id == "show_results":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"user_decision": "approve", "edit_request": "", "edit_count": 0}),
                )
            elif request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer/pull/123", "pr_number": 123}),
                )

    return run, steps_executed, reason_outputs


class TestNewSource:
    """New source path: setup -> classify -> discover_source -> profile -> reference -> design -> confirm -> generate -> validate -> tests -> show -> PR -> orchestration."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-new-source"
    INPUTS = {"request_text": "Build a pipeline for fivetran_database.tiktok_ads.campaign_report"}

    def test_workflow_takes_new_source_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert run.status == "completed"
        assert steps_executed == NEW_SOURCE_STEPS

    def test_new_source_path_step_count(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert len(steps_executed) == 13

    def test_classify_returns_new_source(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "new_source"

    def test_source_discovered_correctly(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        sources = reason_outputs["discover_source_schema"]["source_tables"]
        assert len(sources) == 1
        assert "TIKTOK" in sources[0]["table_fqn"]

    def test_profiling_no_warnings(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["profile_source_data"]["warnings"] == []

    def test_pipeline_pattern_is_multi_source_union(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["design_pipeline"]["pipeline_pattern"] == "multi_source_union"

    def test_design_has_2_models(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        models = reason_outputs["design_pipeline"]["models"]
        assert len(models) == 2
        assert models[0]["is_new"] is True
        assert models[1]["is_new"] is False  # modification to existing

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert reason_outputs["validate_pipeline"]["compiles"] is True
        assert reason_outputs["validate_pipeline"]["errors"] == []

    def test_tests_recommended(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        tests = reason_outputs["recommend_tests"]["recommended_tests"]
        assert len(tests) >= 4
        test_names = [t["test_name"] for t in tests]
        assert "unique" in test_names
        assert "not_null" in test_names

    def test_orchestration_uses_existing_dag(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        orch = reason_outputs["recommend_orchestration"]
        assert orch["dag_name"] == "transform_acquisition__daily"
        assert orch["selector_match"] is True

    def test_skips_other_discovery_branches(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        assert "trace_reference_pipeline" not in steps_executed
        assert "parse_sql_sources" not in steps_executed
        assert "analyze_target_pipeline" not in steps_executed

    def test_trace_records_all_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "create-etl-pipeline"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "discover_source_schema" in step_ids
        assert "trace_reference_pipeline" not in step_ids

    def test_generated_sql_has_tiktok_source(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        sql = reason_outputs["generate_models"]["sql_content"]
        assert "tiktok_ads" in sql
        assert "campaign_report" in sql

    def test_change_points_provided(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, NEW_SOURCE_STEPS)
        cps = reason_outputs["generate_models"]["change_points"]
        assert len(cps) >= 2
        assert any("spend" in cp["section"].lower() for cp in cps)
```

- [ ] **Step 2: Run tests**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_e2e_create_etl_pipeline.py -v --tb=short
```

Expected: All TestNewSource tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_e2e_create_etl_pipeline.py
git commit -m "test: E2E tests for create-etl-pipeline new_source path"
```

---

## Chunk 6: Remaining Branches, Conformance, and Full Verification

### Task 7: Create cassettes for the other 3 entry points

**Files:**
- Create: `tests/cassettes/create-etl-pipeline-similar-to/*.json`
- Create: `tests/cassettes/create-etl-pipeline-sql-to-pipeline/*.json`
- Create: `tests/cassettes/create-etl-pipeline-extend-existing/*.json`

Each directory needs cassettes for the same step IDs as `new_source`, but with the appropriate discovery branch:
- `similar_to` uses `trace_reference_pipeline.json` instead of `discover_source_schema.json`
- `sql_to_pipeline` uses `parse_sql_sources.json` instead of `discover_source_schema.json`
- `extend_existing` uses `analyze_target_pipeline.json` instead of `discover_source_schema.json`

Shared steps (`setup_environment`, `profile_source_data`, `discover_reference_patterns`, `design_pipeline`, `generate_models`, `validate_pipeline`, `recommend_tests`, `recommend_orchestration`) can reuse similar content with scenario-appropriate adjustments.

- [ ] **Step 1: Create `similar_to` cassettes**

Scenario: "Build something like campaign_day_agg but for affiliate traffic"
- `classify_intent.json`: entry_point = "similar_to", reference_model_hint = "campaign_day_agg"
- `trace_reference_pipeline.json`: traces campaign_day_agg lineage (5 layers, hourly_rollup pattern)
- Other cassettes: adapted for affiliate domain with `hourly_rollup` pattern

- [ ] **Step 2: Create `sql_to_pipeline` cassettes**

Scenario: "Make this a proper pipeline: SELECT campaign_id, SUM(spend) FROM fivetran_database.tiktok_ads.campaign_report GROUP BY 1"
- `classify_intent.json`: entry_point = "sql_to_pipeline", sql_text = the SQL
- `parse_sql_sources.json`: parses SQL, finds 1 source table, maps CTE to staging+final
- Other cassettes: adapted for standard pattern

- [ ] **Step 3: Create `extend_existing` cassettes**

Scenario: "Add TikTok as a new channel to marketing_spend_day_country_agg"
- `classify_intent.json`: entry_point = "extend_existing", reference_model_hint = "marketing_spend_day_country_agg"
- `analyze_target_pipeline.json`: reads existing SQL, identifies UNION ALL insertion point
- Other cassettes: design shows 1 modification + 1 new staging model

- [ ] **Step 4: Add test classes for remaining 3 branches**

Add to `tests/test_e2e_create_etl_pipeline.py`:

```python
class TestSimilarTo:
    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-similar-to"
    INPUTS = {"request_text": "Build something like campaign_day_agg but for affiliate traffic"}

    def test_workflow_takes_similar_to_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert run.status == "completed"
        assert steps_executed == SIMILAR_TO_STEPS

    def test_classify_returns_similar_to(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "similar_to"

    def test_uses_trace_reference_not_discover(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SIMILAR_TO_STEPS)
        assert "trace_reference_pipeline" in steps_executed
        assert "discover_source_schema" not in steps_executed


class TestSqlToPipeline:
    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-sql-to-pipeline"
    INPUTS = {
        "request_text": "Make this a proper pipeline",
        "sql_text": "SELECT campaign_id, SUM(spend) AS total_spend FROM fivetran_database.tiktok_ads.campaign_report GROUP BY 1",
    }

    def test_workflow_takes_sql_to_pipeline_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert run.status == "completed"
        assert steps_executed == SQL_TO_PIPELINE_STEPS

    def test_classify_returns_sql_to_pipeline(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, SQL_TO_PIPELINE_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "sql_to_pipeline"


class TestExtendExisting:
    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-etl-pipeline-extend-existing"
    INPUTS = {"request_text": "Add TikTok as a new channel to marketing_spend_day_country_agg"}

    def test_workflow_takes_extend_existing_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert run.status == "completed"
        assert steps_executed == EXTEND_EXISTING_STEPS

    def test_classify_returns_extend_existing(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert reason_outputs["classify_intent"]["entry_point"] == "extend_existing"

    def test_uses_analyze_target_not_discover(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, EXTEND_EXISTING_STEPS)
        assert "analyze_target_pipeline" in steps_executed
        assert "discover_source_schema" not in steps_executed
```

- [ ] **Step 5: Commit**

```bash
git add tests/cassettes/create-etl-pipeline-similar-to/ \
        tests/cassettes/create-etl-pipeline-sql-to-pipeline/ \
        tests/cassettes/create-etl-pipeline-extend-existing/ \
        tests/test_e2e_create_etl_pipeline.py
git commit -m "test: E2E tests and cassettes for all 4 entry point branches"
```

---

### Task 8: Create conformance spec and tests

**Files:**
- Create: `content/workflows/create-etl-pipeline.test.yml`
- Create: `tests/test_conformance_create_etl_pipeline.py`

- [ ] **Step 1: Create conformance spec YAML**

Write `content/workflows/create-etl-pipeline.test.yml` with an entry for each of the 16 steps, following the triage-ae-alert.test.yml pattern. Each entry specifies: `type`, `persona`, `tools_include`, `knowledge_includes`, `dynamic_refs_from`, `has_instruction`.

- [ ] **Step 2: Create conformance test**

Write `tests/test_conformance_create_etl_pipeline.py` following the `test_conformance_triage_ae_alert.py` pattern. Walk the `new_source` path with mock outputs, validate each step matches the conformance spec.

- [ ] **Step 3: Run conformance tests**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_conformance_create_etl_pipeline.py -v --tb=short
```

- [ ] **Step 4: Commit**

```bash
git add content/workflows/create-etl-pipeline.test.yml tests/test_conformance_create_etl_pipeline.py
git commit -m "test: conformance spec and tests for create-etl-pipeline"
```

---

### Task 9: Full test suite verification

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: All tests pass. No regressions in existing workflows.

- [ ] **Step 2: Verify workflow loads with correct step count**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -c "
from pathlib import Path
from dcag import DCAGEngine
engine = DCAGEngine(content_dir=Path('content'))
wf = engine._registry.get_workflow('create-etl-pipeline')
print(f'Steps: {len(wf.steps)}')
print(f'Step IDs: {[s.id for s in wf.steps]}')
assert len(wf.steps) == 16
print('OK')
"
```

- [ ] **Step 3: Verify manifest lists the new workflow**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -c "
import yaml
with open('content/workflows/manifest.yml') as f:
    m = yaml.safe_load(f)
ids = [w['id'] for w in m['workflows']]
assert 'create-etl-pipeline' in ids
print(f'Workflows: {ids}')
print('OK — create-etl-pipeline in manifest')
"
```

- [ ] **Step 4: Final commit if any fixups needed**

```bash
git add content/ tests/
git commit -m "fix: address issues found during full verification"
```
