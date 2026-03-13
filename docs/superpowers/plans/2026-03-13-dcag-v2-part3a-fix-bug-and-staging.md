# DCAG v2 Implementation Plan — Part 3a: fix-model-bug + create-staging-model

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax.

**Goal:** Build fix-model-bug (conditional branching) and create-staging-model (schema cache) workflows.

**Prerequisites:** Part 1 engine features (conditional walker, schema cache) must be built first.

**Tech Stack:** Python 3.14, dataclasses, PyYAML, pytest

---

## Task 1: Workflow — fix-model-bug

### 1.1 — Workflow Definition

- [ ] Create `content/workflows/fix-model-bug.yml`

```yaml
workflow:
  id: fix-model-bug
  name: Fix Bug in dbt Model
  persona: analytics_engineer

  inputs:
    model_name:
      type: string
      required: true
    error_message:
      type: string
      required: false

  steps:
    # Step 0: Parse the error message to extract clues
    - id: parse_error
      mode: reason
      instruction: |
        Parse the error message (if provided) or query recent dbt run results
        to find the failure. Extract:
        1. Error type (compilation, runtime, data type mismatch, join failure)
        2. Failing column/expression
        3. Source table involved
        4. Line number or SQL fragment if available

        If no error_message is provided, query DW.DATAOPS_DBT.DBT_RUN_RESULTS
        for the most recent failure for this model.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query DBT_RUN_RESULTS for recent failures if no error_message provided"
          usage_pattern: |
            1. SELECT * FROM DW.DATAOPS_DBT.DBT_RUN_RESULTS
               WHERE NAME = '{model_name}' AND STATUS = 'error'
               ORDER BY CREATED_AT DESC LIMIT 5
      context:
        static: [troubleshooting_patterns]
        dynamic: []
      output_schema:
        type: object
        required: [error_type, failing_expression, source_table, model_name]
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 1: Read the model SQL and understand its structure
    - id: read_model_sql
      mode: reason
      instruction: |
        Read the model SQL file from GitHub. Understand:
        1. CTE structure and dependencies
        2. Source references (ref/source)
        3. Column definitions and transformations
        4. Join conditions
        5. WHERE clause filters

        Also get dbt node details to understand materialization,
        tags, and upstream dependencies.
      tools:
        - name: github_cli.read_file
          instruction: "Read the model SQL file from the Astronomer repo"
        - name: dbt_mcp.get_node_details_dev
          instruction: "Get model metadata: materialization, tags, dependencies"
      context:
        static: [troubleshooting_patterns]
        dynamic:
          - from: parse_error
            select: model_name
      output_schema:
        type: object
        required: [model_path, model_sql, cte_structure, source_refs, materialization]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 2: Classify the bug type for conditional routing
    - id: classify_bug_type
      mode: reason
      instruction: |
        Based on the error message and model SQL, classify the bug into one of:

        - cast_error: Data type mismatch, invalid cast, numeric overflow,
          TRY_CAST needed, VARCHAR-to-NUMBER failures
        - join_error: Join key mismatch, fanout/duplication from joins,
          missing join condition, NULL join keys
        - logic_error: Wrong filter logic, incorrect date range, missing
          WHERE clause, wrong aggregation, incorrect CASE WHEN

        Consider the error message pattern, the SQL structure, and common
        failure modes for each type. Output a single bug_type classification
        with confidence and rationale.
      tools: []
      context:
        static: [troubleshooting_patterns, data_quality_checks]
        dynamic:
          - from: parse_error
          - from: read_model_sql
      output_schema:
        type: object
        required: [bug_type, confidence, rationale, failing_component]
        properties:
          bug_type:
            type: string
            enum: [cast_error, join_error, logic_error]
      budget:
        max_llm_turns: 2
        max_tokens: 5000
      transitions:
        - when: "output.bug_type == 'cast_error'"
          goto: fix_cast_error
        - when: "output.bug_type == 'join_error'"
          goto: fix_join_error
        - default: fix_logic_error

    # Step 3a: Fix cast/type errors
    - id: fix_cast_error
      mode: reason
      instruction: |
        Fix the data type casting error. Common fixes:
        1. Replace CAST with TRY_CAST for nullable/dirty data
        2. Add NULLIF to handle empty strings before casting
        3. Use TRY_TO_NUMBER, TRY_TO_DATE for safe conversions
        4. Add explicit type coercion in CTE before the failing expression
        5. Handle NULL propagation in numeric expressions

        Read the failing SQL, identify the exact cast expression, and produce
        the corrected SQL. Verify the fix by running a sample query against
        the source data to confirm the problematic values are handled.

        Output the complete modified SQL file content (not a diff).
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Sample the source data to find the problematic values causing cast failures"
          usage_pattern: |
            1. SELECT DISTINCT {failing_column} FROM {source_table}
               WHERE TRY_CAST({failing_column} AS {target_type}) IS NULL
               AND {failing_column} IS NOT NULL LIMIT 20
      context:
        static: [troubleshooting_patterns]
        dynamic:
          - from: read_model_sql
          - from: classify_bug_type
      anti_patterns:
        - "Don't just wrap everything in TRY_CAST — fix the root cause if possible"
        - "Don't swallow NULLs silently — add a comment explaining the fix"
      output_schema:
        type: object
        required: [fixed_sql, fix_description, problematic_values]
      budget:
        max_llm_turns: 5
        max_tokens: 12000

    # Step 3b: Fix join errors
    - id: fix_join_error
      mode: reason
      instruction: |
        Fix the join-related error. Common fixes:
        1. Add missing join condition to prevent fanout
        2. Fix join key type mismatch (e.g., VARCHAR vs NUMBER)
        3. Add COALESCE or NVL for NULL join keys
        4. Change JOIN type (INNER vs LEFT) based on data reality
        5. Add deduplication CTE before joining

        Investigate both sides of the join: describe both tables, check
        key cardinality, and verify the join produces the expected row count.

        Output the complete modified SQL file content (not a diff).
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Check join key cardinality and type on both sides"
          usage_pattern: |
            1. SELECT COUNT(*), COUNT(DISTINCT {join_key}) FROM {left_table}
            2. SELECT COUNT(*), COUNT(DISTINCT {join_key}) FROM {right_table}
            3. SELECT typeof({join_key}) FROM {table} LIMIT 1
        - name: snowflake_mcp.describe_table
          instruction: "Describe both tables involved in the join to verify column types"
      context:
        static: [troubleshooting_patterns]
        dynamic:
          - from: read_model_sql
          - from: classify_bug_type
      anti_patterns:
        - "Don't just switch to LEFT JOIN to hide missing data — understand why"
        - "Don't add DISTINCT to mask a fanout — fix the join condition"
      output_schema:
        type: object
        required: [fixed_sql, fix_description, join_analysis]
      budget:
        max_llm_turns: 5
        max_tokens: 12000

    # Step 3c: Fix logic errors
    - id: fix_logic_error
      mode: reason
      instruction: |
        Fix the logic error. Common fixes:
        1. Correct WHERE clause filter (wrong date range, wrong operator)
        2. Fix CASE WHEN logic (missing ELSE, overlapping conditions)
        3. Correct aggregation (wrong GROUP BY, missing column)
        4. Fix window function (wrong PARTITION BY or ORDER BY)
        5. Correct incremental logic (missing is_incremental() guard)

        Run diagnostic queries to understand the data and verify the fix
        produces correct results. Compare output before and after the fix.

        Output the complete modified SQL file content (not a diff).
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Run diagnostic queries to understand the data and verify the fix"
          usage_pattern: |
            1. Run the original failing query fragment to see the error
            2. Run the corrected query fragment to verify it works
            3. Compare row counts before/after
      context:
        static: [troubleshooting_patterns]
        dynamic:
          - from: read_model_sql
          - from: classify_bug_type
      output_schema:
        type: object
        required: [fixed_sql, fix_description, diagnostic_results]
      budget:
        max_llm_turns: 5
        max_tokens: 12000

    # Step 4: Validate the fix
    - id: validate_fix
      mode: reason
      instruction: |
        Validate the fix using the full dbt-MCP validation suite:
        1. dbt_mcp.compile — check SQL compiles
        2. dbt_mcp.test — run tests for this model
        3. dbt_mcp.show — preview output rows
        4. Snowflake spot-check — verify the previously-failing query now works

        The fix came from one of three possible branches (fix_cast_error,
        fix_join_error, or fix_logic_error). Use whichever produced output —
        the others will be absent. Look for fixed_sql in the available context.
      tools:
        - name: dbt_mcp.compile
          instruction: "Compile the fixed model SQL"
        - name: dbt_mcp.test
          instruction: "Run dbt test --select {model_name}"
        - name: dbt_mcp.show
          instruction: "Preview output rows for the fixed model"
        - name: snowflake_mcp.execute_query
          instruction: "Re-run the previously-failing query to confirm the fix"
      context:
        static: []
        dynamic:
          - from: fix_cast_error
          - from: fix_join_error
          - from: fix_logic_error
      output_schema:
        type: object
        required: [compile_ok, tests_passed, preview_rows, fix_verified]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 5: Create PR
    - id: create_pr
      mode: execute
      type: delegate
      delegate: shift.create_pr
      requires_approval: true
      context:
        dynamic:
          - from: classify_bug_type
            select: [bug_type, rationale]
          - from: fix_cast_error
          - from: fix_join_error
          - from: fix_logic_error
          - from: validate_fix
            select: [compile_ok, tests_passed, fix_verified]
```

### 1.2 — Conformance Test Spec

- [ ] Create `content/workflows/fix-model-bug.test.yml`

```yaml
conformance:
  workflow_id: fix-model-bug

  steps:
    parse_error:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - troubleshooting_patterns
      tools_include:
        - snowflake_mcp.execute_query
      has_instruction: true

    read_model_sql:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - troubleshooting_patterns
      tools_include:
        - github_cli.read_file
        - dbt_mcp.get_node_details_dev
      dynamic_refs_from:
        - parse_error
      has_instruction: true

    classify_bug_type:
      type: ReasonRequest
      persona: analytics_engineer
      tools_count: 0
      knowledge_includes:
        - troubleshooting_patterns
        - data_quality_checks
      dynamic_refs_from:
        - parse_error
        - read_model_sql
      has_instruction: true

    fix_cast_error:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - troubleshooting_patterns
      tools_include:
        - snowflake_mcp.execute_query
      dynamic_refs_from:
        - read_model_sql
        - classify_bug_type
      has_instruction: true

    fix_join_error:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - troubleshooting_patterns
      tools_include:
        - snowflake_mcp.execute_query
        - snowflake_mcp.describe_table
      dynamic_refs_from:
        - read_model_sql
        - classify_bug_type
      has_instruction: true

    fix_logic_error:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - troubleshooting_patterns
      tools_include:
        - snowflake_mcp.execute_query
      dynamic_refs_from:
        - read_model_sql
        - classify_bug_type
      has_instruction: true

    validate_fix:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - dbt_mcp.compile
        - dbt_mcp.test
        - dbt_mcp.show
        - snowflake_mcp.execute_query
      dynamic_refs_from:
        - fix_cast_error
        - fix_join_error
        - fix_logic_error
      has_instruction: true

    create_pr:
      type: DelegateRequest
      requires_approval: true
```

### 1.3 — Cassette Files (Cast Error Path)

- [ ] Create `tests/cassettes/fix-model-bug-cast/parse_error.json`

```json
{
  "output": {
    "error_type": "runtime",
    "failing_expression": "CAST(TICKET_PRICE AS NUMBER(18,2))",
    "source_table": "DW.STG.STG_TICKET_LISTING",
    "model_name": "stg_ticket_listing"
  },
  "decision_log": {
    "decision": "Extracted cast failure from error message",
    "rationale": "Error message contains 'Numeric value is not recognized' pointing to CAST expression on TICKET_PRICE column. Source table is STG_TICKET_LISTING.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["error_message"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 800, "completion": 300}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-cast/read_model_sql.json`

```json
{
  "output": {
    "model_path": "models/staging/ticket/stg_ticket_listing.sql",
    "model_sql": "WITH source AS (\n    SELECT * FROM {{ source('ticket', 'TICKET_LISTING') }}\n),\nfinal AS (\n    SELECT\n        LISTING_ID,\n        EVENT_ID,\n        CAST(TICKET_PRICE AS NUMBER(18,2)) AS ticket_price,\n        CAST(FACE_VALUE AS NUMBER(18,2)) AS face_value,\n        SECTION_NAME,\n        ROW_NAME,\n        QUANTITY,\n        CREATED_AT\n    FROM source\n)\nSELECT * FROM final",
    "cte_structure": ["source", "final"],
    "source_refs": ["{{ source('ticket', 'TICKET_LISTING') }}"],
    "materialization": "view"
  },
  "decision_log": {
    "decision": "Read model SQL and identified CTE structure",
    "rationale": "Model has 2 CTEs (source, final). CAST expressions on TICKET_PRICE and FACE_VALUE are the likely failure points. Model is a view materialization.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["github_cli.read_file", "dbt_mcp.get_node_details_dev"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/staging/ticket/stg_ticket_listing.sql"},
    {"tool": "dbt_mcp.get_node_details_dev", "input": "stg_ticket_listing"}
  ],
  "token_usage": {"prompt": 1200, "completion": 500}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-cast/classify_bug_type.json`

```json
{
  "output": {
    "bug_type": "cast_error",
    "confidence": "high",
    "rationale": "Error message indicates 'Numeric value is not recognized' on CAST(TICKET_PRICE AS NUMBER). Source data likely contains non-numeric values (empty strings, currency symbols, or text) in the TICKET_PRICE column.",
    "failing_component": "CAST(TICKET_PRICE AS NUMBER(18,2))"
  },
  "decision_log": {
    "decision": "Classified as cast_error",
    "rationale": "Clear pattern match: runtime error with 'Numeric value' in message, CAST expression in SQL, non-numeric source data.",
    "alternatives_considered": [
      {"option": "logic_error", "reason": "Could be wrong column, but error message points to cast"}
    ],
    "confidence": "high",
    "informed_by": ["error_message", "model_sql"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 1500, "completion": 300}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-cast/fix_cast_error.json`

```json
{
  "output": {
    "fixed_sql": "WITH source AS (\n    SELECT * FROM {{ source('ticket', 'TICKET_LISTING') }}\n),\nfinal AS (\n    SELECT\n        LISTING_ID,\n        EVENT_ID,\n        TRY_CAST(NULLIF(TRIM(TICKET_PRICE), '') AS NUMBER(18,2)) AS ticket_price,\n        TRY_CAST(NULLIF(TRIM(FACE_VALUE), '') AS NUMBER(18,2)) AS face_value,\n        SECTION_NAME,\n        ROW_NAME,\n        QUANTITY,\n        CREATED_AT\n    FROM source\n)\nSELECT * FROM final",
    "fix_description": "Replaced CAST with TRY_CAST and added NULLIF(TRIM(...), '') to handle empty strings and whitespace in TICKET_PRICE and FACE_VALUE columns. TRY_CAST returns NULL instead of failing on non-numeric values.",
    "problematic_values": ["", " ", "$150.00", "N/A", "TBD"]
  },
  "decision_log": {
    "decision": "Applied TRY_CAST + NULLIF fix pattern",
    "rationale": "Source data contains empty strings, whitespace, currency symbols ($150.00), and text values (N/A, TBD). TRY_CAST safely handles all cases by returning NULL. NULLIF handles empty strings specifically. TRIM handles whitespace.",
    "alternatives_considered": [
      {"option": "WHERE filter to exclude bad rows", "reason": "Would lose data — TRY_CAST is safer"},
      {"option": "REGEXP_REPLACE to strip non-numeric", "reason": "Overkill for this case, TRY_CAST is simpler"}
    ],
    "confidence": "high",
    "informed_by": ["snowflake_mcp.execute_query"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT DISTINCT TICKET_PRICE FROM DW.STG.STG_TICKET_LISTING WHERE TRY_CAST(TICKET_PRICE AS NUMBER(18,2)) IS NULL AND TICKET_PRICE IS NOT NULL LIMIT 20"}
  ],
  "token_usage": {"prompt": 2000, "completion": 600}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-cast/validate_fix.json`

```json
{
  "output": {
    "compile_ok": true,
    "tests_passed": true,
    "preview_rows": [
      {"listing_id": 1001, "ticket_price": 150.00, "face_value": 75.00},
      {"listing_id": 1002, "ticket_price": null, "face_value": 100.00},
      {"listing_id": 1003, "ticket_price": 299.99, "face_value": null}
    ],
    "fix_verified": true
  },
  "decision_log": {
    "decision": "Fix validated — model compiles, tests pass, data looks correct",
    "rationale": "dbt compile succeeded. dbt test passed (not_null on LISTING_ID, unique on LISTING_ID). dbt show previewed 3 rows with correct NULL handling. Previously-failing query now returns results without error.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.compile", "dbt_mcp.test", "dbt_mcp.show", "snowflake_mcp.execute_query"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.compile", "input": "dbt compile --select stg_ticket_listing"},
    {"tool": "dbt_mcp.test", "input": "dbt test --select stg_ticket_listing"},
    {"tool": "dbt_mcp.show", "input": "dbt show --select stg_ticket_listing --limit 5"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM DW.STG.STG_TICKET_LISTING LIMIT 3"}
  ],
  "token_usage": {"prompt": 1800, "completion": 400}
}
```

### 1.4 — Cassette Files (Join Error Path)

- [ ] Create `tests/cassettes/fix-model-bug-join/parse_error.json`

```json
{
  "output": {
    "error_type": "runtime",
    "failing_expression": "JOIN DW.CORE.VENUE_DIM ON t.VENUE_ID = v.VENUE_ID",
    "source_table": "DW.RPT.EVENT_SUMMARY",
    "model_name": "rpt_event_summary"
  },
  "decision_log": {
    "decision": "Extracted join failure from dbt run results",
    "rationale": "Recent dbt run result shows row count explosion (10x expected) after JOIN to VENUE_DIM. Likely a fanout from duplicate join keys.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["DW.DATAOPS_DBT.DBT_RUN_RESULTS"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM DW.DATAOPS_DBT.DBT_RUN_RESULTS WHERE NAME = 'rpt_event_summary' AND STATUS = 'error' ORDER BY CREATED_AT DESC LIMIT 5"}
  ],
  "token_usage": {"prompt": 900, "completion": 350}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-join/read_model_sql.json`

```json
{
  "output": {
    "model_path": "models/marts/reporting/rpt_event_summary.sql",
    "model_sql": "WITH transactions AS (\n    SELECT * FROM {{ ref('fct_transaction') }}\n),\nvenues AS (\n    SELECT * FROM {{ ref('dim_venue') }}\n),\nfinal AS (\n    SELECT\n        t.EVENT_ID,\n        t.EVENT_DATE,\n        v.VENUE_NAME,\n        v.CITY,\n        v.STATE,\n        SUM(t.GMS) AS total_gms,\n        COUNT(t.TRANSACTION_ID) AS transaction_count\n    FROM transactions t\n    JOIN venues v ON t.VENUE_ID = v.VENUE_ID\n    GROUP BY 1, 2, 3, 4, 5\n)\nSELECT * FROM final",
    "cte_structure": ["transactions", "venues", "final"],
    "source_refs": ["{{ ref('fct_transaction') }}", "{{ ref('dim_venue') }}"],
    "materialization": "table"
  },
  "decision_log": {
    "decision": "Read model SQL and identified JOIN structure",
    "rationale": "Model joins fct_transaction to dim_venue on VENUE_ID. The fanout suggests duplicates in dim_venue on VENUE_ID (possibly SCD Type 2 with multiple active rows).",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["github_cli.read_file", "dbt_mcp.get_node_details_dev"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/marts/reporting/rpt_event_summary.sql"},
    {"tool": "dbt_mcp.get_node_details_dev", "input": "rpt_event_summary"}
  ],
  "token_usage": {"prompt": 1200, "completion": 500}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-join/classify_bug_type.json`

```json
{
  "output": {
    "bug_type": "join_error",
    "confidence": "high",
    "rationale": "Row count explosion (10x) after JOIN indicates fanout. VENUE_DIM likely has duplicate VENUE_IDs (SCD Type 2 or data quality issue). The join condition ON t.VENUE_ID = v.VENUE_ID is missing a filter for the current/active record.",
    "failing_component": "JOIN venues v ON t.VENUE_ID = v.VENUE_ID"
  },
  "decision_log": {
    "decision": "Classified as join_error",
    "rationale": "Row count explosion is the hallmark of a join fanout. The dim table likely has multiple rows per VENUE_ID.",
    "alternatives_considered": [
      {"option": "logic_error", "reason": "Could be wrong GROUP BY, but row explosion points to join"}
    ],
    "confidence": "high",
    "informed_by": ["error_message", "model_sql"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 1500, "completion": 300}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-join/fix_join_error.json`

```json
{
  "output": {
    "fixed_sql": "WITH transactions AS (\n    SELECT * FROM {{ ref('fct_transaction') }}\n),\nvenues AS (\n    SELECT * FROM {{ ref('dim_venue') }}\n    WHERE IS_CURRENT = TRUE\n),\nfinal AS (\n    SELECT\n        t.EVENT_ID,\n        t.EVENT_DATE,\n        v.VENUE_NAME,\n        v.CITY,\n        v.STATE,\n        SUM(t.GMS) AS total_gms,\n        COUNT(t.TRANSACTION_ID) AS transaction_count\n    FROM transactions t\n    JOIN venues v ON t.VENUE_ID = v.VENUE_ID\n    GROUP BY 1, 2, 3, 4, 5\n)\nSELECT * FROM final",
    "fix_description": "Added WHERE IS_CURRENT = TRUE filter to the venues CTE to select only the current SCD Type 2 record per VENUE_ID. This eliminates the fanout caused by multiple historical records per venue.",
    "join_analysis": {
      "left_table": "fct_transaction",
      "left_key_cardinality": 15000,
      "right_table": "dim_venue",
      "right_key_cardinality": 15000,
      "right_total_rows": 42000,
      "duplicates_per_key": 2.8,
      "root_cause": "SCD Type 2 dimension with IS_CURRENT flag — multiple rows per VENUE_ID without filter"
    }
  },
  "decision_log": {
    "decision": "Added IS_CURRENT = TRUE filter to dimension CTE",
    "rationale": "dim_venue has 42K rows but only 15K distinct VENUE_IDs (2.8 rows per key on average). IS_CURRENT column exists — filtering to TRUE gives 1:1 cardinality and eliminates fanout.",
    "alternatives_considered": [
      {"option": "QUALIFY ROW_NUMBER() dedup", "reason": "Would work but IS_CURRENT flag is the intended pattern"},
      {"option": "Add effective_date range filter", "reason": "More complex and IS_CURRENT is sufficient"}
    ],
    "confidence": "high",
    "informed_by": ["snowflake_mcp.execute_query", "snowflake_mcp.describe_table"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT COUNT(*), COUNT(DISTINCT VENUE_ID) FROM DW.CORE.VENUE_DIM"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT COUNT(*), COUNT(DISTINCT VENUE_ID) FROM DW.CORE.VENUE_DIM WHERE IS_CURRENT = TRUE"},
    {"tool": "snowflake_mcp.describe_table", "input": "DW.CORE.VENUE_DIM"}
  ],
  "token_usage": {"prompt": 2200, "completion": 700}
}
```

- [ ] Create `tests/cassettes/fix-model-bug-join/validate_fix.json`

```json
{
  "output": {
    "compile_ok": true,
    "tests_passed": true,
    "preview_rows": [
      {"event_id": 5001, "event_date": "2025-03-15", "venue_name": "Madison Square Garden", "total_gms": 125000.00, "transaction_count": 450},
      {"event_id": 5002, "event_date": "2025-03-16", "venue_name": "Staples Center", "total_gms": 89000.00, "transaction_count": 312}
    ],
    "fix_verified": true
  },
  "decision_log": {
    "decision": "Fix validated — row count is correct, no fanout",
    "rationale": "dbt compile succeeded. Row count matches expected (15K events, not 42K). dbt test passed. Preview rows show correct aggregations.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.compile", "dbt_mcp.test", "dbt_mcp.show"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.compile", "input": "dbt compile --select rpt_event_summary"},
    {"tool": "dbt_mcp.test", "input": "dbt test --select rpt_event_summary"},
    {"tool": "dbt_mcp.show", "input": "dbt show --select rpt_event_summary --limit 5"}
  ],
  "token_usage": {"prompt": 1600, "completion": 350}
}
```

### 1.5 — E2E Test

- [ ] Create `tests/test_e2e_fix_model_bug.py`

```python
"""
End-to-end test for the fix-model-bug workflow.

Tests the full 8-step workflow with CONDITIONAL BRANCHING.
Two test classes exercise different branch paths:
  - TestFixModelBugCast: cast_error branch (classify → fix_cast_error → validate)
  - TestFixModelBugJoin: join_error branch (classify → fix_join_error → validate)
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

# Steps for cast_error path (skips fix_join_error and fix_logic_error)
CAST_PATH_STEPS = [
    "parse_error",
    "read_model_sql",
    "classify_bug_type",
    "fix_cast_error",
    "validate_fix",
    "create_pr",
]

# Steps for join_error path (skips fix_cast_error and fix_logic_error)
JOIN_PATH_STEPS = [
    "parse_error",
    "read_model_sql",
    "classify_bug_type",
    "fix_join_error",
    "validate_fix",
    "create_pr",
]


def load_cassettes(cassette_dir: Path, reason_steps: list[str]) -> dict[str, dict]:
    """Load cassettes for the steps that will actually execute."""
    cassettes = {}
    for step_id in reason_steps:
        path = cassette_dir / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(
    cassette_dir: Path,
    inputs: dict,
    reason_steps: list[str],
) -> tuple:
    """Drive the workflow with cassette responses, handling branching."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes(cassette_dir, reason_steps)

    run = engine.start("fix-model-bug", inputs)
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
            if request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/99"}),
                )

    return run, steps_executed, reason_outputs


class TestFixModelBugCast:
    """Cast error branch: parse_error → read_model_sql → classify → fix_cast_error → validate → create_pr."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "fix-model-bug-cast"
    INPUTS = {"model_name": "stg_ticket_listing", "error_message": "Numeric value 'N/A' is not recognized"}
    REASON_STEPS = [s for s in CAST_PATH_STEPS if s != "create_pr"]

    def test_workflow_takes_cast_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == CAST_PATH_STEPS

    def test_cast_path_has_6_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 6

    def test_classify_returns_cast_error(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_bug_type"]["bug_type"] == "cast_error"

    def test_fix_uses_try_cast(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        fixed_sql = reason_outputs["fix_cast_error"]["fixed_sql"]
        assert "TRY_CAST" in fixed_sql
        assert "NULLIF" in fixed_sql

    def test_problematic_values_found(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        values = reason_outputs["fix_cast_error"]["problematic_values"]
        assert len(values) > 0
        assert "N/A" in values

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["validate_fix"]["compile_ok"] is True
        assert reason_outputs["validate_fix"]["tests_passed"] is True
        assert reason_outputs["validate_fix"]["fix_verified"] is True

    def test_skips_join_and_logic_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "fix_join_error" not in steps_executed
        assert "fix_logic_error" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "fix-model-bug"
        assert trace["status"] == "completed"
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "fix_cast_error" in step_ids
        assert "fix_join_error" not in step_ids

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("fix-model-bug", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "parse_error"
        assert request.persona.id == "analytics_engineer"
        assert len(request.tools) > 0


class TestFixModelBugJoin:
    """Join error branch: parse_error → read_model_sql → classify → fix_join_error → validate → create_pr."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "fix-model-bug-join"
    INPUTS = {"model_name": "rpt_event_summary", "error_message": "Row count exceeded threshold (10x expected)"}
    REASON_STEPS = [s for s in JOIN_PATH_STEPS if s != "create_pr"]

    def test_workflow_takes_join_path(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert run.status == "completed"
        assert steps_executed == JOIN_PATH_STEPS

    def test_join_path_has_6_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert len(steps_executed) == 6

    def test_classify_returns_join_error(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["classify_bug_type"]["bug_type"] == "join_error"

    def test_fix_adds_is_current_filter(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        fixed_sql = reason_outputs["fix_join_error"]["fixed_sql"]
        assert "IS_CURRENT" in fixed_sql

    def test_join_analysis_present(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        analysis = reason_outputs["fix_join_error"]["join_analysis"]
        assert analysis["root_cause"] is not None
        assert analysis["duplicates_per_key"] > 1

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert reason_outputs["validate_fix"]["compile_ok"] is True
        assert reason_outputs["validate_fix"]["tests_passed"] is True
        assert reason_outputs["validate_fix"]["fix_verified"] is True

    def test_skips_cast_and_logic_steps(self):
        _, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        assert "fix_cast_error" not in steps_executed
        assert "fix_logic_error" not in steps_executed

    def test_trace_records_branching(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        trace = run.get_trace()
        step_ids = [s["step_id"] for s in trace["steps"]]
        assert "fix_join_error" in step_ids
        assert "fix_cast_error" not in step_ids
```

### 1.6 — Conformance Test

- [ ] Create `tests/test_conformance_fix_model_bug.py`

```python
"""Conformance tests for fix-model-bug workflow.

Validates context assembly per step WITHOUT LLM, using .test.yml spec.
Tests the cast_error path through conditional branching.
"""
import yaml
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.types import (
    ReasonRequest,
    DelegateRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"


def load_conformance(workflow_id: str) -> dict:
    path = CONTENT_DIR / "workflows" / f"{workflow_id}.test.yml"
    with open(path) as f:
        return yaml.safe_load(f)["conformance"]


class TestFixModelBugConformance:
    """Validate that fix-model-bug assembles correct context per step.

    Walks the cast_error branch to validate conformance for the branching path.
    """

    WORKFLOW_ID = "fix-model-bug"
    INPUTS = {"model_name": "stg_ticket_listing", "error_message": "Numeric value not recognized"}

    @pytest.fixture
    def conformance(self):
        return load_conformance(self.WORKFLOW_ID)

    @pytest.fixture
    def engine(self):
        return DCAGEngine(content_dir=CONTENT_DIR)

    def test_all_steps_on_cast_path_match_spec(self, engine, conformance):
        """Walk the cast_error branch and verify each step matches conformance spec."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)
        type_map = {
            "ReasonRequest": ReasonRequest,
            "DelegateRequest": DelegateRequest,
        }

        # Steps that execute on cast_error path
        cast_path = [
            "parse_error",
            "read_model_sql",
            "classify_bug_type",
            "fix_cast_error",
            "validate_fix",
            "create_pr",
        ]

        step_outputs = {
            "parse_error": {
                "error_type": "runtime",
                "failing_expression": "CAST(TICKET_PRICE AS NUMBER(18,2))",
                "source_table": "DW.STG.STG_TICKET_LISTING",
                "model_name": "stg_ticket_listing",
            },
            "read_model_sql": {
                "model_path": "models/staging/ticket/stg_ticket_listing.sql",
                "model_sql": "SELECT ...",
                "cte_structure": ["source", "final"],
                "source_refs": [],
                "materialization": "view",
            },
            "classify_bug_type": {
                "bug_type": "cast_error",
                "confidence": "high",
                "rationale": "Cast failure on TICKET_PRICE",
                "failing_component": "CAST(TICKET_PRICE AS NUMBER(18,2))",
            },
            "fix_cast_error": {
                "fixed_sql": "WITH source AS ... TRY_CAST ...",
                "fix_description": "Replaced CAST with TRY_CAST",
                "problematic_values": ["N/A", ""],
            },
            "validate_fix": {
                "compile_ok": True,
                "tests_passed": True,
                "preview_rows": [],
                "fix_verified": True,
            },
        }

        for step_id in cast_path:
            request = run.next_step()
            assert request is not None, f"Workflow ended before step '{step_id}'"
            assert request.step_id == step_id, f"Expected step '{step_id}', got '{request.step_id}'"

            spec = conformance["steps"][step_id]
            expected_type = type_map[spec["type"]]
            assert isinstance(request, expected_type), (
                f"Step '{step_id}': expected {spec['type']}, got {type(request).__name__}"
            )

            # Validate ReasonRequest specifics
            if isinstance(request, ReasonRequest):
                if "persona" in spec:
                    assert request.persona.id == spec["persona"], (
                        f"Step '{step_id}': expected persona '{spec['persona']}', got '{request.persona.id}'"
                    )
                if "tools_include" in spec:
                    tool_names = [t.name for t in request.tools]
                    for expected_tool in spec["tools_include"]:
                        assert expected_tool in tool_names, (
                            f"Step '{step_id}': missing tool '{expected_tool}'. Has: {tool_names}"
                        )
                if "tools_count" in spec:
                    assert len(request.tools) == spec["tools_count"], (
                        f"Step '{step_id}': expected {spec['tools_count']} tools, got {len(request.tools)}"
                    )
                if "has_instruction" in spec and spec["has_instruction"]:
                    assert request.instruction and len(request.instruction.strip()) > 0, (
                        f"Step '{step_id}': expected non-empty instruction"
                    )
                if "knowledge_includes" in spec:
                    for kid in spec["knowledge_includes"]:
                        assert kid in request.context.static, (
                            f"Step '{step_id}': missing knowledge '{kid}' in static context. Has: {list(request.context.static.keys())}"
                        )

            # Validate DelegateRequest specifics
            if isinstance(request, DelegateRequest):
                if "requires_approval" in spec:
                    assert request.requires_approval == spec["requires_approval"], (
                        f"Step '{step_id}': requires_approval mismatch"
                    )

            # Record results to advance
            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=step_outputs.get(step_id, {"placeholder": True})))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"approved": True}))

    def test_conformance_covers_all_steps(self, engine, conformance):
        """Ensure conformance spec covers every step in the workflow."""
        from dcag._loaders import WorkflowLoader
        wf = WorkflowLoader(CONTENT_DIR / "workflows").load(self.WORKFLOW_ID)
        workflow_steps = {s.id for s in wf.steps}
        conformance_steps = set(conformance["steps"].keys())
        assert workflow_steps == conformance_steps, (
            f"Conformance spec mismatch. "
            f"In workflow but not conformance: {workflow_steps - conformance_steps}. "
            f"In conformance but not workflow: {conformance_steps - workflow_steps}"
        )
```

### 1.7 — Manifest Entry

- [ ] Add to `content/workflows/manifest.yml`

Append after the `table-optimizer` entry:

```yaml
  - id: fix-model-bug
    name: Fix Bug in dbt Model
    persona: analytics_engineer
    triggers:
      keywords: [fix bug, fix error, model failing, debug model, fix cast, fix join]
      input_pattern: "fix {model}"
```

### 1.8 — Verify & Commit

- [ ] Run all fix-model-bug tests

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_e2e_fix_model_bug.py tests/test_conformance_fix_model_bug.py -v
# Expected: ALL PASS (requires Part 1 conditional walker to be implemented first)
```

- [ ] Run full suite to confirm no regressions

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
```

- [ ] Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add content/workflows/fix-model-bug.yml content/workflows/fix-model-bug.test.yml content/workflows/manifest.yml tests/cassettes/fix-model-bug-cast/ tests/cassettes/fix-model-bug-join/ tests/test_e2e_fix_model_bug.py tests/test_conformance_fix_model_bug.py
git commit -m "$(cat <<'EOF'
feat(workflow): add fix-model-bug workflow with conditional branching

8-step workflow with 3 branch paths (cast_error, join_error, logic_error).
Uses conditional walker transitions to route to the correct fix step.
Includes conformance spec, 2 cassette sets, and E2E tests for both paths.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Workflow — create-staging-model

### 2.1 — Workflow Definition

- [ ] Create `content/workflows/create-staging-model.yml`

```yaml
workflow:
  id: create-staging-model
  name: Create Staging Model for Source Table
  persona: analytics_engineer

  inputs:
    table_name:
      type: string
      required: true
    source_system:
      type: string
      required: false
    database:
      type: string
      required: false
    schema:
      type: string
      required: false

  steps:
    # Step 0: Discover source table metadata and cache it
    - id: discover_source_table
      mode: reason
      cache_as: source_metadata
      instruction: |
        Find the specified table in Snowflake and collect full metadata:
        1. Fully-qualified name (DATABASE.SCHEMA.TABLE)
        2. All columns with types, nullability, and comments
        3. Row count and size in bytes
        4. Creation date and last altered date
        5. Table type (BASE TABLE, VIEW, EXTERNAL TABLE)

        If database/schema not specified, search across known databases.
        If source_system is provided, narrow the search to that system's schema.

        This metadata will be cached as "source_metadata" for use in later steps.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query INFORMATION_SCHEMA.TABLES and COLUMNS for full metadata"
          usage_pattern: |
            1. SELECT * FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME ILIKE '{name}'
            2. SELECT * FROM {db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{name}' ORDER BY ORDINAL_POSITION
            3. SELECT COUNT(*) as row_count FROM {table_fqn}
        - name: snowflake_mcp.describe_table
          instruction: "Get column details including comments"
      context:
        static: [snowflake_environment, sf_type_mapping]
        dynamic: []
      output_schema:
        type: object
        required: [table_fqn, columns, row_count, size_bytes, table_type, created_at]
        properties:
          columns:
            type: array
            items:
              type: object
              required: [name, type, nullable, ordinal_position]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 1: Check if a staging model already exists
    - id: check_existing_models
      mode: reason
      instruction: |
        Check if a staging model already exists for this source table.
        Search for:
        1. A dbt model named stg_{table_name} (lowercase, snake_case)
        2. Any model that references this source table via source()
        3. An existing sources.yml entry for this table

        If a model exists, report its path and status. The workflow should
        still proceed (to update or regenerate) but the user should be informed.
      tools:
        - name: dbt_mcp.get_node_details_dev
          instruction: "Search for existing stg_ model by name"
        - name: github_cli.search_code
          instruction: "Search for source() references to this table"
      context:
        static: []
        dynamic:
          - from: discover_source_table
            select: table_fqn
      output_schema:
        type: object
        required: [model_exists, existing_model_path, sources_yml_exists, source_entry_exists]
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 2: Choose materialization strategy
    - id: choose_materialization
      mode: reason
      instruction: |
        Determine the optimal materialization for this staging model based on:

        1. Row count:
           - < 100K rows → view (cheap, always fresh)
           - 100K - 10M rows → table (predictable performance)
           - > 10M rows → incremental (avoid full refreshes)

        2. Table characteristics from cached source_metadata:
           - Has CREATED_AT or UPDATED_AT → incremental candidate
           - Has no timestamp columns → table (can't do incremental)
           - Is an append-only log → incremental with insert_overwrite

        3. Update frequency:
           - Static reference data → view
           - Daily batch loads → table
           - Streaming / frequent updates → incremental

        Output the recommended materialization with rationale.
      tools: []
      context:
        static: [dbt_project_structure, model_templates]
        dynamic:
          - from: discover_source_table
            select: [row_count, size_bytes]
        cache: [source_metadata]
      output_schema:
        type: object
        required: [materialization, unique_key, incremental_strategy, rationale]
        properties:
          materialization:
            type: string
            enum: [view, table, incremental]
      budget:
        max_llm_turns: 2
        max_tokens: 5000

    # Step 3: Generate the staging model SQL
    - id: generate_model_sql
      mode: reason
      instruction: |
        Generate the complete staging model SQL file following team conventions:

        1. CTE structure:
           - `source` CTE: SELECT * FROM {{ source('{source_system}', '{TABLE_NAME}') }}
           - `renamed` CTE: Rename columns to snake_case, apply type casts
           - `final` CTE (if needed): Add computed columns, filters

        2. Column handling from cached source_metadata:
           - Rename camelCase/UPPER_CASE to snake_case
           - Apply appropriate casts (e.g., VARCHAR dates → DATE)
           - Add TRIM() for VARCHAR columns
           - Preserve column order from source

        3. If incremental:
           - Add {% if is_incremental() %} block
           - Filter on the incremental key column (UPDATED_AT or CREATED_AT)

        4. Config block:
           - {{ config(materialized='{materialization}') }}
           - Add unique_key if incremental
           - Add tags if source_system is known

        Output the complete SQL file content.
      tools: []
      context:
        static: [naming_conventions, model_templates]
        dynamic:
          - from: discover_source_table
          - from: choose_materialization
        cache: [source_metadata]
      anti_patterns:
        - "Don't use SELECT * in the final output — list all columns explicitly"
        - "Don't skip type casting — always cast to the target type"
        - "Don't hardcode database names — use source() macro"
      quality_criteria:
        - "All columns are explicitly listed (no SELECT *)"
        - "Column names are snake_case"
        - "Type casts are applied where appropriate"
        - "Config block matches materialization choice"
      output_schema:
        type: object
        required: [model_sql, model_filename, column_count, config_block]
      budget:
        max_llm_turns: 3
        max_tokens: 15000

    # Step 4: Generate schema.yml for the staging model
    - id: generate_schema_yml
      mode: reason
      instruction: |
        Generate the schema.yml file for this staging model.

        From cached source_metadata, generate:
        1. Model-level description
        2. Column entries with:
           - name (snake_case, matching the model SQL)
           - description (business-friendly, inferred from column name and type)
           - tests:
             - not_null on non-nullable columns
             - unique on likely primary keys (columns ending in _id that are non-nullable)
             - accepted_values for known enum columns (status, type, category)
        3. Follow existing project conventions for YAML formatting

        Output the complete schema.yml content.
      tools: []
      context:
        static: [testing_standards, naming_conventions]
        dynamic:
          - from: generate_model_sql
        cache: [source_metadata]
      anti_patterns:
        - "Don't skip descriptions — every column must have one"
        - "Don't forget tests for primary key columns"
      quality_criteria:
        - "Every column has a non-empty description"
        - "PK columns have not_null + unique"
        - "YAML is valid and properly indented"
      output_schema:
        type: object
        required: [schema_yml_content, tests_added, column_count]
      budget:
        max_llm_turns: 3
        max_tokens: 15000

    # Step 5: Add or update sources.yml entry
    - id: add_to_sources_yml
      mode: reason
      instruction: |
        Add (or update) the source entry in sources.yml for this table.

        1. Read the existing sources.yml for this source system
        2. If the source group exists, add the table entry
        3. If no source group exists, create the full source definition:
           - name: {source_system}
           - database: {database}
           - schema: {schema}
           - tables: [{table_name}]
        4. Include freshness checks if the table has timestamp columns
        5. Add the loaded_at_field if CREATED_AT or UPDATED_AT exists

        Output the complete sources.yml content (or the section to add).
      tools:
        - name: github_cli.read_file
          instruction: "Read existing sources.yml to check for the source group"
      context:
        static: [dbt_project_structure]
        dynamic:
          - from: discover_source_table
            select: [table_fqn, source_system]
      output_schema:
        type: object
        required: [sources_yml_content, is_new_source, source_name]
      budget:
        max_llm_turns: 3
        max_tokens: 10000

    # Step 6: Validate — dbt compile + parse
    - id: validate
      mode: reason
      instruction: |
        Validate all generated files:
        1. dbt_mcp.compile — check the staging model SQL compiles
        2. dbt_mcp.parse — check the full project parses with the new schema.yml and sources.yml
        3. Verify source reference resolves correctly
        4. Check for any name collisions with existing models

        If validation fails, report specific errors for correction.
      tools:
        - name: dbt_mcp.compile
          instruction: "Compile the new staging model"
        - name: dbt_mcp.parse
          instruction: "Parse the full dbt project with new files"
      context:
        static: []
        dynamic:
          - from: generate_model_sql
            select: [model_sql, model_filename]
          - from: generate_schema_yml
            select: schema_yml_content
      output_schema:
        type: object
        required: [compile_ok, parse_ok, errors]
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 7: Create PR
    - id: create_pr
      mode: execute
      type: delegate
      delegate: shift.create_pr
      requires_approval: true
      context:
        dynamic:
          - from: discover_source_table
            select: [table_fqn, row_count]
          - from: choose_materialization
            select: [materialization, rationale]
          - from: generate_model_sql
            select: [model_filename, column_count]
          - from: generate_schema_yml
            select: [tests_added, column_count]
          - from: add_to_sources_yml
            select: [is_new_source, source_name]
          - from: validate
            select: [compile_ok, parse_ok]
```

### 2.2 — Conformance Test Spec

- [ ] Create `content/workflows/create-staging-model.test.yml`

```yaml
conformance:
  workflow_id: create-staging-model

  steps:
    discover_source_table:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - snowflake_environment
        - sf_type_mapping
      tools_include:
        - snowflake_mcp.execute_query
        - snowflake_mcp.describe_table
      has_instruction: true

    check_existing_models:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - dbt_mcp.get_node_details_dev
        - github_cli.search_code
      dynamic_refs_from:
        - discover_source_table
      has_instruction: true

    choose_materialization:
      type: ReasonRequest
      persona: analytics_engineer
      tools_count: 0
      knowledge_includes:
        - dbt_project_structure
        - model_templates
      dynamic_refs_from:
        - discover_source_table
      has_instruction: true

    generate_model_sql:
      type: ReasonRequest
      persona: analytics_engineer
      tools_count: 0
      knowledge_includes:
        - naming_conventions
        - model_templates
      dynamic_refs_from:
        - discover_source_table
        - choose_materialization
      has_instruction: true

    generate_schema_yml:
      type: ReasonRequest
      persona: analytics_engineer
      tools_count: 0
      knowledge_includes:
        - testing_standards
        - naming_conventions
      dynamic_refs_from:
        - generate_model_sql
      has_instruction: true

    add_to_sources_yml:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - dbt_project_structure
      tools_include:
        - github_cli.read_file
      dynamic_refs_from:
        - discover_source_table
      has_instruction: true

    validate:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - dbt_mcp.compile
        - dbt_mcp.parse
      dynamic_refs_from:
        - generate_model_sql
        - generate_schema_yml
      has_instruction: true

    create_pr:
      type: DelegateRequest
      requires_approval: true
```

### 2.3 — Cassette Files

- [ ] Create `tests/cassettes/create-staging-model/discover_source_table.json`

```json
{
  "output": {
    "table_fqn": "DW.RAW.PAYMENT_TRANSACTION",
    "columns": [
      {"name": "PAYMENT_ID", "type": "NUMBER(38,0)", "nullable": false, "ordinal_position": 1},
      {"name": "ORDER_ID", "type": "NUMBER(38,0)", "nullable": false, "ordinal_position": 2},
      {"name": "PAYMENT_METHOD", "type": "VARCHAR(50)", "nullable": false, "ordinal_position": 3},
      {"name": "AMOUNT", "type": "NUMBER(18,2)", "nullable": false, "ordinal_position": 4},
      {"name": "CURRENCY", "type": "VARCHAR(3)", "nullable": false, "ordinal_position": 5},
      {"name": "STATUS", "type": "VARCHAR(20)", "nullable": false, "ordinal_position": 6},
      {"name": "PROCESSOR_RESPONSE_CODE", "type": "VARCHAR(10)", "nullable": true, "ordinal_position": 7},
      {"name": "PROCESSOR_RESPONSE_TEXT", "type": "VARCHAR(256)", "nullable": true, "ordinal_position": 8},
      {"name": "CREATED_AT", "type": "TIMESTAMP_NTZ", "nullable": false, "ordinal_position": 9},
      {"name": "UPDATED_AT", "type": "TIMESTAMP_NTZ", "nullable": true, "ordinal_position": 10}
    ],
    "row_count": 25000000,
    "size_bytes": 3200000000,
    "table_type": "BASE TABLE",
    "created_at": "2024-06-15T10:00:00Z"
  },
  "decision_log": {
    "decision": "Found table DW.RAW.PAYMENT_TRANSACTION",
    "rationale": "Table exists in RAW schema, 25M rows, ~3.2GB. Has CREATED_AT and UPDATED_AT columns suitable for incremental loading. 10 columns total.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["INFORMATION_SCHEMA.TABLES", "INFORMATION_SCHEMA.COLUMNS"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM DW.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PAYMENT_TRANSACTION'"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM DW.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PAYMENT_TRANSACTION' ORDER BY ORDINAL_POSITION"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT COUNT(*) as row_count FROM DW.RAW.PAYMENT_TRANSACTION"}
  ],
  "token_usage": {"prompt": 1200, "completion": 500}
}
```

- [ ] Create `tests/cassettes/create-staging-model/check_existing_models.json`

```json
{
  "output": {
    "model_exists": false,
    "existing_model_path": null,
    "sources_yml_exists": true,
    "source_entry_exists": false
  },
  "decision_log": {
    "decision": "No existing staging model found for PAYMENT_TRANSACTION",
    "rationale": "dbt_mcp found no model named stg_payment_transaction. GitHub search found no source() references to PAYMENT_TRANSACTION. The payment sources.yml exists but has no entry for this table.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.get_node_details_dev", "github_cli.search_code"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.get_node_details_dev", "input": "stg_payment_transaction"},
    {"tool": "github_cli.search_code", "input": "source('payment', 'PAYMENT_TRANSACTION')"}
  ],
  "token_usage": {"prompt": 900, "completion": 250}
}
```

- [ ] Create `tests/cassettes/create-staging-model/choose_materialization.json`

```json
{
  "output": {
    "materialization": "incremental",
    "unique_key": "payment_id",
    "incremental_strategy": "merge",
    "rationale": "Table has 25M rows (>10M threshold) with UPDATED_AT column available for incremental loading. Merge strategy using PAYMENT_ID as unique key ensures upsert behavior for updated records."
  },
  "decision_log": {
    "decision": "Incremental materialization with merge strategy",
    "rationale": "25M rows exceeds the 10M threshold for incremental. UPDATED_AT column is available for incremental filtering. PAYMENT_ID is a natural unique key for merge.",
    "alternatives_considered": [
      {"option": "table", "reason": "Would work but 25M row full refresh is expensive"},
      {"option": "view", "reason": "Too many rows for view — would be slow"}
    ],
    "confidence": "high",
    "informed_by": ["source_metadata (cached)", "row_count", "column types"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 1100, "completion": 300}
}
```

- [ ] Create `tests/cassettes/create-staging-model/generate_model_sql.json`

```json
{
  "output": {
    "model_sql": "{{ config(\n    materialized='incremental',\n    unique_key='payment_id',\n    incremental_strategy='merge',\n    tags=['payment']\n) }}\n\nWITH source AS (\n    SELECT * FROM {{ source('payment', 'PAYMENT_TRANSACTION') }}\n    {% if is_incremental() %}\n    WHERE UPDATED_AT > (SELECT MAX(updated_at) FROM {{ this }})\n    {% endif %}\n),\n\nrenamed AS (\n    SELECT\n        PAYMENT_ID AS payment_id,\n        ORDER_ID AS order_id,\n        TRIM(PAYMENT_METHOD) AS payment_method,\n        AMOUNT AS amount,\n        TRIM(CURRENCY) AS currency,\n        TRIM(STATUS) AS status,\n        TRIM(PROCESSOR_RESPONSE_CODE) AS processor_response_code,\n        TRIM(PROCESSOR_RESPONSE_TEXT) AS processor_response_text,\n        CREATED_AT AS created_at,\n        UPDATED_AT AS updated_at\n    FROM source\n)\n\nSELECT * FROM renamed",
    "model_filename": "stg_payment_transaction.sql",
    "column_count": 10,
    "config_block": "materialized='incremental', unique_key='payment_id', incremental_strategy='merge'"
  },
  "decision_log": {
    "decision": "Generated incremental staging model with 10 columns",
    "rationale": "All columns renamed to snake_case. VARCHAR columns wrapped in TRIM(). Incremental filter on UPDATED_AT. Config block includes materialization, unique_key, and strategy.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["source_metadata (cached)", "naming_conventions", "model_templates"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 2000, "completion": 700}
}
```

- [ ] Create `tests/cassettes/create-staging-model/generate_schema_yml.json`

```json
{
  "output": {
    "schema_yml_content": "version: 2\n\nmodels:\n  - name: stg_payment_transaction\n    description: >\n      Staging model for payment transactions from the payment system.\n      Each row represents a single payment attempt with status,\n      amount, and processor response details.\n    columns:\n      - name: payment_id\n        description: Unique identifier for each payment transaction.\n        tests:\n          - not_null\n          - unique\n      - name: order_id\n        description: Foreign key to the order associated with this payment.\n        tests:\n          - not_null\n      - name: payment_method\n        description: Payment method used (e.g., credit_card, paypal, apple_pay).\n        tests:\n          - not_null\n      - name: amount\n        description: Payment amount in the transaction currency.\n        tests:\n          - not_null\n      - name: currency\n        description: ISO 4217 currency code for the payment amount.\n        tests:\n          - not_null\n          - accepted_values:\n              values: ['USD', 'GBP', 'EUR', 'CAD', 'AUD']\n      - name: status\n        description: Current status of the payment (e.g., pending, completed, failed, refunded).\n        tests:\n          - not_null\n          - accepted_values:\n              values: ['pending', 'completed', 'failed', 'refunded', 'cancelled']\n      - name: processor_response_code\n        description: Response code returned by the payment processor. May be null for pending payments.\n      - name: processor_response_text\n        description: Human-readable response message from the payment processor.\n      - name: created_at\n        description: Timestamp when the payment record was created.\n        tests:\n          - not_null\n      - name: updated_at\n        description: Timestamp when the payment record was last updated. May be null if never updated.\n",
    "tests_added": [
      {"column": "payment_id", "tests": ["not_null", "unique"]},
      {"column": "order_id", "tests": ["not_null"]},
      {"column": "payment_method", "tests": ["not_null"]},
      {"column": "amount", "tests": ["not_null"]},
      {"column": "currency", "tests": ["not_null", "accepted_values"]},
      {"column": "status", "tests": ["not_null", "accepted_values"]},
      {"column": "created_at", "tests": ["not_null"]}
    ],
    "column_count": 10
  },
  "decision_log": {
    "decision": "Generated schema.yml with 10 columns, 11 tests",
    "rationale": "Applied testing standards: not_null on non-nullable columns, unique on PK (payment_id), accepted_values on enum columns (currency, status). Nullable columns (processor_response_code, processor_response_text, updated_at) have no not_null test.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["source_metadata (cached)", "testing_standards"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 1800, "completion": 800}
}
```

- [ ] Create `tests/cassettes/create-staging-model/add_to_sources_yml.json`

```json
{
  "output": {
    "sources_yml_content": "version: 2\n\nsources:\n  - name: payment\n    database: DW\n    schema: RAW\n    description: Payment system source tables\n    loader: fivetran\n    loaded_at_field: UPDATED_AT\n    freshness:\n      warn_after: {count: 24, period: hour}\n      error_after: {count: 48, period: hour}\n    tables:\n      - name: PAYMENT_TRANSACTION\n        description: Raw payment transaction records from the payment processing system.\n        columns:\n          - name: PAYMENT_ID\n            tests:\n              - not_null\n              - unique\n",
    "is_new_source": false,
    "source_name": "payment"
  },
  "decision_log": {
    "decision": "Added PAYMENT_TRANSACTION to existing payment source group",
    "rationale": "The payment source group already exists in sources.yml. Added PAYMENT_TRANSACTION as a new table entry with freshness check on UPDATED_AT (warn after 24h, error after 48h).",
    "alternatives_considered": [
      {"option": "Create new source group", "reason": "Unnecessary — payment group already exists"}
    ],
    "confidence": "high",
    "informed_by": ["github_cli.read_file"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/staging/payment/_payment_sources.yml"}
  ],
  "token_usage": {"prompt": 1000, "completion": 400}
}
```

- [ ] Create `tests/cassettes/create-staging-model/validate.json`

```json
{
  "output": {
    "compile_ok": true,
    "parse_ok": true,
    "errors": []
  },
  "decision_log": {
    "decision": "Validation passed — model compiles, project parses",
    "rationale": "dbt compile succeeded for stg_payment_transaction. dbt parse completed without errors. Source reference resolves correctly. No name collisions detected.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.compile", "dbt_mcp.parse"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.compile", "input": "dbt compile --select stg_payment_transaction"},
    {"tool": "dbt_mcp.parse", "input": "dbt parse"}
  ],
  "token_usage": {"prompt": 800, "completion": 200}
}
```

### 2.4 — Golden File

- [ ] Create `tests/goldens/create-staging-model/staging_model_output.json`

```json
{
  "table_fqn": "DW.RAW.PAYMENT_TRANSACTION",
  "model_filename": "stg_payment_transaction.sql",
  "materialization": "incremental",
  "unique_key": "payment_id",
  "column_count": 10,
  "tests_added_count": 11,
  "has_incremental_filter": true,
  "has_source_ref": true,
  "compile_ok": true,
  "parse_ok": true
}
```

### 2.5 — E2E Test

- [ ] Create `tests/test_e2e_create_staging_model.py`

```python
"""
End-to-end test for the create-staging-model workflow.

Tests the full 8-step linear workflow with cassette responses, verifying
the engine walks all steps and produces a correct staging model package
(SQL + schema.yml + sources.yml).
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

# All 8 steps in execution order
EXPECTED_STEPS = [
    "discover_source_table",
    "check_existing_models",
    "choose_materialization",
    "generate_model_sql",
    "generate_schema_yml",
    "add_to_sources_yml",
    "validate",
    "create_pr",
]

# 7 REASON steps that need cassettes
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("create_pr",)]


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 7 cassettes for the create-staging-model test."""
    cassettes = {}
    for step_id in REASON_STEPS:
        path = cassette_dir / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(cassette_dir: Path, inputs: dict) -> tuple:
    """Drive the full workflow with cassette responses."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes(cassette_dir)

    run = engine.start("create-staging-model", inputs)
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
            if request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/67"}),
                )

    return run, steps_executed, reason_outputs


class TestCreateStagingModel:
    """Create staging model for PAYMENT_TRANSACTION from RAW schema."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "create-staging-model"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "create-staging-model"
    INPUTS = {"table_name": "PAYMENT_TRANSACTION", "source_system": "payment", "database": "DW", "schema": "RAW"}

    def test_workflow_completes_8_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 8
        assert steps_executed == EXPECTED_STEPS

    def test_table_fqn_resolved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["discover_source_table"]["table_fqn"] == "DW.RAW.PAYMENT_TRANSACTION"

    def test_columns_discovered(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        columns = reason_outputs["discover_source_table"]["columns"]
        assert len(columns) == 10
        assert columns[0]["name"] == "PAYMENT_ID"

    def test_no_existing_model(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["check_existing_models"]["model_exists"] is False

    def test_materialization_is_incremental(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        mat = reason_outputs["choose_materialization"]
        assert mat["materialization"] == "incremental"
        assert mat["unique_key"] == "payment_id"
        assert mat["incremental_strategy"] == "merge"

    def test_model_sql_generated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["generate_model_sql"]
        assert output["model_filename"] == "stg_payment_transaction.sql"
        assert output["column_count"] == 10
        assert "incremental" in output["model_sql"]
        assert "source('payment', 'PAYMENT_TRANSACTION')" in output["model_sql"]
        assert "is_incremental()" in output["model_sql"]

    def test_model_sql_has_snake_case_columns(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        sql = reason_outputs["generate_model_sql"]["model_sql"]
        assert "payment_id" in sql
        assert "payment_method" in sql
        assert "processor_response_code" in sql

    def test_model_sql_has_trim(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        sql = reason_outputs["generate_model_sql"]["model_sql"]
        assert "TRIM(" in sql

    def test_schema_yml_generated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["generate_schema_yml"]
        assert output["column_count"] == 10
        assert "stg_payment_transaction" in output["schema_yml_content"]
        assert "payment_id" in output["schema_yml_content"]

    def test_pk_has_not_null_and_unique(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_schema_yml"]["tests_added"]
        pk_tests = next(t for t in tests_added if t["column"] == "payment_id")
        assert "not_null" in pk_tests["tests"]
        assert "unique" in pk_tests["tests"]

    def test_enum_columns_have_accepted_values(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_schema_yml"]["tests_added"]
        status_tests = next(t for t in tests_added if t["column"] == "status")
        assert "accepted_values" in status_tests["tests"]
        currency_tests = next(t for t in tests_added if t["column"] == "currency")
        assert "accepted_values" in currency_tests["tests"]

    def test_sources_yml_updated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["add_to_sources_yml"]
        assert output["is_new_source"] is False
        assert output["source_name"] == "payment"
        assert "PAYMENT_TRANSACTION" in output["sources_yml_content"]

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["validate"]["compile_ok"] is True
        assert reason_outputs["validate"]["parse_ok"] is True
        assert reason_outputs["validate"]["errors"] == []

    def test_report_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        with open(self.GOLDEN_DIR / "staging_model_output.json") as f:
            golden = json.load(f)
        assert reason_outputs["discover_source_table"]["table_fqn"] == golden["table_fqn"]
        assert reason_outputs["generate_model_sql"]["model_filename"] == golden["model_filename"]
        assert reason_outputs["choose_materialization"]["materialization"] == golden["materialization"]
        assert reason_outputs["generate_model_sql"]["column_count"] == golden["column_count"]
        assert reason_outputs["validate"]["compile_ok"] == golden["compile_ok"]

    def test_trace_has_all_8_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "create-staging-model"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 8

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("create-staging-model", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "discover_source_table"
        assert request.persona.id == "analytics_engineer"
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_cache_as_declared_on_discover_step(self):
        """Verify the discover_source_table step declares cache_as."""
        from dcag._loaders import WorkflowLoader
        wf = WorkflowLoader(CONTENT_DIR / "workflows").load("create-staging-model")
        discover_step = next(s for s in wf.steps if s.id == "discover_source_table")
        assert discover_step.cache_as == "source_metadata"
```

### 2.6 — Conformance Test

- [ ] Create `tests/test_conformance_create_staging_model.py`

```python
"""Conformance tests for create-staging-model workflow.

Validates context assembly per step WITHOUT LLM, using .test.yml spec.
"""
import yaml
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.types import (
    ReasonRequest,
    DelegateRequest,
    StepSuccess,
)

CONTENT_DIR = Path(__file__).parent.parent / "content"


def load_conformance(workflow_id: str) -> dict:
    path = CONTENT_DIR / "workflows" / f"{workflow_id}.test.yml"
    with open(path) as f:
        return yaml.safe_load(f)["conformance"]


class TestCreateStagingModelConformance:
    """Validate that create-staging-model assembles correct context per step."""

    WORKFLOW_ID = "create-staging-model"
    INPUTS = {"table_name": "PAYMENT_TRANSACTION", "source_system": "payment", "database": "DW", "schema": "RAW"}

    @pytest.fixture
    def conformance(self):
        return load_conformance(self.WORKFLOW_ID)

    @pytest.fixture
    def engine(self):
        return DCAGEngine(content_dir=CONTENT_DIR)

    def test_all_steps_match_expected_types(self, engine, conformance):
        """Walk the workflow and verify each step returns the expected request type."""
        run = engine.start(self.WORKFLOW_ID, self.INPUTS)
        type_map = {
            "ReasonRequest": ReasonRequest,
            "DelegateRequest": DelegateRequest,
        }

        step_outputs = {
            "discover_source_table": {
                "table_fqn": "DW.RAW.PAYMENT_TRANSACTION",
                "columns": [
                    {"name": "PAYMENT_ID", "type": "NUMBER(38,0)", "nullable": False, "ordinal_position": 1},
                    {"name": "AMOUNT", "type": "NUMBER(18,2)", "nullable": False, "ordinal_position": 2},
                ],
                "row_count": 25000000,
                "size_bytes": 3200000000,
                "table_type": "BASE TABLE",
                "created_at": "2024-06-15T10:00:00Z",
            },
            "check_existing_models": {
                "model_exists": False,
                "existing_model_path": None,
                "sources_yml_exists": True,
                "source_entry_exists": False,
            },
            "choose_materialization": {
                "materialization": "incremental",
                "unique_key": "payment_id",
                "incremental_strategy": "merge",
                "rationale": "25M rows with UPDATED_AT column",
            },
            "generate_model_sql": {
                "model_sql": "{{ config(materialized='incremental') }}...",
                "model_filename": "stg_payment_transaction.sql",
                "column_count": 10,
                "config_block": "materialized='incremental'",
            },
            "generate_schema_yml": {
                "schema_yml_content": "version: 2...",
                "tests_added": [{"column": "payment_id", "tests": ["not_null", "unique"]}],
                "column_count": 10,
            },
            "add_to_sources_yml": {
                "sources_yml_content": "version: 2...",
                "is_new_source": False,
                "source_name": "payment",
            },
            "validate": {
                "compile_ok": True,
                "parse_ok": True,
                "errors": [],
            },
        }

        for step_id, spec in conformance["steps"].items():
            request = run.next_step()
            assert request is not None, f"Workflow ended before step '{step_id}'"
            assert request.step_id == step_id, f"Expected step '{step_id}', got '{request.step_id}'"

            expected_type = type_map[spec["type"]]
            assert isinstance(request, expected_type), (
                f"Step '{step_id}': expected {spec['type']}, got {type(request).__name__}"
            )

            # Validate ReasonRequest specifics
            if isinstance(request, ReasonRequest):
                if "persona" in spec:
                    assert request.persona.id == spec["persona"], (
                        f"Step '{step_id}': expected persona '{spec['persona']}', got '{request.persona.id}'"
                    )
                if "tools_include" in spec:
                    tool_names = [t.name for t in request.tools]
                    for expected_tool in spec["tools_include"]:
                        assert expected_tool in tool_names, (
                            f"Step '{step_id}': missing tool '{expected_tool}'. Has: {tool_names}"
                        )
                if "tools_count" in spec:
                    assert len(request.tools) == spec["tools_count"], (
                        f"Step '{step_id}': expected {spec['tools_count']} tools, got {len(request.tools)}"
                    )
                if "has_instruction" in spec and spec["has_instruction"]:
                    assert request.instruction and len(request.instruction.strip()) > 0, (
                        f"Step '{step_id}': expected non-empty instruction"
                    )
                if "knowledge_includes" in spec:
                    for kid in spec["knowledge_includes"]:
                        assert kid in request.context.static, (
                            f"Step '{step_id}': missing knowledge '{kid}' in static context. Has: {list(request.context.static.keys())}"
                        )

            # Validate DelegateRequest specifics
            if isinstance(request, DelegateRequest):
                if "requires_approval" in spec:
                    assert request.requires_approval == spec["requires_approval"], (
                        f"Step '{step_id}': requires_approval mismatch"
                    )

            # Record dummy results to advance
            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=step_outputs.get(step_id, {"placeholder": True})))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"approved": True}))

    def test_conformance_covers_all_steps(self, engine, conformance):
        """Ensure conformance spec covers every step in the workflow."""
        from dcag._loaders import WorkflowLoader
        wf = WorkflowLoader(CONTENT_DIR / "workflows").load(self.WORKFLOW_ID)
        workflow_steps = {s.id for s in wf.steps}
        conformance_steps = set(conformance["steps"].keys())
        assert workflow_steps == conformance_steps, (
            f"Conformance spec mismatch. "
            f"In workflow but not conformance: {workflow_steps - conformance_steps}. "
            f"In conformance but not workflow: {conformance_steps - workflow_steps}"
        )
```

### 2.7 — Manifest Entry

- [ ] Add to `content/workflows/manifest.yml`

Append after the `fix-model-bug` entry:

```yaml
  - id: create-staging-model
    name: Create Staging Model for Source Table
    persona: analytics_engineer
    triggers:
      keywords: [create staging model, new source, add source table, scaffold model, new staging]
      input_pattern: "create staging for {table}"
```

### 2.8 — Verify & Commit

- [ ] Run all create-staging-model tests

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest tests/test_e2e_create_staging_model.py tests/test_conformance_create_staging_model.py -v
# Expected: ALL PASS (requires Part 1 schema cache to be implemented first)
```

- [ ] Run full suite to confirm no regressions

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph && python -m pytest --tb=short -q
```

- [ ] Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add content/workflows/create-staging-model.yml content/workflows/create-staging-model.test.yml content/workflows/manifest.yml tests/cassettes/create-staging-model/ tests/goldens/create-staging-model/ tests/test_e2e_create_staging_model.py tests/test_conformance_create_staging_model.py
git commit -m "$(cat <<'EOF'
feat(workflow): add create-staging-model workflow with schema cache

8-step linear workflow that scaffolds a complete staging model package
(SQL + schema.yml + sources.yml). Uses cache_as on discover step to
avoid redundant Snowflake metadata queries in downstream steps.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Summary

| Artifact | fix-model-bug | create-staging-model |
|----------|---------------|---------------------|
| Workflow YAML | `content/workflows/fix-model-bug.yml` | `content/workflows/create-staging-model.yml` |
| Conformance spec | `content/workflows/fix-model-bug.test.yml` | `content/workflows/create-staging-model.test.yml` |
| Cassette sets | 2 (cast + join paths, 5 files each) | 1 (7 files) |
| Golden file | N/A (branching precludes single golden) | `tests/goldens/create-staging-model/staging_model_output.json` |
| E2E test | `tests/test_e2e_fix_model_bug.py` (2 classes, 17 tests) | `tests/test_e2e_create_staging_model.py` (1 class, 17 tests) |
| Conformance test | `tests/test_conformance_fix_model_bug.py` (2 tests) | `tests/test_conformance_create_staging_model.py` (2 tests) |
| Manifest entry | Yes | Yes |
| Engine features used | Conditional Walker (transitions) | Schema Cache (cache_as) |
| Steps | 8 (3 branches at step 2) | 8 (linear) |
| Persona | analytics_engineer | analytics_engineer |

**Total new tests:** ~38 (17 + 2 + 17 + 2)
**Total new files:** ~22 (2 YAML + 2 test specs + 12 cassettes + 1 golden + 2 E2E tests + 2 conformance tests + 1 manifest update)
