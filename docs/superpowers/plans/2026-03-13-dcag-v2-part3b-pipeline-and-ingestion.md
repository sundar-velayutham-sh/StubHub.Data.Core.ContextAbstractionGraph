# DCAG v2 Implementation Plan — Part 3b: thread-field-through-pipeline + configure-ingestion-pipeline

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax.

**Goal:** Build thread-field-through-pipeline (step loops) and configure-ingestion-pipeline (schema cache) workflows.

**Prerequisites:** Part 1 engine features (conditional walker, schema cache, step loops) must be built first.

**Tech Stack:** Python 3.14, dataclasses, PyYAML, pytest

---

## Task 1: thread-field-through-pipeline workflow

### 1.1 — Knowledge Files

- [ ] Create `content/knowledge/pipeline_threading_conventions.yml`

```yaml
knowledge:
  id: pipeline_threading_conventions
  domain: pipeline
  description: "Conventions for threading a new field through a dbt pipeline (stg → int → fct/dim)"

  principles:
    - "A column must be explicitly added at every layer — it does not auto-propagate"
    - "Each layer may rename or transform the column (e.g., camelCase → snake_case in staging)"
    - "Add the column near related columns in the SELECT, not at the end"
    - "Intermediate models may aggregate or filter — the column must survive the transformation"
    - "Fact tables receive measures; dimension tables receive attributes"

  layer_rules:
    staging:
      prefix: "stg_"
      action: "Add column to SELECT from source, apply snake_case rename if needed"
      example: "userId → user_id"
      tests: ["not_null if source guarantees it"]
    intermediate:
      prefix: "int_"
      action: "Pass column through from ref('stg_*'). If model aggregates, decide: GROUP BY or aggregate"
      example: "If int_ model does GROUP BY, add column to GROUP BY or wrap in MAX()/FIRST_VALUE()"
      tests: ["not_null if upstream guarantees it"]
    fact:
      prefix: "fct_"
      action: "Include in final SELECT from ref('int_*') or ref('stg_*')"
      tests: ["not_null", "unique on PK columns"]
    dimension:
      prefix: "dim_"
      action: "Include as attribute column from ref('int_*') or ref('stg_*')"
      tests: ["not_null for key attributes"]

  schema_yml_rules:
    - "Every new column MUST be added to the model's schema.yml"
    - "Include a clear, business-friendly description"
    - "Add appropriate tests based on column type and nullability"
    - "If the column is a foreign key, add a relationships test"

  anti_patterns:
    - "Don't use SELECT * — it hides whether the column actually propagates"
    - "Don't add the column only at the final layer — it must exist at every intermediate step"
    - "Don't skip schema.yml updates — undocumented columns cause confusion"
    - "Don't thread a column through a model that aggregates without considering GROUP BY impact"
```

- [ ] Create `content/knowledge/testing_standards.yml`

```yaml
knowledge:
  id: testing_standards
  domain: testing
  description: "dbt testing standards for StubHub data models"

  required_tests:
    primary_key:
      tests: ["not_null", "unique"]
      description: "Every primary key column must have both tests"
    foreign_key:
      tests: ["not_null", "relationships"]
      description: "Foreign keys should reference the parent table"
    boolean:
      tests: ["not_null", "accepted_values"]
      values: [true, false]
    enum:
      tests: ["accepted_values"]
      description: "Columns with known value sets should have accepted_values"
    timestamp:
      tests: ["not_null"]
      description: "Created/updated timestamps are typically not_null"

  naming_patterns:
    - pattern: ".*_id$"
      likely_type: "foreign_key"
      suggested_tests: ["not_null", "relationships"]
    - pattern: "^is_.*|^has_.*"
      likely_type: "boolean"
      suggested_tests: ["not_null", "accepted_values"]
    - pattern: ".*_at$|.*_date$"
      likely_type: "timestamp"
      suggested_tests: ["not_null"]
    - pattern: ".*_code$|.*_type$|.*_status$"
      likely_type: "enum"
      suggested_tests: ["accepted_values"]

  coverage_rules:
    - "Every model must have at least one not_null test"
    - "Every model must have at least one unique test on its grain column"
    - "New columns added via pipeline threading inherit test expectations from their layer"
```

### 1.2 — Workflow Definition

- [ ] Create `content/workflows/thread-field-through-pipeline.yml`

```yaml
workflow:
  id: thread-field-through-pipeline
  name: Thread Field Through dbt Pipeline
  persona: analytics_engineer

  inputs:
    column_name:
      type: string
      required: true
    source_model:
      type: string
      required: true

  steps:
    # Step 0: Find the column in the source model
    - id: resolve_source_column
      mode: reason
      instruction: |
        Find the specified column in the source model. Verify the column exists
        in the Snowflake table backing this model. Get the column's data type,
        nullability, and sample values. If the column doesn't exist, check for
        similar column names (fuzzy match) and report.

        Also resolve the model's file path in the dbt project and the source
        table FQN from Snowflake INFORMATION_SCHEMA.
      tools:
        - name: snowflake_mcp.describe_table
          instruction: "Get column details for the source model's backing table"
        - name: snowflake_mcp.execute_query
          instruction: "Query INFORMATION_SCHEMA.COLUMNS for column metadata and sample values"
        - name: github_cli.read_file
          instruction: "Read the source model SQL to understand its SELECT list"
      context:
        static: [naming_conventions, sf_type_mapping]
        dynamic: []
      output_schema:
        type: object
        required: [model_name, model_path, table_fqn, column_info]
        properties:
          column_info:
            type: object
            required: [name, sf_type, nullable, sample_values]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 1: Get the full downstream lineage chain
    - id: trace_pipeline_lineage
      mode: reason
      instruction: |
        Trace the downstream lineage from the source model to find all models
        in the pipeline that need the new column. Use dbt lineage to get the
        full chain (e.g., stg_orders → int_orders_enriched → fct_orders).

        For each model in the chain, collect:
        - model name
        - file path
        - layer (staging, intermediate, fact, dimension)
        - existing columns (to find insertion point)
        - materialization type

        Return the models_in_chain as an ordered list from source to final.
      tools:
        - name: dbt_mcp.get_lineage_dev
          instruction: "Get downstream lineage for the source model"
        - name: github_cli.search_code
          instruction: "Search for model SQL files to get paths"
      context:
        static: []
        dynamic:
          - from: resolve_source_column
            select: [model_name, column_info]
      output_schema:
        type: object
        required: [models_in_chain, chain_length]
        properties:
          models_in_chain:
            type: array
            items:
              type: object
              required: [model_name, model_path, layer, existing_columns, materialization]
      budget:
        max_llm_turns: 5
        max_tokens: 12000

    # Step 2: Show the threading plan for approval
    - id: show_plan
      mode: execute
      type: delegate
      delegate: shift.show_plan
      requires_approval: true
      context:
        dynamic:
          - from: trace_pipeline_lineage
            select: [models_in_chain, chain_length]
          - from: resolve_source_column
            select: [column_info, model_name]

    # Step 3: Modify each model SQL to add the column — LOOP
    - id: modify_each_model
      mode: reason
      loop:
        over: trace_pipeline_lineage.models_in_chain
        as: current_model
      instruction: |
        Read the SQL file for {{current_model.model_name}} and add the new column.
        Follow the layer-specific rules:
        - Staging: Add to SELECT from source, apply snake_case conversion if needed
        - Intermediate: Add to SELECT from ref(), handle GROUP BY if present
        - Fact/Dimension: Add to final SELECT

        Place the column near related columns, not at the end of the SELECT.
        Preserve existing formatting and style.

        Output the modified SQL content and a description of what changed.
      tools:
        - name: github_cli.read_file
          instruction: "Read the current model SQL file"
      context:
        static: [naming_conventions, pipeline_threading_conventions]
        dynamic:
          - from: resolve_source_column
            select: column_info
      output_schema:
        type: object
        required: [model_name, modified_sql, changes_description]
      budget:
        max_llm_turns: 3
        max_tokens: 10000

    # Step 4: Update schema.yml for each model — LOOP
    - id: update_each_schema
      mode: reason
      loop:
        over: trace_pipeline_lineage.models_in_chain
        as: current_model
      instruction: |
        Read the schema.yml file for {{current_model.model_name}} and add the
        new column entry. Include:
        - Column name (snake_case)
        - Clear business-friendly description
        - Appropriate tests based on column type and layer:
          - PK columns: not_null + unique
          - FK columns: not_null + relationships
          - Nullable columns: no not_null test
          - Enum columns: accepted_values

        If no schema.yml exists for this model, create one with the model
        description and the new column entry.
      tools:
        - name: github_cli.read_file
          instruction: "Read the existing schema.yml for this model"
      context:
        static: [testing_standards]
        dynamic:
          - from: resolve_source_column
            select: column_info
      output_schema:
        type: object
        required: [model_name, schema_yml_content, tests_added]
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 5: Validate all modified models compile and pass tests
    - id: validate_pipeline
      mode: reason
      instruction: |
        Compile and test all modified models to ensure the column was threaded
        correctly through the entire pipeline. Run dbt compile on each model,
        then dbt test to verify all tests pass (including newly added tests).

        Report any compilation errors or test failures with specific details
        about which model and which test failed.
      tools:
        - name: dbt_mcp.compile
          instruction: "Compile each modified model to check for SQL errors"
        - name: dbt_mcp.test
          instruction: "Run tests on each modified model"
      context:
        static: []
        dynamic:
          - from: modify_each_model
          - from: update_each_schema
      output_schema:
        type: object
        required: [compile_ok, tests_ok, errors]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 6: Create PR with all changes
    - id: create_pr
      mode: execute
      type: delegate
      delegate: shift.create_pr
      requires_approval: true
      context:
        dynamic:
          - from: resolve_source_column
            select: [model_name, column_info]
          - from: trace_pipeline_lineage
            select: models_in_chain
          - from: modify_each_model
          - from: update_each_schema
          - from: validate_pipeline
            select: [compile_ok, tests_ok]
```

### 1.3 — Conformance Test Spec

- [ ] Create `content/workflows/thread-field-through-pipeline.test.yml`

```yaml
conformance:
  workflow_id: thread-field-through-pipeline

  steps:
    resolve_source_column:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - naming_conventions
        - sf_type_mapping
      tools_include:
        - snowflake_mcp.describe_table
        - snowflake_mcp.execute_query
        - github_cli.read_file
      has_instruction: true

    trace_pipeline_lineage:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - dbt_mcp.get_lineage_dev
        - github_cli.search_code
      dynamic_refs_from:
        - resolve_source_column
      has_instruction: true

    show_plan:
      type: DelegateRequest
      requires_approval: true

    modify_each_model:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - naming_conventions
        - pipeline_threading_conventions
      tools_include:
        - github_cli.read_file
      dynamic_refs_from:
        - resolve_source_column
      has_instruction: true
      has_loop: true
      loop_over: trace_pipeline_lineage.models_in_chain
      loop_as: current_model

    update_each_schema:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - testing_standards
      tools_include:
        - github_cli.read_file
      dynamic_refs_from:
        - resolve_source_column
      has_instruction: true
      has_loop: true
      loop_over: trace_pipeline_lineage.models_in_chain
      loop_as: current_model

    validate_pipeline:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - dbt_mcp.compile
        - dbt_mcp.test
      dynamic_refs_from:
        - modify_each_model
        - update_each_schema
      has_instruction: true

    create_pr:
      type: DelegateRequest
      requires_approval: true
```

### 1.4 — Cassette Files

The test scenario: threading `VENUE_CAPACITY` through a 3-model pipeline: `stg_venues → int_venues_enriched → fct_event_sales`.

- [ ] Create `tests/cassettes/thread-field-through-pipeline/resolve_source_column.json`

```json
{
  "output": {
    "model_name": "stg_venues",
    "model_path": "models/staging/ticketing/stg_venues.sql",
    "table_fqn": "DW.STAGING.STG_VENUES",
    "column_info": {
      "name": "VENUE_CAPACITY",
      "sf_type": "NUMBER(10,0)",
      "nullable": true,
      "sample_values": [20000, 45000, 5500, null, 18000]
    }
  },
  "decision_log": {
    "decision": "Found VENUE_CAPACITY in stg_venues backing table",
    "rationale": "Column exists in DW.STAGING.STG_VENUES with NUMBER(10,0) type. Nullable — 3% null rate. Represents venue seating capacity.",
    "alternatives_considered": [
      {"option": "CAPACITY", "reason": "Similar name exists but refers to parking capacity — VENUE_CAPACITY is the correct column"}
    ],
    "confidence": "high",
    "informed_by": ["INFORMATION_SCHEMA.COLUMNS", "sample query"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.describe_table", "input": "DW.STAGING.STG_VENUES"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT VENUE_CAPACITY FROM DW.STAGING.STG_VENUES LIMIT 5"},
    {"tool": "github_cli.read_file", "input": "models/staging/ticketing/stg_venues.sql"}
  ],
  "token_usage": {"prompt": 1300, "completion": 450}
}
```

- [ ] Create `tests/cassettes/thread-field-through-pipeline/trace_pipeline_lineage.json`

```json
{
  "output": {
    "models_in_chain": [
      {
        "model_name": "stg_venues",
        "model_path": "models/staging/ticketing/stg_venues.sql",
        "layer": "staging",
        "existing_columns": ["VENUE_ID", "VENUE_NAME", "CITY", "STATE", "COUNTRY", "CREATED_AT"],
        "materialization": "view"
      },
      {
        "model_name": "int_venues_enriched",
        "model_path": "models/intermediate/ticketing/int_venues_enriched.sql",
        "layer": "intermediate",
        "existing_columns": ["VENUE_ID", "VENUE_NAME", "CITY", "STATE", "COUNTRY", "REGION", "TIMEZONE", "CREATED_AT"],
        "materialization": "table"
      },
      {
        "model_name": "fct_event_sales",
        "model_path": "models/marts/core/fct_event_sales.sql",
        "layer": "fact",
        "existing_columns": ["EVENT_SALE_ID", "EVENT_DATE", "VENUE_ID", "VENUE_NAME", "TICKET_COUNT", "GMS", "CREATED_AT"],
        "materialization": "incremental"
      }
    ],
    "chain_length": 3
  },
  "decision_log": {
    "decision": "Traced 3-model pipeline: stg_venues → int_venues_enriched → fct_event_sales",
    "rationale": "dbt lineage shows stg_venues feeds int_venues_enriched which feeds fct_event_sales. All three models need VENUE_CAPACITY added.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.get_lineage_dev", "github_cli.search_code"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.get_lineage_dev", "input": "stg_venues"},
    {"tool": "github_cli.search_code", "input": "filename:int_venues_enriched.sql"},
    {"tool": "github_cli.search_code", "input": "filename:fct_event_sales.sql"}
  ],
  "token_usage": {"prompt": 1500, "completion": 600}
}
```

- [ ] Create `tests/cassettes/thread-field-through-pipeline/modify_each_model.json`

This cassette contains 3 outputs (one per loop iteration):

```json
{
  "loop_outputs": [
    {
      "iteration": 0,
      "loop_item": {
        "model_name": "stg_venues",
        "model_path": "models/staging/ticketing/stg_venues.sql",
        "layer": "staging"
      },
      "output": {
        "model_name": "stg_venues",
        "modified_sql": "WITH source AS (\n    SELECT * FROM {{ source('ticketing', 'venues') }}\n)\n\nSELECT\n    venue_id,\n    venue_name,\n    venue_capacity,\n    city,\n    state,\n    country,\n    created_at\nFROM source",
        "changes_description": "Added venue_capacity to SELECT list after venue_name (related attribute). Applied snake_case naming."
      }
    },
    {
      "iteration": 1,
      "loop_item": {
        "model_name": "int_venues_enriched",
        "model_path": "models/intermediate/ticketing/int_venues_enriched.sql",
        "layer": "intermediate"
      },
      "output": {
        "model_name": "int_venues_enriched",
        "modified_sql": "WITH venues AS (\n    SELECT * FROM {{ ref('stg_venues') }}\n),\n\nregions AS (\n    SELECT * FROM {{ ref('seed_regions') }}\n)\n\nSELECT\n    v.venue_id,\n    v.venue_name,\n    v.venue_capacity,\n    v.city,\n    v.state,\n    v.country,\n    r.region,\n    r.timezone,\n    v.created_at\nFROM venues v\nLEFT JOIN regions r ON v.country = r.country AND v.state = r.state",
        "changes_description": "Added v.venue_capacity to SELECT after venue_name. No GROUP BY in this model — simple passthrough."
      }
    },
    {
      "iteration": 2,
      "loop_item": {
        "model_name": "fct_event_sales",
        "model_path": "models/marts/core/fct_event_sales.sql",
        "layer": "fact"
      },
      "output": {
        "model_name": "fct_event_sales",
        "modified_sql": "WITH events AS (\n    SELECT * FROM {{ ref('stg_events') }}\n),\n\nvenues AS (\n    SELECT * FROM {{ ref('int_venues_enriched') }}\n),\n\nsales AS (\n    SELECT * FROM {{ ref('stg_ticket_sales') }}\n)\n\nSELECT\n    s.event_sale_id,\n    e.event_date,\n    v.venue_id,\n    v.venue_name,\n    v.venue_capacity,\n    s.ticket_count,\n    s.gms,\n    s.created_at\nFROM sales s\nJOIN events e ON s.event_id = e.event_id\nJOIN venues v ON e.venue_id = v.venue_id",
        "changes_description": "Added v.venue_capacity to SELECT after venue_name. Column sourced from int_venues_enriched via JOIN."
      }
    }
  ],
  "decision_log": {
    "decision": "Modified all 3 models to include venue_capacity",
    "rationale": "Column threaded through stg → int → fct. Placement follows convention: near related venue columns. No GROUP BY conflicts in any model.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["github_cli.read_file", "pipeline_threading_conventions"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/staging/ticketing/stg_venues.sql"},
    {"tool": "github_cli.read_file", "input": "models/intermediate/ticketing/int_venues_enriched.sql"},
    {"tool": "github_cli.read_file", "input": "models/marts/core/fct_event_sales.sql"}
  ],
  "token_usage": {"prompt": 3600, "completion": 1200}
}
```

- [ ] Create `tests/cassettes/thread-field-through-pipeline/update_each_schema.json`

This cassette contains 3 outputs (one per loop iteration):

```json
{
  "loop_outputs": [
    {
      "iteration": 0,
      "loop_item": {
        "model_name": "stg_venues",
        "model_path": "models/staging/ticketing/stg_venues.sql",
        "layer": "staging"
      },
      "output": {
        "model_name": "stg_venues",
        "schema_yml_content": "version: 2\n\nmodels:\n  - name: stg_venues\n    description: Staged venue data from ticketing source.\n    columns:\n      - name: venue_id\n        description: Unique venue identifier.\n        tests:\n          - not_null\n          - unique\n      - name: venue_name\n        description: Display name of the venue.\n        tests:\n          - not_null\n      - name: venue_capacity\n        description: Maximum seating capacity of the venue.\n      - name: city\n        description: City where the venue is located.\n      - name: state\n        description: State or province where the venue is located.\n      - name: country\n        description: Country where the venue is located.\n        tests:\n          - not_null\n      - name: created_at\n        description: Timestamp when the venue record was created.\n        tests:\n          - not_null\n",
        "tests_added": [
          {"column": "venue_capacity", "tests": []}
        ]
      }
    },
    {
      "iteration": 1,
      "loop_item": {
        "model_name": "int_venues_enriched",
        "model_path": "models/intermediate/ticketing/int_venues_enriched.sql",
        "layer": "intermediate"
      },
      "output": {
        "model_name": "int_venues_enriched",
        "schema_yml_content": "version: 2\n\nmodels:\n  - name: int_venues_enriched\n    description: Venues enriched with region and timezone data.\n    columns:\n      - name: venue_id\n        description: Unique venue identifier.\n        tests:\n          - not_null\n          - unique\n      - name: venue_name\n        description: Display name of the venue.\n        tests:\n          - not_null\n      - name: venue_capacity\n        description: Maximum seating capacity of the venue.\n      - name: city\n        description: City where the venue is located.\n      - name: state\n        description: State or province where the venue is located.\n      - name: country\n        description: Country where the venue is located.\n        tests:\n          - not_null\n      - name: region\n        description: Geographic region derived from country and state.\n      - name: timezone\n        description: Timezone of the venue location.\n      - name: created_at\n        description: Timestamp when the venue record was created.\n        tests:\n          - not_null\n",
        "tests_added": [
          {"column": "venue_capacity", "tests": []}
        ]
      }
    },
    {
      "iteration": 2,
      "loop_item": {
        "model_name": "fct_event_sales",
        "model_path": "models/marts/core/fct_event_sales.sql",
        "layer": "fact"
      },
      "output": {
        "model_name": "fct_event_sales",
        "schema_yml_content": "version: 2\n\nmodels:\n  - name: fct_event_sales\n    description: Fact table for event ticket sales with venue details.\n    columns:\n      - name: event_sale_id\n        description: Unique identifier for the event sale.\n        tests:\n          - not_null\n          - unique\n      - name: event_date\n        description: Date of the event.\n        tests:\n          - not_null\n      - name: venue_id\n        description: Foreign key to the venue dimension.\n        tests:\n          - not_null\n      - name: venue_name\n        description: Display name of the venue.\n        tests:\n          - not_null\n      - name: venue_capacity\n        description: Maximum seating capacity of the venue. Nullable for venues without reported capacity.\n      - name: ticket_count\n        description: Number of tickets sold.\n        tests:\n          - not_null\n      - name: gms\n        description: Gross merchandise sales amount.\n        tests:\n          - not_null\n      - name: created_at\n        description: Timestamp when the sale record was created.\n        tests:\n          - not_null\n",
        "tests_added": [
          {"column": "venue_capacity", "tests": []}
        ]
      }
    }
  ],
  "decision_log": {
    "decision": "Updated schema.yml for all 3 models with venue_capacity column",
    "rationale": "Column is nullable (3% null rate) so no not_null test added. Description explains the business meaning. Placed after venue_name in column order.",
    "alternatives_considered": [
      {"option": "Add not_null test", "reason": "Skipped — 3% null rate indicates intentional nulls for venues without reported capacity"}
    ],
    "confidence": "high",
    "informed_by": ["testing_standards", "column_info"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/staging/ticketing/schema.yml"},
    {"tool": "github_cli.read_file", "input": "models/intermediate/ticketing/schema.yml"},
    {"tool": "github_cli.read_file", "input": "models/marts/core/schema.yml"}
  ],
  "token_usage": {"prompt": 3200, "completion": 1100}
}
```

- [ ] Create `tests/cassettes/thread-field-through-pipeline/validate_pipeline.json`

```json
{
  "output": {
    "compile_ok": true,
    "tests_ok": true,
    "errors": []
  },
  "decision_log": {
    "decision": "All 3 models compile and pass tests",
    "rationale": "dbt compile succeeded for stg_venues, int_venues_enriched, fct_event_sales. All existing tests pass. No new test failures introduced.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.compile", "dbt_mcp.test"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.compile", "input": "dbt compile --select stg_venues int_venues_enriched fct_event_sales"},
    {"tool": "dbt_mcp.test", "input": "dbt test --select stg_venues int_venues_enriched fct_event_sales"}
  ],
  "token_usage": {"prompt": 1800, "completion": 300}
}
```

### 1.5 — Golden File

- [ ] Create `tests/goldens/thread-field-through-pipeline/pipeline_threading_result.json`

```json
{
  "column_name": "VENUE_CAPACITY",
  "source_model": "stg_venues",
  "chain_length": 3,
  "models_modified": ["stg_venues", "int_venues_enriched", "fct_event_sales"],
  "column_sf_type": "NUMBER(10,0)",
  "compile_ok": true,
  "tests_ok": true
}
```

### 1.6 — E2E Test

- [ ] Create `tests/test_e2e_thread_field_through_pipeline.py`

```python
"""
End-to-end test for the thread-field-through-pipeline workflow.

Tests the full 7-step workflow with cassette responses, including
LOOP steps that iterate over a 3-model pipeline chain.

Test scenario: Thread VENUE_CAPACITY through stg_venues → int_venues_enriched → fct_event_sales.
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
CASSETTE_DIR = Path(__file__).parent / "cassettes" / "thread-field-through-pipeline"
GOLDEN_DIR = Path(__file__).parent / "goldens" / "thread-field-through-pipeline"

# All 7 logical steps in execution order.
# Loop steps (modify_each_model, update_each_schema) execute 3 times each,
# so actual step count is 3 + 3*2 + 2 = 11 step executions.
EXPECTED_LOGICAL_STEPS = [
    "resolve_source_column",
    "trace_pipeline_lineage",
    "show_plan",
    "modify_each_model",
    "update_each_schema",
    "validate_pipeline",
    "create_pr",
]

# Non-loop REASON steps that need simple cassettes
SIMPLE_REASON_STEPS = ["resolve_source_column", "trace_pipeline_lineage", "validate_pipeline"]

# Loop steps with multi-output cassettes
LOOP_STEPS = ["modify_each_model", "update_each_schema"]

INPUTS = {"column_name": "VENUE_CAPACITY", "source_model": "stg_venues"}

CHAIN_LENGTH = 3


def load_cassettes() -> dict[str, dict]:
    """Load all cassettes for the thread-field-through-pipeline test."""
    cassettes = {}
    for step_id in SIMPLE_REASON_STEPS + LOOP_STEPS:
        path = CASSETTE_DIR / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow() -> tuple:
    """Drive the full workflow with cassette responses."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes()

    run = engine.start("thread-field-through-pipeline", INPUTS)
    assert run.status == "running"

    steps_executed = []
    reason_outputs = {}
    loop_iteration_count = {"modify_each_model": 0, "update_each_schema": 0}

    while run.status == "running":
        request = run.next_step()
        if request is None:
            break

        steps_executed.append(request.step_id)

        if isinstance(request, ReasonRequest):
            if request.step_id in LOOP_STEPS:
                # Loop step — feed the iteration-specific output
                idx = loop_iteration_count[request.step_id]
                cassette = cassettes[request.step_id]
                iteration_output = cassette["loop_outputs"][idx]["output"]
                reason_outputs.setdefault(request.step_id, []).append(iteration_output)
                run.record_result(
                    request.step_id,
                    StepSuccess(output=iteration_output),
                )
                loop_iteration_count[request.step_id] += 1
            else:
                # Simple reason step
                cassette = cassettes[request.step_id]
                reason_outputs[request.step_id] = cassette["output"]
                run.record_result(
                    request.step_id,
                    StepSuccess(output=cassette["output"]),
                )

        elif isinstance(request, DelegateRequest):
            if request.step_id == "show_plan":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"approved": True, "user_feedback": None}),
                )
            elif request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/99"}),
                )

    return run, steps_executed, reason_outputs


class TestThreadFieldThroughPipeline:
    """Thread VENUE_CAPACITY through a 3-model pipeline."""

    def test_workflow_completes(self):
        run, steps_executed, _ = run_workflow()
        assert run.status == "completed"

    def test_total_step_executions(self):
        """7 logical steps, but loop steps execute 3x each = 11 total."""
        _, steps_executed, _ = run_workflow()
        # resolve_source_column, trace_pipeline_lineage, show_plan,
        # modify_each_model x3, update_each_schema x3,
        # validate_pipeline, create_pr
        assert len(steps_executed) == 11

    def test_loop_steps_execute_3_times_each(self):
        _, steps_executed, _ = run_workflow()
        assert steps_executed.count("modify_each_model") == CHAIN_LENGTH
        assert steps_executed.count("update_each_schema") == CHAIN_LENGTH

    def test_column_info_resolved(self):
        _, _, reason_outputs = run_workflow()
        col = reason_outputs["resolve_source_column"]["column_info"]
        assert col["name"] == "VENUE_CAPACITY"
        assert col["sf_type"] == "NUMBER(10,0)"

    def test_pipeline_chain_has_3_models(self):
        _, _, reason_outputs = run_workflow()
        chain = reason_outputs["trace_pipeline_lineage"]["models_in_chain"]
        assert len(chain) == CHAIN_LENGTH
        assert chain[0]["model_name"] == "stg_venues"
        assert chain[1]["model_name"] == "int_venues_enriched"
        assert chain[2]["model_name"] == "fct_event_sales"

    def test_all_3_models_modified(self):
        _, _, reason_outputs = run_workflow()
        modifications = reason_outputs["modify_each_model"]
        assert len(modifications) == CHAIN_LENGTH
        model_names = [m["model_name"] for m in modifications]
        assert model_names == ["stg_venues", "int_venues_enriched", "fct_event_sales"]

    def test_modified_sql_contains_column(self):
        _, _, reason_outputs = run_workflow()
        for mod in reason_outputs["modify_each_model"]:
            assert "venue_capacity" in mod["modified_sql"].lower()

    def test_all_3_schemas_updated(self):
        _, _, reason_outputs = run_workflow()
        schemas = reason_outputs["update_each_schema"]
        assert len(schemas) == CHAIN_LENGTH
        model_names = [s["model_name"] for s in schemas]
        assert model_names == ["stg_venues", "int_venues_enriched", "fct_event_sales"]

    def test_schema_yml_contains_column(self):
        _, _, reason_outputs = run_workflow()
        for schema in reason_outputs["update_each_schema"]:
            assert "venue_capacity" in schema["schema_yml_content"].lower()

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow()
        assert reason_outputs["validate_pipeline"]["compile_ok"] is True
        assert reason_outputs["validate_pipeline"]["tests_ok"] is True
        assert reason_outputs["validate_pipeline"]["errors"] == []

    def test_golden_match(self):
        _, _, reason_outputs = run_workflow()
        with open(GOLDEN_DIR / "pipeline_threading_result.json") as f:
            golden = json.load(f)
        assert reason_outputs["trace_pipeline_lineage"]["chain_length"] == golden["chain_length"]
        model_names = [m["model_name"] for m in reason_outputs["modify_each_model"]]
        assert model_names == golden["models_modified"]

    def test_trace_has_all_step_executions(self):
        run, _, _ = run_workflow()
        trace = run.get_trace()
        assert trace["workflow_id"] == "thread-field-through-pipeline"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 11

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("thread-field-through-pipeline", INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_source_column"
        assert request.persona.id == "analytics_engineer"
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_show_plan_requires_approval(self):
        """Verify show_plan is a delegate with approval gate."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes()
        run = engine.start("thread-field-through-pipeline", INPUTS)

        # Walk through first 2 REASON steps
        for step_id in ["resolve_source_column", "trace_pipeline_lineage"]:
            request = run.next_step()
            assert request.step_id == step_id
            run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        # Step 3 should be show_plan (DelegateRequest)
        request = run.next_step()
        assert isinstance(request, DelegateRequest)
        assert request.step_id == "show_plan"
        assert request.requires_approval is True
```

### 1.7 — Conformance Test

- [ ] Create `tests/test_conformance_thread_field_through_pipeline.py`

```python
"""Conformance tests for thread-field-through-pipeline workflow.

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


class TestThreadFieldConformance:
    """Validate that thread-field-through-pipeline assembles correct context per step."""

    WORKFLOW_ID = "thread-field-through-pipeline"
    INPUTS = {"column_name": "VENUE_CAPACITY", "source_model": "stg_venues"}

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

        # Dummy outputs to advance through steps
        step_outputs = {
            "resolve_source_column": {
                "model_name": "stg_venues",
                "model_path": "models/staging/ticketing/stg_venues.sql",
                "table_fqn": "DW.STAGING.STG_VENUES",
                "column_info": {
                    "name": "VENUE_CAPACITY",
                    "sf_type": "NUMBER(10,0)",
                    "nullable": True,
                    "sample_values": [20000, 45000, 5500],
                },
            },
            "trace_pipeline_lineage": {
                "models_in_chain": [
                    {"model_name": "stg_venues", "model_path": "models/staging/ticketing/stg_venues.sql", "layer": "staging", "existing_columns": ["VENUE_ID", "VENUE_NAME"], "materialization": "view"},
                    {"model_name": "int_venues_enriched", "model_path": "models/intermediate/ticketing/int_venues_enriched.sql", "layer": "intermediate", "existing_columns": ["VENUE_ID", "VENUE_NAME"], "materialization": "table"},
                    {"model_name": "fct_event_sales", "model_path": "models/marts/core/fct_event_sales.sql", "layer": "fact", "existing_columns": ["EVENT_SALE_ID", "VENUE_ID"], "materialization": "incremental"},
                ],
                "chain_length": 3,
            },
            "modify_each_model": {
                "model_name": "stg_venues",
                "modified_sql": "SELECT venue_id, venue_name, venue_capacity FROM source",
                "changes_description": "Added venue_capacity",
            },
            "update_each_schema": {
                "model_name": "stg_venues",
                "schema_yml_content": "version: 2\nmodels:\n  - name: stg_venues",
                "tests_added": [],
            },
            "validate_pipeline": {
                "compile_ok": True,
                "tests_ok": True,
                "errors": [],
            },
        }

        visited_steps = []

        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            step_id = request.step_id

            # Check conformance spec for this logical step
            if step_id in conformance["steps"]:
                spec = conformance["steps"][step_id]
                expected_type = type_map[spec["type"]]
                assert isinstance(request, expected_type), (
                    f"Step '{step_id}': expected {spec['type']}, got {type(request).__name__}"
                )

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
                    if "has_instruction" in spec and spec["has_instruction"]:
                        assert request.instruction and len(request.instruction.strip()) > 0, (
                            f"Step '{step_id}': expected non-empty instruction"
                        )
                    if "knowledge_includes" in spec:
                        for kid in spec["knowledge_includes"]:
                            assert kid in request.context.static, (
                                f"Step '{step_id}': missing knowledge '{kid}' in static context. Has: {list(request.context.static.keys())}"
                            )

                if isinstance(request, DelegateRequest):
                    if "requires_approval" in spec:
                        assert request.requires_approval == spec["requires_approval"], (
                            f"Step '{step_id}': requires_approval mismatch"
                        )

            visited_steps.append(step_id)

            # Record dummy results to advance
            if isinstance(request, ReasonRequest):
                output = step_outputs.get(step_id, {"placeholder": True})
                run.record_result(step_id, StepSuccess(output=output))
            elif isinstance(request, DelegateRequest):
                run.record_result(step_id, StepSuccess(output={"approved": True}))

        # Verify all conformance steps were visited
        for step_id in conformance["steps"]:
            assert step_id in visited_steps, (
                f"Conformance step '{step_id}' was never visited"
            )

    def test_conformance_covers_all_steps(self, engine, conformance):
        """Ensure conformance spec covers every logical step in the workflow."""
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

### 1.8 — Manifest Entry

- [ ] Update `content/workflows/manifest.yml` — add thread-field-through-pipeline entry

```yaml
  - id: thread-field-through-pipeline
    name: Thread Field Through dbt Pipeline
    persona: analytics_engineer
    triggers:
      keywords: [thread column, add field through pipeline, propagate column, thread field]
      input_pattern: "{column} through {model} pipeline"
```

### 1.9 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add \
  content/knowledge/pipeline_threading_conventions.yml \
  content/knowledge/testing_standards.yml \
  content/workflows/thread-field-through-pipeline.yml \
  content/workflows/thread-field-through-pipeline.test.yml \
  content/workflows/manifest.yml \
  tests/cassettes/thread-field-through-pipeline/resolve_source_column.json \
  tests/cassettes/thread-field-through-pipeline/trace_pipeline_lineage.json \
  tests/cassettes/thread-field-through-pipeline/modify_each_model.json \
  tests/cassettes/thread-field-through-pipeline/update_each_schema.json \
  tests/cassettes/thread-field-through-pipeline/validate_pipeline.json \
  tests/goldens/thread-field-through-pipeline/pipeline_threading_result.json \
  tests/test_e2e_thread_field_through_pipeline.py \
  tests/test_conformance_thread_field_through_pipeline.py

git commit -m "feat: add thread-field-through-pipeline workflow with loop steps

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: configure-ingestion-pipeline workflow

### 2.1 — Knowledge Files

- [ ] Create `content/knowledge/database_classes.yml`

```yaml
knowledge:
  id: database_classes
  domain: ingestion
  description: "Catalog of source database classes and their ingestion patterns"

  classes:
    sql_server:
      display_name: "SQL Server"
      connection_type: "linked_service"
      ingestion_method: "SqlServerTable()"
      config_file: "dags/configs/sql_server_tables.py"
      staging_schema: "STAGING"
      default_warehouse: "LOADING_S"
      default_schedule: "0 */30 * * *"
      notes:
        - "Uses linked service via Azure Data Factory"
        - "Config uses SqlServerTable() dataclass in Astronomer repo"
        - "Primary key must be specified for incremental loads"

    snowflake_share:
      display_name: "Snowflake Data Share"
      connection_type: "share"
      ingestion_method: "CREATE DATABASE FROM SHARE"
      config_file: null
      staging_schema: null
      default_warehouse: null
      default_schedule: null
      notes:
        - "Shares are auto-updated — no ingestion pipeline needed"
        - "Create a view in DW pointing to shared database"

    api:
      display_name: "REST API"
      connection_type: "http"
      ingestion_method: "PythonOperator + COPY INTO"
      config_file: "dags/api_ingestion/"
      staging_schema: "STAGING"
      default_warehouse: "LOADING_S"
      default_schedule: "0 */60 * * *"
      notes:
        - "Custom Python operator per API"
        - "Land as JSON in blob storage, then COPY INTO"
        - "Always add rate limiting and retry logic"

    blob_storage:
      display_name: "Azure Blob / S3"
      connection_type: "storage_integration"
      ingestion_method: "Snowpipe or COPY INTO"
      config_file: null
      staging_schema: "STAGING"
      default_warehouse: "LOADING_S"
      default_schedule: "AUTO_INGEST"
      notes:
        - "Prefer Snowpipe for continuous ingestion"
        - "Use COPY INTO for batch/scheduled loads"
        - "Match by column name for Parquet files"

  default_class: "sql_server"
```

- [ ] Create `content/knowledge/sla_contracts.yml`

```yaml
knowledge:
  id: sla_contracts
  domain: operations
  description: "SLA contracts for data freshness by domain and tier"

  tiers:
    tier_1:
      name: "Real-time"
      max_latency_minutes: 15
      schedule: "*/5 * * * *"
      examples: ["transaction events", "listing events", "pricing signals"]
      monitoring: "PagerDuty alert on > 15min stale"

    tier_2:
      name: "Near-real-time"
      max_latency_minutes: 60
      schedule: "0 * * * *"
      examples: ["inventory snapshots", "user activity aggregates"]
      monitoring: "Slack alert on > 60min stale"

    tier_3:
      name: "Batch"
      max_latency_minutes: 480
      schedule: "0 */4 * * *"
      examples: ["financial reconciliation", "partner reports"]
      monitoring: "Daily freshness check"

    tier_4:
      name: "Daily"
      max_latency_minutes: 1440
      schedule: "0 6 * * *"
      examples: ["dimension tables", "reference data", "configuration"]
      monitoring: "Morning health check"

  heuristics:
    - "If source updates > 100 times/hour → Tier 1 or Tier 2"
    - "If source updates 1-100 times/hour → Tier 2 or Tier 3"
    - "If source updates < 1 time/hour → Tier 3 or Tier 4"
    - "Financial data defaults to Tier 3 (reconciliation needs)"
    - "Customer-facing data defaults to Tier 2 minimum"
    - "Internal reporting defaults to Tier 4"
```

### 2.2 — Workflow Definition

- [ ] Create `content/workflows/configure-ingestion-pipeline.yml`

```yaml
workflow:
  id: configure-ingestion-pipeline
  name: Configure Data Ingestion Pipeline
  persona: data_engineer

  inputs:
    table_name:
      type: string
      required: true
    source_database:
      type: string
      required: true
    database_class:
      type: string
      required: false

  steps:
    # Step 0: Discover source table schema and cache it
    - id: discover_source_schema
      mode: reason
      instruction: |
        Query the source database to discover the table schema. Get:
        - All column names, data types, and nullability
        - Primary key columns
        - Row count and approximate update frequency
        - Table description or comments if available

        Use INFORMATION_SCHEMA on the source database. If the source is a SQL Server
        linked via Snowflake, query the linked database's INFORMATION_SCHEMA.

        The discovered schema will be cached for subsequent steps to avoid
        redundant metadata queries.
      cache_as: source_schema
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query source database INFORMATION_SCHEMA for table metadata"
          usage_pattern: |
            1. SELECT * FROM {source_db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'
            2. SELECT COUNT(*) as row_count FROM {source_db}.{schema}.{table}
        - name: snowflake_mcp.describe_table
          instruction: "Get column details for the source table"
      context:
        static: [snowflake_environment, database_classes]
        dynamic: []
      output_schema:
        type: object
        required: [source_table_fqn, columns, primary_key, row_count, update_frequency]
        properties:
          columns:
            type: array
            items:
              type: object
              required: [name, source_type, nullable]
          update_frequency:
            type: string
            enum: [CONTINUOUS, HOURLY, DAILY, WEEKLY, UNKNOWN]
      budget:
        max_llm_turns: 5
        max_tokens: 12000

    # Step 1: Design the staging table DDL
    - id: design_staging_table
      mode: reason
      instruction: |
        Design the Snowflake staging table DDL based on the source schema.
        Apply type mapping rules to convert source types to Snowflake types.
        Follow staging conventions:
        - Table name: STG_{SOURCE_TABLE_NAME}
        - Schema: STAGING
        - Include _LOADED_AT (TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())
        - Include _SOURCE (VARCHAR DEFAULT '{source_database}')
        - Use TRANSIENT table for cost savings
        - Apply snake_case naming for all columns
        - Flag any PII columns detected by pattern matching

        Output the complete CREATE TABLE statement and a column mapping table.
      tools: []
      context:
        static: [sf_type_mapping, naming_conventions, ingestion_conventions]
        cache: [source_schema]
      output_schema:
        type: object
        required: [staging_table_fqn, create_table_sql, column_mapping, pii_columns]
      budget:
        max_llm_turns: 3
        max_tokens: 12000

    # Step 2: Generate the ingestion config (e.g., SqlServerTable())
    - id: generate_ingestion_config
      mode: reason
      instruction: |
        Generate the ingestion configuration for this table. The format depends
        on the database_class:

        For sql_server (default):
        - Generate a SqlServerTable() config entry for the Astronomer DAG
        - Include: table name, schema, primary key, incremental column, schedule
        - Read existing config file to match formatting and patterns

        For other classes:
        - Generate the appropriate config (Snowpipe DDL, Python operator, etc.)

        Output the complete config snippet ready to be inserted into the config file.
      tools:
        - name: github_cli.read_file
          instruction: "Read existing ingestion config file to match patterns"
      context:
        static: [ingestion_conventions, database_classes]
        dynamic:
          - from: design_staging_table
            select: [staging_table_fqn, column_mapping]
        cache: [source_schema]
      output_schema:
        type: object
        required: [config_snippet, config_file_path, database_class]
      budget:
        max_llm_turns: 3
        max_tokens: 10000

    # Step 3: Determine load schedule based on source patterns
    - id: configure_load_frequency
      mode: reason
      instruction: |
        Determine the optimal load frequency for this table based on:
        1. Source table update frequency (from discover_source_schema)
        2. Row count and data volume
        3. SLA contracts for the data domain
        4. Existing similar tables' schedules

        Query for recent update patterns if available. Check if the source
        has a change tracking mechanism (CDC, timestamps, etc.).

        Output the recommended cron schedule, SLA tier, and rationale.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Query for update patterns and existing similar schedules"
      context:
        static: [sla_contracts, load_frequency_heuristics]
        dynamic:
          - from: discover_source_schema
            select: [row_count, update_frequency]
      output_schema:
        type: object
        required: [cron_schedule, sla_tier, rationale, incremental_strategy]
        properties:
          incremental_strategy:
            type: string
            enum: [APPEND, UPSERT, SCD_TYPE_2, FULL_REFRESH]
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 4: Validate connectivity to source
    - id: validate_connectivity
      mode: reason
      instruction: |
        Test that the source table is reachable from Snowflake. Run a simple
        SELECT COUNT(*) or SELECT TOP 1 against the source. Verify:
        1. The connection works (no permission errors)
        2. The table has data (row count > 0)
        3. The columns we expect actually exist

        If the source is behind a linked service, verify the linked service
        is active. Report any connectivity issues clearly.
      tools:
        - name: snowflake_mcp.execute_query
          instruction: "Test connectivity with a simple query against the source table"
      context:
        static: []
        dynamic:
          - from: discover_source_schema
            select: source_table_fqn
      output_schema:
        type: object
        required: [connectivity_ok, row_count_verified, errors]
      budget:
        max_llm_turns: 3
        max_tokens: 6000

    # Step 5: Show the full ingestion plan for approval
    - id: show_plan
      mode: execute
      type: delegate
      delegate: shift.show_plan
      requires_approval: true
      context:
        dynamic:
          - from: discover_source_schema
            select: [source_table_fqn, columns, row_count]
          - from: design_staging_table
            select: [staging_table_fqn, create_table_sql, pii_columns]
          - from: generate_ingestion_config
            select: [config_snippet, config_file_path, database_class]
          - from: configure_load_frequency
            select: [cron_schedule, sla_tier, incremental_strategy]
          - from: validate_connectivity
            select: [connectivity_ok, errors]

    # Step 6: Create PR with all ingestion artifacts
    - id: create_pr
      mode: execute
      type: delegate
      delegate: shift.create_pr
      requires_approval: true
      context:
        dynamic:
          - from: design_staging_table
            select: [staging_table_fqn, create_table_sql]
          - from: generate_ingestion_config
            select: [config_snippet, config_file_path]
          - from: configure_load_frequency
            select: [cron_schedule, incremental_strategy]
          - from: validate_connectivity
            select: connectivity_ok
```

### 2.3 — Conformance Test Spec

- [ ] Create `content/workflows/configure-ingestion-pipeline.test.yml`

```yaml
conformance:
  workflow_id: configure-ingestion-pipeline

  steps:
    discover_source_schema:
      type: ReasonRequest
      persona: data_engineer
      knowledge_includes:
        - snowflake_environment
        - database_classes
      tools_include:
        - snowflake_mcp.execute_query
        - snowflake_mcp.describe_table
      has_instruction: true

    design_staging_table:
      type: ReasonRequest
      persona: data_engineer
      tools_count: 0
      knowledge_includes:
        - sf_type_mapping
        - naming_conventions
        - ingestion_conventions
      has_instruction: true

    generate_ingestion_config:
      type: ReasonRequest
      persona: data_engineer
      knowledge_includes:
        - ingestion_conventions
        - database_classes
      tools_include:
        - github_cli.read_file
      dynamic_refs_from:
        - design_staging_table
      has_instruction: true

    configure_load_frequency:
      type: ReasonRequest
      persona: data_engineer
      knowledge_includes:
        - sla_contracts
        - load_frequency_heuristics
      tools_include:
        - snowflake_mcp.execute_query
      dynamic_refs_from:
        - discover_source_schema
      has_instruction: true

    validate_connectivity:
      type: ReasonRequest
      persona: data_engineer
      tools_include:
        - snowflake_mcp.execute_query
      dynamic_refs_from:
        - discover_source_schema
      has_instruction: true

    show_plan:
      type: DelegateRequest
      requires_approval: true

    create_pr:
      type: DelegateRequest
      requires_approval: true
```

### 2.4 — Cassette Files

Test scenario: Ingesting `PARTNER_COMMISSION` table from SQL Server database `ECOMM`.

- [ ] Create `tests/cassettes/configure-ingestion-pipeline/discover_source_schema.json`

```json
{
  "output": {
    "source_table_fqn": "ECOMM.DBO.PARTNER_COMMISSION",
    "columns": [
      {"name": "CommissionId", "source_type": "BIGINT", "nullable": false},
      {"name": "PartnerId", "source_type": "INT", "nullable": false},
      {"name": "TransactionId", "source_type": "BIGINT", "nullable": false},
      {"name": "CommissionAmount", "source_type": "DECIMAL(18,2)", "nullable": false},
      {"name": "CommissionRate", "source_type": "DECIMAL(5,4)", "nullable": false},
      {"name": "CurrencyCode", "source_type": "VARCHAR(3)", "nullable": false},
      {"name": "CommissionType", "source_type": "VARCHAR(50)", "nullable": false},
      {"name": "Status", "source_type": "VARCHAR(20)", "nullable": false},
      {"name": "CreatedDate", "source_type": "DATETIME", "nullable": false},
      {"name": "ModifiedDate", "source_type": "DATETIME", "nullable": true}
    ],
    "primary_key": ["CommissionId"],
    "row_count": 8500000,
    "update_frequency": "HOURLY"
  },
  "decision_log": {
    "decision": "Discovered PARTNER_COMMISSION schema from ECOMM SQL Server database",
    "rationale": "Table has 10 columns, 8.5M rows. CommissionId is PK. ModifiedDate suggests CDC-friendly. Updates hourly based on recent row timestamps.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["INFORMATION_SCHEMA.COLUMNS", "row count query"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM ECOMM.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PARTNER_COMMISSION'"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT COUNT(*) as row_count FROM ECOMM.DBO.PARTNER_COMMISSION"},
    {"tool": "snowflake_mcp.describe_table", "input": "ECOMM.DBO.PARTNER_COMMISSION"}
  ],
  "token_usage": {"prompt": 1400, "completion": 500}
}
```

- [ ] Create `tests/cassettes/configure-ingestion-pipeline/design_staging_table.json`

```json
{
  "output": {
    "staging_table_fqn": "DW.STAGING.STG_PARTNER_COMMISSION",
    "create_table_sql": "CREATE TRANSIENT TABLE IF NOT EXISTS DW.STAGING.STG_PARTNER_COMMISSION (\n    COMMISSION_ID NUMBER(38,0) NOT NULL,\n    PARTNER_ID NUMBER(10,0) NOT NULL,\n    TRANSACTION_ID NUMBER(38,0) NOT NULL,\n    COMMISSION_AMOUNT NUMBER(18,2) NOT NULL,\n    COMMISSION_RATE NUMBER(5,4) NOT NULL,\n    CURRENCY_CODE VARCHAR(3) NOT NULL,\n    COMMISSION_TYPE VARCHAR(50) NOT NULL,\n    STATUS VARCHAR(20) NOT NULL,\n    CREATED_DATE TIMESTAMP_NTZ NOT NULL,\n    MODIFIED_DATE TIMESTAMP_NTZ,\n    _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),\n    _SOURCE VARCHAR DEFAULT 'ECOMM'\n);",
    "column_mapping": [
      {"source": "CommissionId", "target": "COMMISSION_ID", "source_type": "BIGINT", "target_type": "NUMBER(38,0)"},
      {"source": "PartnerId", "target": "PARTNER_ID", "source_type": "INT", "target_type": "NUMBER(10,0)"},
      {"source": "TransactionId", "target": "TRANSACTION_ID", "source_type": "BIGINT", "target_type": "NUMBER(38,0)"},
      {"source": "CommissionAmount", "target": "COMMISSION_AMOUNT", "source_type": "DECIMAL(18,2)", "target_type": "NUMBER(18,2)"},
      {"source": "CommissionRate", "target": "COMMISSION_RATE", "source_type": "DECIMAL(5,4)", "target_type": "NUMBER(5,4)"},
      {"source": "CurrencyCode", "target": "CURRENCY_CODE", "source_type": "VARCHAR(3)", "target_type": "VARCHAR(3)"},
      {"source": "CommissionType", "target": "COMMISSION_TYPE", "source_type": "VARCHAR(50)", "target_type": "VARCHAR(50)"},
      {"source": "Status", "target": "STATUS", "source_type": "VARCHAR(20)", "target_type": "VARCHAR(20)"},
      {"source": "CreatedDate", "target": "CREATED_DATE", "source_type": "DATETIME", "target_type": "TIMESTAMP_NTZ"},
      {"source": "ModifiedDate", "target": "MODIFIED_DATE", "source_type": "DATETIME", "target_type": "TIMESTAMP_NTZ"}
    ],
    "pii_columns": []
  },
  "decision_log": {
    "decision": "Designed staging table STG_PARTNER_COMMISSION with type mappings",
    "rationale": "Applied sf_type_mapping: BIGINT→NUMBER(38,0), INT→NUMBER(10,0), DATETIME→TIMESTAMP_NTZ. CamelCase→UPPER_SNAKE. Added _LOADED_AT and _SOURCE metadata columns. TRANSIENT for cost savings.",
    "alternatives_considered": [
      {"option": "Permanent table", "reason": "TRANSIENT preferred for staging — no failsafe bytes, cheaper"}
    ],
    "confidence": "high",
    "informed_by": ["sf_type_mapping", "naming_conventions", "ingestion_conventions"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 1800, "completion": 700}
}
```

- [ ] Create `tests/cassettes/configure-ingestion-pipeline/generate_ingestion_config.json`

```json
{
  "output": {
    "config_snippet": "SqlServerTable(\n    source_schema='dbo',\n    source_table='PARTNER_COMMISSION',\n    target_schema='STAGING',\n    target_table='STG_PARTNER_COMMISSION',\n    primary_key='COMMISSION_ID',\n    incremental_column='MODIFIED_DATE',\n    warehouse='LOADING_S',\n),",
    "config_file_path": "dags/configs/sql_server_tables.py",
    "database_class": "sql_server"
  },
  "decision_log": {
    "decision": "Generated SqlServerTable() config entry",
    "rationale": "Read existing sql_server_tables.py to match formatting. Used MODIFIED_DATE as incremental column (supports CDC via timestamp). LOADING_S warehouse per convention.",
    "alternatives_considered": [
      {"option": "CREATED_DATE as incremental", "reason": "MODIFIED_DATE is better — captures updates, not just inserts"}
    ],
    "confidence": "high",
    "informed_by": ["existing config file", "database_classes", "ingestion_conventions"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "dags/configs/sql_server_tables.py"}
  ],
  "token_usage": {"prompt": 1600, "completion": 400}
}
```

- [ ] Create `tests/cassettes/configure-ingestion-pipeline/configure_load_frequency.json`

```json
{
  "output": {
    "cron_schedule": "0 * * * *",
    "sla_tier": "tier_2",
    "rationale": "Source updates hourly (HOURLY frequency detected). 8.5M rows with MODIFIED_DATE incremental column supports efficient upsert. Similar partner tables in the pipeline run hourly. SLA tier 2 (near-real-time, max 60min latency) is appropriate for financial commission data.",
    "incremental_strategy": "UPSERT"
  },
  "decision_log": {
    "decision": "Set hourly schedule with UPSERT strategy",
    "rationale": "Source update_frequency=HOURLY, row_count=8.5M. MODIFIED_DATE enables incremental upsert. Commission data is financial → Tier 2 SLA minimum. Checked existing partner ingestion configs — all run hourly.",
    "alternatives_considered": [
      {"option": "Every 30 minutes", "reason": "Hourly is sufficient — source only updates hourly"},
      {"option": "APPEND strategy", "reason": "Table has updates (MODIFIED_DATE) — UPSERT needed"}
    ],
    "confidence": "high",
    "informed_by": ["sla_contracts", "load_frequency_heuristics", "existing schedules"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT DATE_TRUNC('hour', MODIFIED_DATE), COUNT(*) FROM ECOMM.DBO.PARTNER_COMMISSION WHERE MODIFIED_DATE >= DATEADD('day', -7, CURRENT_TIMESTAMP()) GROUP BY 1 ORDER BY 1 DESC LIMIT 24"}
  ],
  "token_usage": {"prompt": 1200, "completion": 350}
}
```

- [ ] Create `tests/cassettes/configure-ingestion-pipeline/validate_connectivity.json`

```json
{
  "output": {
    "connectivity_ok": true,
    "row_count_verified": true,
    "errors": []
  },
  "decision_log": {
    "decision": "Source table is reachable and has data",
    "rationale": "SELECT COUNT(*) returned 8,500,000 rows. All expected columns exist. Linked service to ECOMM is active. No permission errors.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["snowflake_mcp.execute_query"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT COUNT(*) FROM ECOMM.DBO.PARTNER_COMMISSION"}
  ],
  "token_usage": {"prompt": 800, "completion": 200}
}
```

### 2.5 — Golden File

- [ ] Create `tests/goldens/configure-ingestion-pipeline/ingestion_config_result.json`

```json
{
  "source_table": "ECOMM.DBO.PARTNER_COMMISSION",
  "staging_table": "DW.STAGING.STG_PARTNER_COMMISSION",
  "database_class": "sql_server",
  "column_count": 10,
  "has_pii": false,
  "cron_schedule": "0 * * * *",
  "sla_tier": "tier_2",
  "incremental_strategy": "UPSERT",
  "connectivity_ok": true,
  "create_table_has_transient": true,
  "create_table_has_loaded_at": true,
  "create_table_has_source": true
}
```

### 2.6 — E2E Test

- [ ] Create `tests/test_e2e_configure_ingestion_pipeline.py`

```python
"""
End-to-end test for the configure-ingestion-pipeline workflow.

Tests the full 7-step workflow with cassette responses, verifying
the engine walks all steps and produces a correct ingestion config.

Test scenario: Configure ingestion for PARTNER_COMMISSION from ECOMM SQL Server.
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
CASSETTE_DIR = Path(__file__).parent / "cassettes" / "configure-ingestion-pipeline"
GOLDEN_DIR = Path(__file__).parent / "goldens" / "configure-ingestion-pipeline"

# All 7 steps in execution order
EXPECTED_STEPS = [
    "discover_source_schema",
    "design_staging_table",
    "generate_ingestion_config",
    "configure_load_frequency",
    "validate_connectivity",
    "show_plan",
    "create_pr",
]

# 5 REASON steps that need cassettes
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("show_plan", "create_pr")]

INPUTS = {"table_name": "PARTNER_COMMISSION", "source_database": "ECOMM"}


def load_cassettes() -> dict[str, dict]:
    """Load all 5 cassettes for the configure-ingestion-pipeline test."""
    cassettes = {}
    for step_id in REASON_STEPS:
        path = CASSETTE_DIR / f"{step_id}.json"
        with open(path) as f:
            cassettes[step_id] = json.load(f)
    return cassettes


def run_workflow(inputs: dict = None) -> tuple:
    """Drive the full workflow with cassette responses."""
    engine = DCAGEngine(content_dir=CONTENT_DIR)
    cassettes = load_cassettes()

    run = engine.start("configure-ingestion-pipeline", inputs or INPUTS)
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
            if request.step_id == "show_plan":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"approved": True, "user_feedback": None}),
                )
            elif request.step_id == "create_pr":
                run.record_result(
                    request.step_id,
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/55"}),
                )

    return run, steps_executed, reason_outputs


class TestConfigureIngestionPipeline:
    """Configure ingestion for PARTNER_COMMISSION from ECOMM."""

    def test_workflow_completes_7_steps(self):
        run, steps_executed, _ = run_workflow()
        assert run.status == "completed"
        assert len(steps_executed) == 7
        assert steps_executed == EXPECTED_STEPS

    def test_source_schema_discovered(self):
        _, _, reason_outputs = run_workflow()
        schema = reason_outputs["discover_source_schema"]
        assert schema["source_table_fqn"] == "ECOMM.DBO.PARTNER_COMMISSION"
        assert len(schema["columns"]) == 10
        assert schema["primary_key"] == ["CommissionId"]
        assert schema["row_count"] == 8500000
        assert schema["update_frequency"] == "HOURLY"

    def test_staging_table_is_transient(self):
        _, _, reason_outputs = run_workflow()
        sql = reason_outputs["design_staging_table"]["create_table_sql"]
        assert "TRANSIENT" in sql

    def test_staging_table_has_metadata_columns(self):
        _, _, reason_outputs = run_workflow()
        sql = reason_outputs["design_staging_table"]["create_table_sql"]
        assert "_LOADED_AT" in sql
        assert "_SOURCE" in sql

    def test_staging_table_fqn_follows_convention(self):
        _, _, reason_outputs = run_workflow()
        fqn = reason_outputs["design_staging_table"]["staging_table_fqn"]
        assert fqn == "DW.STAGING.STG_PARTNER_COMMISSION"

    def test_column_mapping_correct_count(self):
        _, _, reason_outputs = run_workflow()
        mapping = reason_outputs["design_staging_table"]["column_mapping"]
        assert len(mapping) == 10

    def test_type_mapping_applied(self):
        _, _, reason_outputs = run_workflow()
        mapping = reason_outputs["design_staging_table"]["column_mapping"]
        bigint_col = next(m for m in mapping if m["source"] == "CommissionId")
        assert bigint_col["target_type"] == "NUMBER(38,0)"
        datetime_col = next(m for m in mapping if m["source"] == "CreatedDate")
        assert datetime_col["target_type"] == "TIMESTAMP_NTZ"

    def test_snake_case_naming(self):
        _, _, reason_outputs = run_workflow()
        mapping = reason_outputs["design_staging_table"]["column_mapping"]
        camel_col = next(m for m in mapping if m["source"] == "CommissionId")
        assert camel_col["target"] == "COMMISSION_ID"

    def test_ingestion_config_is_sql_server(self):
        _, _, reason_outputs = run_workflow()
        config = reason_outputs["generate_ingestion_config"]
        assert config["database_class"] == "sql_server"
        assert "SqlServerTable(" in config["config_snippet"]

    def test_config_has_incremental_column(self):
        _, _, reason_outputs = run_workflow()
        snippet = reason_outputs["generate_ingestion_config"]["config_snippet"]
        assert "MODIFIED_DATE" in snippet

    def test_load_frequency_is_hourly(self):
        _, _, reason_outputs = run_workflow()
        freq = reason_outputs["configure_load_frequency"]
        assert freq["cron_schedule"] == "0 * * * *"
        assert freq["sla_tier"] == "tier_2"
        assert freq["incremental_strategy"] == "UPSERT"

    def test_connectivity_validated(self):
        _, _, reason_outputs = run_workflow()
        assert reason_outputs["validate_connectivity"]["connectivity_ok"] is True
        assert reason_outputs["validate_connectivity"]["row_count_verified"] is True
        assert reason_outputs["validate_connectivity"]["errors"] == []

    def test_no_pii_detected(self):
        _, _, reason_outputs = run_workflow()
        assert reason_outputs["design_staging_table"]["pii_columns"] == []

    def test_golden_match(self):
        _, _, reason_outputs = run_workflow()
        with open(GOLDEN_DIR / "ingestion_config_result.json") as f:
            golden = json.load(f)
        assert reason_outputs["discover_source_schema"]["source_table_fqn"] == golden["source_table"]
        assert reason_outputs["design_staging_table"]["staging_table_fqn"] == golden["staging_table"]
        assert reason_outputs["generate_ingestion_config"]["database_class"] == golden["database_class"]
        assert reason_outputs["configure_load_frequency"]["cron_schedule"] == golden["cron_schedule"]
        assert reason_outputs["configure_load_frequency"]["incremental_strategy"] == golden["incremental_strategy"]

    def test_trace_has_all_7_steps(self):
        run, _, _ = run_workflow()
        trace = run.get_trace()
        assert trace["workflow_id"] == "configure-ingestion-pipeline"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 7

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("configure-ingestion-pipeline", INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "discover_source_schema"
        assert request.persona.id == "data_engineer"
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_show_plan_requires_approval(self):
        """Verify show_plan is a delegate with approval gate."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes()
        run = engine.start("configure-ingestion-pipeline", INPUTS)

        # Walk through 5 REASON steps
        for step_id in REASON_STEPS:
            request = run.next_step()
            assert request.step_id == step_id
            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        # Step 6 should be show_plan (DelegateRequest)
        request = run.next_step()
        assert isinstance(request, DelegateRequest)
        assert request.step_id == "show_plan"
        assert request.requires_approval is True

    def test_design_staging_has_no_tools(self):
        """Verify design_staging_table is a pure reasoning step with no tools."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes()
        run = engine.start("configure-ingestion-pipeline", INPUTS)

        # Walk to discover_source_schema
        request = run.next_step()
        run.record_result(request.step_id, StepSuccess(output=cassettes[request.step_id]["output"]))

        # design_staging_table should have 0 tools
        request = run.next_step()
        assert request.step_id == "design_staging_table"
        assert isinstance(request, ReasonRequest)
        assert len(request.tools) == 0
```

### 2.7 — Conformance Test

- [ ] Create `tests/test_conformance_configure_ingestion_pipeline.py`

```python
"""Conformance tests for configure-ingestion-pipeline workflow.

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


class TestConfigureIngestionConformance:
    """Validate that configure-ingestion-pipeline assembles correct context per step."""

    WORKFLOW_ID = "configure-ingestion-pipeline"
    INPUTS = {"table_name": "PARTNER_COMMISSION", "source_database": "ECOMM"}

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
            "discover_source_schema": {
                "source_table_fqn": "ECOMM.DBO.PARTNER_COMMISSION",
                "columns": [
                    {"name": "CommissionId", "source_type": "BIGINT", "nullable": False},
                    {"name": "PartnerId", "source_type": "INT", "nullable": False},
                ],
                "primary_key": ["CommissionId"],
                "row_count": 8500000,
                "update_frequency": "HOURLY",
            },
            "design_staging_table": {
                "staging_table_fqn": "DW.STAGING.STG_PARTNER_COMMISSION",
                "create_table_sql": "CREATE TRANSIENT TABLE ...",
                "column_mapping": [],
                "pii_columns": [],
            },
            "generate_ingestion_config": {
                "config_snippet": "SqlServerTable(...)",
                "config_file_path": "dags/configs/sql_server_tables.py",
                "database_class": "sql_server",
            },
            "configure_load_frequency": {
                "cron_schedule": "0 * * * *",
                "sla_tier": "tier_2",
                "rationale": "Hourly updates",
                "incremental_strategy": "UPSERT",
            },
            "validate_connectivity": {
                "connectivity_ok": True,
                "row_count_verified": True,
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

### 2.8 — Manifest Entry

- [ ] Update `content/workflows/manifest.yml` — add configure-ingestion-pipeline entry

```yaml
  - id: configure-ingestion-pipeline
    name: Configure Data Ingestion Pipeline
    persona: data_engineer
    triggers:
      keywords: [add ingestion, configure pipeline, new data source, ingest table, add source ingestion]
      input_pattern: "ingest {table} from {source}"
```

### 2.9 — Commit

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
git add \
  content/knowledge/database_classes.yml \
  content/knowledge/sla_contracts.yml \
  content/workflows/configure-ingestion-pipeline.yml \
  content/workflows/configure-ingestion-pipeline.test.yml \
  content/workflows/manifest.yml \
  tests/cassettes/configure-ingestion-pipeline/discover_source_schema.json \
  tests/cassettes/configure-ingestion-pipeline/design_staging_table.json \
  tests/cassettes/configure-ingestion-pipeline/generate_ingestion_config.json \
  tests/cassettes/configure-ingestion-pipeline/configure_load_frequency.json \
  tests/cassettes/configure-ingestion-pipeline/validate_connectivity.json \
  tests/goldens/configure-ingestion-pipeline/ingestion_config_result.json \
  tests/test_e2e_configure_ingestion_pipeline.py \
  tests/test_conformance_configure_ingestion_pipeline.py

git commit -m "feat: add configure-ingestion-pipeline workflow with schema cache

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## File Summary

### Task 1: thread-field-through-pipeline (14 files)

| File | Type |
|------|------|
| `content/knowledge/pipeline_threading_conventions.yml` | Knowledge |
| `content/knowledge/testing_standards.yml` | Knowledge |
| `content/workflows/thread-field-through-pipeline.yml` | Workflow |
| `content/workflows/thread-field-through-pipeline.test.yml` | Conformance spec |
| `content/workflows/manifest.yml` | Manifest (update) |
| `tests/cassettes/thread-field-through-pipeline/resolve_source_column.json` | Cassette |
| `tests/cassettes/thread-field-through-pipeline/trace_pipeline_lineage.json` | Cassette |
| `tests/cassettes/thread-field-through-pipeline/modify_each_model.json` | Cassette (loop: 3 outputs) |
| `tests/cassettes/thread-field-through-pipeline/update_each_schema.json` | Cassette (loop: 3 outputs) |
| `tests/cassettes/thread-field-through-pipeline/validate_pipeline.json` | Cassette |
| `tests/goldens/thread-field-through-pipeline/pipeline_threading_result.json` | Golden |
| `tests/test_e2e_thread_field_through_pipeline.py` | E2E test |
| `tests/test_conformance_thread_field_through_pipeline.py` | Conformance test |

### Task 2: configure-ingestion-pipeline (13 files)

| File | Type |
|------|------|
| `content/knowledge/database_classes.yml` | Knowledge |
| `content/knowledge/sla_contracts.yml` | Knowledge |
| `content/workflows/configure-ingestion-pipeline.yml` | Workflow |
| `content/workflows/configure-ingestion-pipeline.test.yml` | Conformance spec |
| `content/workflows/manifest.yml` | Manifest (update) |
| `tests/cassettes/configure-ingestion-pipeline/discover_source_schema.json` | Cassette |
| `tests/cassettes/configure-ingestion-pipeline/design_staging_table.json` | Cassette |
| `tests/cassettes/configure-ingestion-pipeline/generate_ingestion_config.json` | Cassette |
| `tests/cassettes/configure-ingestion-pipeline/configure_load_frequency.json` | Cassette |
| `tests/cassettes/configure-ingestion-pipeline/validate_connectivity.json` | Cassette |
| `tests/goldens/configure-ingestion-pipeline/ingestion_config_result.json` | Golden |
| `tests/test_e2e_configure_ingestion_pipeline.py` | E2E test |
| `tests/test_conformance_configure_ingestion_pipeline.py` | Conformance test |

### Engine Prerequisites (from Part 1)

- **Step Loops**: Required by thread-field-through-pipeline (modify_each_model, update_each_schema)
- **Schema Cache**: Required by configure-ingestion-pipeline (discover_source_schema caches, design_staging_table and generate_ingestion_config consume)
- **Conditional Walker**: Required by step loops (loop termination)

### Manifest Final State (after both tasks)

```yaml
workflows:
  - id: add-column-to-model
    name: Add Column to Existing dbt Model
    persona: analytics_engineer
    triggers:
      keywords: [add column, new column, include field, add field]
      input_pattern: "{column} to {model}"

  - id: table-optimizer
    name: Optimize Snowflake Table Performance
    persona: data_engineer
    triggers:
      keywords: [optimize table, clustering, table performance, slow queries, partition]
      input_pattern: "optimize {table}"

  - id: thread-field-through-pipeline
    name: Thread Field Through dbt Pipeline
    persona: analytics_engineer
    triggers:
      keywords: [thread column, add field through pipeline, propagate column, thread field]
      input_pattern: "{column} through {model} pipeline"

  - id: configure-ingestion-pipeline
    name: Configure Data Ingestion Pipeline
    persona: data_engineer
    triggers:
      keywords: [add ingestion, configure pipeline, new data source, ingest table, add source ingestion]
      input_pattern: "ingest {table} from {source}"
```
