# DCAG v2 Implementation Plan — Part 2: Basic Workflows

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox syntax for tracking.

**Goal:** Build 2 workflows that work with today's DCAG engine (no engine changes needed): generate-schema-yml and add-dbt-tests.

**Architecture:** Each workflow follows the existing pattern: YAML definition + conformance test spec + cassette-driven E2E tests. Both use the analytics_engineer persona. Both are linear (no branching).

**Tech Stack:** Python 3.14, dataclasses, PyYAML, pytest

**Spec:** `docs/superpowers/specs/2026-03-13-dcag-v2-engine-and-workflows-design.md`

---

## Task 1: Workflow — generate-schema-yml

### 1.1 — Workflow Definition

- [ ] Create `content/workflows/generate-schema-yml.yml`

```yaml
workflow:
  id: generate-schema-yml
  name: Generate schema.yml for dbt Model
  persona: analytics_engineer

  inputs:
    model_name:
      type: string
      required: true
    source_table:
      type: string
      required: false

  steps:
    # Step 0: Resolve Model — find model SQL file, get path
    - id: resolve_model
      mode: reason
      instruction: |
        Find the dbt model file for the given model name.
        Use dbt_mcp.get_node_details_dev to locate the model, then search for the
        SQL file path via GitHub CLI. Also resolve the source table FQN — either
        from the input or by reading sources.yml to get database/schema and verifying
        against Snowflake INFORMATION_SCHEMA.
        Output the model_path, source_table_fqn, and the existing schema.yml path
        (if one exists already).
      tools:
        - name: dbt_mcp.get_node_details_dev
          instruction: "Look up model metadata in dbt project"
        - name: github_cli.search_code
          instruction: "Search for model SQL file in the Astronomer repo"
        - name: snowflake_mcp.execute_query
          instruction: "Verify source table exists in INFORMATION_SCHEMA"
      context:
        static: [dbt_project_structure]
        dynamic: []
      output_schema:
        type: object
        required: [model_path, source_table_fqn, existing_schema_yml_path]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 1: Parse Columns — extract SELECT columns from model SQL
    - id: parse_columns
      mode: reason
      instruction: |
        Read the model SQL file and parse the SELECT columns.
        Extract column names, any aliases, and source references (ref/source).
        Identify the final SELECT statement (after CTEs) to get the output columns.
        For each column, note whether it is a simple passthrough, renamed, or computed.
      tools:
        - name: github_cli.read_file
          instruction: "Read the model SQL file to extract column definitions"
      context:
        static: []
        dynamic:
          - from: resolve_model
            select: model_path
      output_schema:
        type: object
        required: [columns, cte_count, has_star_select]
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 2: Describe Columns — get types, nullability, samples from Snowflake
    - id: describe_columns
      mode: reason
      instruction: |
        Query Snowflake for detailed column metadata: data types, nullability,
        and sample values. Use INFORMATION_SCHEMA.COLUMNS for type info and
        run sample queries for value distribution.
        Map Snowflake types to human-readable descriptions.
        Flag any columns that are always NULL or have suspicious patterns.
      tools:
        - name: snowflake_mcp.describe_table
          instruction: "Get column types and nullability from INFORMATION_SCHEMA"
        - name: snowflake_mcp.execute_query
          instruction: "Sample values and check null rates per column"
      context:
        static: [sf_type_mapping, naming_conventions]
        dynamic:
          - from: parse_columns
            select: columns
          - from: resolve_model
            select: source_table_fqn
      output_schema:
        type: object
        required: [column_metadata]
        properties:
          column_metadata:
            type: array
            items:
              type: object
              required: [name, sf_type, nullable, null_pct, sample_values]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 3: Generate YML — produce schema.yml content
    - id: generate_yml
      mode: reason
      instruction: |
        Generate the complete schema.yml content for this model.
        Include:
        1. Model-level description
        2. Column entries with:
           - name (snake_case)
           - description (clear, business-friendly)
           - tests: not_null for non-nullable columns, unique for PK columns,
             accepted_values where appropriate
        3. Follow existing schema.yml conventions in the project
        Output the full YAML content as a string, plus a summary of tests added.
      tools: []
      context:
        static: [naming_conventions]
        dynamic:
          - from: describe_columns
            select: column_metadata
          - from: parse_columns
            select: columns
          - from: resolve_model
            select: [model_path, existing_schema_yml_path]
      anti_patterns:
        - "Don't skip descriptions — every column must have one"
        - "Don't use generic descriptions like 'The X column' — be specific"
        - "Don't forget tests for primary key columns"
      quality_criteria:
        - "Every column has a non-empty description"
        - "PK columns have not_null + unique tests"
        - "YAML is valid and properly indented"
      output_schema:
        type: object
        required: [schema_yml_content, tests_added, column_count]
      budget:
        max_llm_turns: 3
        max_tokens: 15000

    # Step 4: Validate — dbt parse to verify YAML is correct
    - id: validate
      mode: reason
      instruction: |
        Validate the generated schema.yml by running dbt parse and dbt compile.
        Check that:
        1. The YAML parses without errors
        2. All column names match the model's actual output columns
        3. Test definitions are syntactically valid
        If validation fails, report the specific errors for correction.
      tools:
        - name: dbt_mcp.parse
          instruction: "Parse the dbt project with the new schema.yml"
        - name: dbt_mcp.compile
          instruction: "Compile the model to verify schema references"
      context:
        static: []
        dynamic:
          - from: generate_yml
            select: schema_yml_content
      output_schema:
        type: object
        required: [parse_ok, compile_ok, errors]
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 5: Create PR — DELEGATE with approval gate
    - id: create_pr
      mode: execute
      type: delegate
      delegate: shift.create_pr
      requires_approval: true
      context:
        dynamic:
          - from: generate_yml
            select: [schema_yml_content, tests_added, column_count]
          - from: resolve_model
            select: [model_path, existing_schema_yml_path]
```

### 1.2 — Conformance Test Spec

- [ ] Create `content/workflows/generate-schema-yml.test.yml`

```yaml
conformance:
  workflow_id: generate-schema-yml

  steps:
    resolve_model:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - dbt_project_structure
      tools_include:
        - dbt_mcp.get_node_details_dev
        - github_cli.search_code
        - snowflake_mcp.execute_query
      has_instruction: true

    parse_columns:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - github_cli.read_file
      dynamic_refs_from:
        - resolve_model
      has_instruction: true

    describe_columns:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - sf_type_mapping
        - naming_conventions
      tools_include:
        - snowflake_mcp.describe_table
        - snowflake_mcp.execute_query
      dynamic_refs_from:
        - parse_columns
        - resolve_model
      has_instruction: true

    generate_yml:
      type: ReasonRequest
      persona: analytics_engineer
      tools_count: 0
      knowledge_includes:
        - naming_conventions
      dynamic_refs_from:
        - describe_columns
        - parse_columns
        - resolve_model
      has_instruction: true

    validate:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - dbt_mcp.parse
        - dbt_mcp.compile
      dynamic_refs_from:
        - generate_yml
      has_instruction: true

    create_pr:
      type: DelegateRequest
      requires_approval: true
```

### 1.3 — Cassette Files

- [ ] Create `tests/cassettes/generate-schema-yml/resolve_model.json`

```json
{
  "output": {
    "model_path": "models/marts/core/fct_ticket_sales.sql",
    "source_table_fqn": "DW.RPT.TICKET_SALES",
    "existing_schema_yml_path": null
  },
  "decision_log": {
    "decision": "Found model fct_ticket_sales in marts/core directory",
    "rationale": "dbt_mcp.get_node_details_dev returned the model node. Source table resolved from sources.yml: DW.RPT.TICKET_SALES. No existing schema.yml found for this model.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.get_node_details_dev", "INFORMATION_SCHEMA.TABLES"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.get_node_details_dev", "input": "fct_ticket_sales"},
    {"tool": "github_cli.search_code", "input": "filename:fct_ticket_sales.sql"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM DW.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TICKET_SALES'"}
  ],
  "token_usage": {"prompt": 1100, "completion": 350}
}
```

- [ ] Create `tests/cassettes/generate-schema-yml/parse_columns.json`

```json
{
  "output": {
    "columns": [
      {"name": "TICKET_SALE_ID", "type": "passthrough", "source_column": "TICKET_SALE_ID"},
      {"name": "EVENT_DATE", "type": "passthrough", "source_column": "EVENT_DATE"},
      {"name": "VENUE_ID", "type": "passthrough", "source_column": "VENUE_ID"},
      {"name": "BUYER_ID", "type": "passthrough", "source_column": "BUYER_ID"},
      {"name": "SECTION_NAME", "type": "passthrough", "source_column": "SECTION_NAME"},
      {"name": "QUANTITY", "type": "passthrough", "source_column": "QUANTITY"},
      {"name": "SALE_PRICE", "type": "passthrough", "source_column": "SALE_PRICE"},
      {"name": "CURRENCY_CODE", "type": "passthrough", "source_column": "CURRENCY_CODE"},
      {"name": "CREATED_AT", "type": "passthrough", "source_column": "CREATED_AT"},
      {"name": "UPDATED_AT", "type": "passthrough", "source_column": "UPDATED_AT"}
    ],
    "cte_count": 1,
    "has_star_select": false
  },
  "decision_log": {
    "decision": "Parsed 10 columns from final SELECT in fct_ticket_sales.sql",
    "rationale": "Model has 1 CTE (source) and a final SELECT with 10 explicitly listed columns. No SELECT * detected.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["github_cli.read_file"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/marts/core/fct_ticket_sales.sql"}
  ],
  "token_usage": {"prompt": 900, "completion": 400}
}
```

- [ ] Create `tests/cassettes/generate-schema-yml/describe_columns.json`

```json
{
  "output": {
    "column_metadata": [
      {"name": "TICKET_SALE_ID", "sf_type": "NUMBER(38,0)", "nullable": false, "null_pct": 0.0, "sample_values": [100001, 100002, 100003], "is_pk": true},
      {"name": "EVENT_DATE", "sf_type": "DATE", "nullable": false, "null_pct": 0.0, "sample_values": ["2025-01-15", "2025-02-20", "2025-03-10"], "is_pk": false},
      {"name": "VENUE_ID", "sf_type": "NUMBER(38,0)", "nullable": false, "null_pct": 0.0, "sample_values": [501, 502, 503], "is_pk": false},
      {"name": "BUYER_ID", "sf_type": "NUMBER(38,0)", "nullable": false, "null_pct": 0.0, "sample_values": [10001, 10002, 10003], "is_pk": false},
      {"name": "SECTION_NAME", "sf_type": "VARCHAR(256)", "nullable": true, "null_pct": 2.1, "sample_values": ["Orchestra", "Mezzanine", "Balcony"], "is_pk": false},
      {"name": "QUANTITY", "sf_type": "NUMBER(10,0)", "nullable": false, "null_pct": 0.0, "sample_values": [1, 2, 4], "is_pk": false},
      {"name": "SALE_PRICE", "sf_type": "NUMBER(18,2)", "nullable": false, "null_pct": 0.0, "sample_values": [75.00, 150.50, 299.99], "is_pk": false},
      {"name": "CURRENCY_CODE", "sf_type": "VARCHAR(3)", "nullable": false, "null_pct": 0.0, "sample_values": ["USD", "GBP", "EUR"], "is_pk": false},
      {"name": "CREATED_AT", "sf_type": "TIMESTAMP_NTZ", "nullable": false, "null_pct": 0.0, "sample_values": ["2025-01-15T10:30:00", "2025-02-20T14:15:00"], "is_pk": false},
      {"name": "UPDATED_AT", "sf_type": "TIMESTAMP_NTZ", "nullable": true, "null_pct": 5.3, "sample_values": ["2025-01-16T08:00:00", null], "is_pk": false}
    ]
  },
  "decision_log": {
    "decision": "Collected metadata for all 10 columns from Snowflake",
    "rationale": "INFORMATION_SCHEMA.COLUMNS provided types and nullability. Sample queries confirmed value distributions. TICKET_SALE_ID identified as PK (unique, non-null). SECTION_NAME and UPDATED_AT are nullable.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["INFORMATION_SCHEMA.COLUMNS", "sample queries"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.describe_table", "input": "DW.RPT.TICKET_SALES"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT COUNT(*), COUNT(TICKET_SALE_ID), COUNT(DISTINCT TICKET_SALE_ID) FROM DW.RPT.TICKET_SALES"}
  ],
  "token_usage": {"prompt": 1500, "completion": 600}
}
```

- [ ] Create `tests/cassettes/generate-schema-yml/generate_yml.json`

```json
{
  "output": {
    "schema_yml_content": "version: 2\n\nmodels:\n  - name: fct_ticket_sales\n    description: >\n      Fact table capturing individual ticket sale transactions.\n      Each row represents a single ticket sale with pricing,\n      venue, and buyer information.\n    columns:\n      - name: ticket_sale_id\n        description: Unique identifier for each ticket sale transaction.\n        tests:\n          - not_null\n          - unique\n      - name: event_date\n        description: Date of the event for which the ticket was sold.\n        tests:\n          - not_null\n      - name: venue_id\n        description: Foreign key to the venue dimension where the event takes place.\n        tests:\n          - not_null\n          - relationships:\n              to: ref('dim_venue')\n              field: venue_id\n      - name: buyer_id\n        description: Foreign key to the buyer who purchased the ticket.\n        tests:\n          - not_null\n      - name: section_name\n        description: Name of the seating section (e.g., Orchestra, Mezzanine, Balcony).\n      - name: quantity\n        description: Number of tickets in this sale transaction.\n        tests:\n          - not_null\n      - name: sale_price\n        description: Sale price of the ticket in the transaction currency.\n        tests:\n          - not_null\n      - name: currency_code\n        description: ISO 4217 currency code for the sale price (e.g., USD, GBP, EUR).\n        tests:\n          - not_null\n          - accepted_values:\n              values: ['USD', 'GBP', 'EUR', 'CAD', 'AUD']\n      - name: created_at\n        description: Timestamp when the sale record was created.\n        tests:\n          - not_null\n      - name: updated_at\n        description: Timestamp when the sale record was last updated. May be null if never updated.\n",
    "tests_added": [
      {"column": "ticket_sale_id", "tests": ["not_null", "unique"]},
      {"column": "event_date", "tests": ["not_null"]},
      {"column": "venue_id", "tests": ["not_null", "relationships"]},
      {"column": "buyer_id", "tests": ["not_null"]},
      {"column": "quantity", "tests": ["not_null"]},
      {"column": "sale_price", "tests": ["not_null"]},
      {"column": "currency_code", "tests": ["not_null", "accepted_values"]},
      {"column": "created_at", "tests": ["not_null"]}
    ],
    "column_count": 10
  },
  "decision_log": {
    "decision": "Generated schema.yml with 10 columns, 12 tests total",
    "rationale": "Applied testing standards: not_null on all non-nullable columns, unique on PK (ticket_sale_id), relationships on FK (venue_id), accepted_values on currency_code. Nullable columns (section_name, updated_at) have no not_null test.",
    "alternatives_considered": [
      {"option": "Add not_null to section_name", "reason": "2.1% null rate suggests intentional — skip"}
    ],
    "confidence": "high",
    "informed_by": ["column_metadata", "naming_conventions"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 2000, "completion": 900}
}
```

- [ ] Create `tests/cassettes/generate-schema-yml/validate.json`

```json
{
  "output": {
    "parse_ok": true,
    "compile_ok": true,
    "errors": []
  },
  "decision_log": {
    "decision": "Validation passed — schema.yml is valid",
    "rationale": "dbt parse completed without errors. dbt compile confirmed all column references are valid. Test definitions are syntactically correct.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.parse", "dbt_mcp.compile"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.parse", "input": "dbt parse"},
    {"tool": "dbt_mcp.compile", "input": "dbt compile --select fct_ticket_sales"}
  ],
  "token_usage": {"prompt": 800, "completion": 200}
}
```

### 1.4 — Golden File

- [ ] Create `tests/goldens/generate-schema-yml/schema_yml_output.json`

```json
{
  "model_name": "fct_ticket_sales",
  "column_count": 10,
  "tests_added_count": 12,
  "pk_column": "ticket_sale_id",
  "pk_tests": ["not_null", "unique"],
  "has_relationships_test": true,
  "has_accepted_values_test": true,
  "parse_ok": true,
  "compile_ok": true
}
```

### 1.5 — E2E Test

- [ ] Create `tests/test_e2e_generate_schema_yml.py`

```python
"""
End-to-end test for the generate-schema-yml workflow.

Tests the full 6-step workflow with cassette responses, verifying
the engine walks all steps and produces a correct schema.yml.
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

# All 6 steps in execution order
EXPECTED_STEPS = [
    "resolve_model",
    "parse_columns",
    "describe_columns",
    "generate_yml",
    "validate",
    "create_pr",
]

# 5 REASON steps that need cassettes
REASON_STEPS = [s for s in EXPECTED_STEPS if s not in ("create_pr",)]


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 5 cassettes for the generate-schema-yml test."""
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

    run = engine.start("generate-schema-yml", inputs)
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
                    StepSuccess(output={"pr_url": "https://github.com/stubhub/astronomer-core-data/pull/55"}),
                )

    return run, steps_executed, reason_outputs


class TestGenerateSchemaYml:
    """Generate schema.yml for fct_ticket_sales model."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "generate-schema-yml"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "generate-schema-yml"
    INPUTS = {"model_name": "fct_ticket_sales"}

    def test_workflow_completes_6_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 6
        assert steps_executed == EXPECTED_STEPS

    def test_model_path_resolved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["resolve_model"]["model_path"] == "models/marts/core/fct_ticket_sales.sql"

    def test_source_table_fqn_resolved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["resolve_model"]["source_table_fqn"] == "DW.RPT.TICKET_SALES"

    def test_columns_parsed(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        columns = reason_outputs["parse_columns"]["columns"]
        assert len(columns) == 10
        assert columns[0]["name"] == "TICKET_SALE_ID"
        assert reason_outputs["parse_columns"]["has_star_select"] is False

    def test_column_metadata_collected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        metadata = reason_outputs["describe_columns"]["column_metadata"]
        assert len(metadata) == 10
        pk = metadata[0]
        assert pk["name"] == "TICKET_SALE_ID"
        assert pk["nullable"] is False
        assert pk["is_pk"] is True

    def test_schema_yml_generated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["generate_yml"]
        assert output["column_count"] == 10
        assert "fct_ticket_sales" in output["schema_yml_content"]
        assert "ticket_sale_id" in output["schema_yml_content"]

    def test_pk_has_not_null_and_unique(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_yml"]["tests_added"]
        pk_tests = next(t for t in tests_added if t["column"] == "ticket_sale_id")
        assert "not_null" in pk_tests["tests"]
        assert "unique" in pk_tests["tests"]

    def test_nullable_columns_skip_not_null(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        tests_added = reason_outputs["generate_yml"]["tests_added"]
        test_columns = [t["column"] for t in tests_added]
        # section_name and updated_at are nullable — should not have not_null
        assert "section_name" not in test_columns
        assert "updated_at" not in test_columns

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["validate"]["parse_ok"] is True
        assert reason_outputs["validate"]["compile_ok"] is True
        assert reason_outputs["validate"]["errors"] == []

    def test_report_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        with open(self.GOLDEN_DIR / "schema_yml_output.json") as f:
            golden = json.load(f)
        output = reason_outputs["generate_yml"]
        assert output["column_count"] == golden["column_count"]
        pk_tests = next(t for t in output["tests_added"] if t["column"] == golden["pk_column"])
        assert set(pk_tests["tests"]) == set(golden["pk_tests"])

    def test_trace_has_all_6_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "generate-schema-yml"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 6

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("generate-schema-yml", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_model"
        assert request.persona.id == "analytics_engineer"
        assert len(request.persona.heuristics) > 0
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_delegate_create_pr_has_approval(self):
        """Verify create_pr is a delegate with approval gate."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes(self.CASSETTE_DIR)
        run = engine.start("generate-schema-yml", self.INPUTS)

        # Walk through REASON steps until we hit create_pr
        for step_id in EXPECTED_STEPS[:5]:
            request = run.next_step()
            assert request.step_id == step_id
            if isinstance(request, ReasonRequest):
                run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        # Step 5 should be create_pr (DelegateRequest)
        request = run.next_step()
        assert isinstance(request, DelegateRequest)
        assert request.step_id == "create_pr"
        assert request.requires_approval is True
```

### 1.6 — Conformance Test

- [ ] Create `tests/test_conformance_generate_schema_yml.py`

```python
"""Conformance tests for generate-schema-yml workflow.

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


class TestGenerateSchemaYmlConformance:
    """Validate that generate-schema-yml assembles correct context per step."""

    WORKFLOW_ID = "generate-schema-yml"
    INPUTS = {"model_name": "fct_ticket_sales"}

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
                step_outputs = {
                    "resolve_model": {
                        "model_path": "models/marts/core/fct_ticket_sales.sql",
                        "source_table_fqn": "DW.RPT.TICKET_SALES",
                        "existing_schema_yml_path": None,
                    },
                    "parse_columns": {
                        "columns": [
                            {"name": "TICKET_SALE_ID", "type": "passthrough", "source_column": "TICKET_SALE_ID"}
                        ],
                        "cte_count": 1,
                        "has_star_select": False,
                    },
                    "describe_columns": {
                        "column_metadata": [
                            {"name": "TICKET_SALE_ID", "sf_type": "NUMBER(38,0)", "nullable": False, "null_pct": 0.0, "sample_values": [100001], "is_pk": True}
                        ],
                    },
                    "generate_yml": {
                        "schema_yml_content": "version: 2\nmodels:\n  - name: fct_ticket_sales\n",
                        "tests_added": [{"column": "ticket_sale_id", "tests": ["not_null", "unique"]}],
                        "column_count": 10,
                    },
                    "validate": {
                        "parse_ok": True,
                        "compile_ok": True,
                        "errors": [],
                    },
                }
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

### 1.7 — Manifest Update

- [ ] Add entry to `content/workflows/manifest.yml`

Add this entry after the existing `table-optimizer` entry:

```yaml
  - id: generate-schema-yml
    name: Generate schema.yml for dbt Model
    persona: analytics_engineer
    triggers:
      keywords: [generate schema, add documentation, document model, schema yml]
      input_pattern: "generate schema for {model}"
```

### 1.8 — Verify & Commit

- [ ] Run tests:
```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_conformance_generate_schema_yml.py tests/test_e2e_generate_schema_yml.py -v
```

- [ ] Commit:
```bash
git add \
  content/workflows/generate-schema-yml.yml \
  content/workflows/generate-schema-yml.test.yml \
  content/workflows/manifest.yml \
  tests/cassettes/generate-schema-yml/ \
  tests/goldens/generate-schema-yml/ \
  tests/test_e2e_generate_schema_yml.py \
  tests/test_conformance_generate_schema_yml.py

git commit -m "$(cat <<'EOF'
feat: add generate-schema-yml workflow (6 steps)

Workflow generates schema.yml for dbt models with column descriptions
and appropriate tests (not_null, unique, relationships, accepted_values).
Includes conformance spec, cassette-driven E2E tests, and golden file.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Workflow — add-dbt-tests

### 2.1 — Workflow Definition

- [ ] Create `content/workflows/add-dbt-tests.yml`

```yaml
workflow:
  id: add-dbt-tests
  name: Add dbt Tests to Existing Model
  persona: analytics_engineer

  inputs:
    model_name:
      type: string
      required: true

  steps:
    # Step 0: Resolve Model — find model and read existing schema.yml
    - id: resolve_model
      mode: reason
      instruction: |
        Find the dbt model and its existing schema.yml file.
        Use dbt_mcp.get_node_details_dev to locate the model, then read the
        existing schema.yml via GitHub CLI. Extract the list of columns that
        already have tests defined, and identify columns with no tests.
        Also resolve the source table FQN for Snowflake metadata queries.
      tools:
        - name: dbt_mcp.get_node_details_dev
          instruction: "Look up model metadata in dbt project"
        - name: github_cli.read_file
          instruction: "Read the existing schema.yml for this model"
        - name: snowflake_mcp.execute_query
          instruction: "Verify source table exists in INFORMATION_SCHEMA"
      context:
        static: [dbt_project_structure]
        dynamic: []
      output_schema:
        type: object
        required: [model_path, schema_yml_path, source_table_fqn, existing_tests, columns_without_tests]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 1: Get Column Metadata — types, cardinality, null rates
    - id: get_column_metadata
      mode: reason
      instruction: |
        Get detailed column metadata from Snowflake for test inference.
        For each column, collect:
        - Data type
        - Nullability
        - Null rate (% of rows that are NULL)
        - Cardinality (COUNT DISTINCT)
        - Min/max values for numeric and date columns
        This metadata drives automatic test inference in the next step.
      tools:
        - name: snowflake_mcp.describe_table
          instruction: "Get column types and nullability from INFORMATION_SCHEMA"
        - name: snowflake_mcp.execute_query
          instruction: "Query cardinality, null rates, and value ranges per column"
          usage_pattern: |
            1. SELECT column_name, COUNT(*), COUNT(column_name), COUNT(DISTINCT column_name) GROUP BY 1
            2. For date/numeric columns: SELECT MIN(col), MAX(col)
      context:
        static: []
        dynamic:
          - from: resolve_model
            select: [source_table_fqn, existing_tests]
      output_schema:
        type: object
        required: [column_metadata]
        properties:
          column_metadata:
            type: array
            items:
              type: object
              required: [name, sf_type, nullable, null_pct, cardinality, row_count]
      budget:
        max_llm_turns: 5
        max_tokens: 10000

    # Step 2: Infer Tests — determine which tests to add
    - id: infer_tests
      mode: reason
      instruction: |
        Determine which dbt tests to add based on column patterns and metadata.
        Apply these rules:
        - Primary key columns (unique + non-null + high cardinality): not_null + unique
        - Foreign key columns (name ends with _id, not PK): not_null + relationships
        - Non-nullable columns without not_null test: add not_null
        - Low-cardinality columns (< 20 distinct values): accepted_values
        - Date columns: not_null (if non-nullable) + recency test if applicable
        - Boolean columns: accepted_values with [true, false]
        Skip columns that already have the inferred test type.
        Output the list of new tests to add, grouped by column.
      tools: []
      context:
        static: [naming_conventions]
        dynamic:
          - from: get_column_metadata
            select: column_metadata
          - from: resolve_model
            select: existing_tests
      heuristics:
        - "If column is named *_id and cardinality equals row count, it is likely a PK"
        - "If column is named *_id and cardinality is much less than row count, it is a FK"
        - "Low cardinality + VARCHAR = good accepted_values candidate"
        - "Always add not_null to non-nullable columns that lack it"
      anti_patterns:
        - "Don't add accepted_values to high-cardinality columns"
        - "Don't add unique test to FK columns"
        - "Don't duplicate tests that already exist"
      output_schema:
        type: object
        required: [new_tests, total_tests_to_add, columns_affected]
        properties:
          new_tests:
            type: array
            items:
              type: object
              required: [column, test_type, rationale]
      quality_criteria:
        - "Every non-nullable column has not_null test"
        - "PK column has unique test"
        - "No duplicate tests"
      budget:
        max_llm_turns: 3
        max_tokens: 8000

    # Step 3: Update Schema YML — generate updated YAML with new tests
    - id: update_schema_yml
      mode: reason
      instruction: |
        Read the existing schema.yml and produce the updated version with
        new tests added. Preserve all existing content (descriptions, existing
        tests). Add the inferred tests to the appropriate column entries.
        If a column entry doesn't exist yet, create it with the test.
        Output the complete modified schema.yml content.
      tools:
        - name: github_cli.read_file
          instruction: "Read the current schema.yml to preserve existing content"
      context:
        static: []
        dynamic:
          - from: infer_tests
            select: new_tests
          - from: resolve_model
            select: schema_yml_path
      anti_patterns:
        - "Don't remove existing tests or descriptions"
        - "Don't reorder existing columns — add new entries at the end"
        - "Don't change indentation style of existing content"
      output_schema:
        type: object
        required: [modified_yml, tests_added_count, columns_modified]
      quality_criteria:
        - "Existing tests preserved"
        - "New tests properly indented"
        - "YAML is valid"
      budget:
        max_llm_turns: 3
        max_tokens: 15000

    # Step 4: Validate — run dbt test to verify
    - id: validate
      mode: reason
      instruction: |
        Validate the updated schema.yml by running dbt parse and dbt test.
        Check that:
        1. The YAML parses without errors
        2. All new tests pass against the actual data
        If any tests fail, report which ones and why (e.g., accepted_values
        has unexpected values, unique test finds duplicates).
      tools:
        - name: dbt_mcp.test
          instruction: "Run dbt test --select {model_name} to validate all tests"
        - name: dbt_mcp.parse
          instruction: "Parse the dbt project with updated schema.yml"
      context:
        static: []
        dynamic:
          - from: update_schema_yml
            select: modified_yml
      output_schema:
        type: object
        required: [parse_ok, tests_passed, tests_failed, errors]
      budget:
        max_llm_turns: 3
        max_tokens: 8000
```

### 2.2 — Conformance Test Spec

- [ ] Create `content/workflows/add-dbt-tests.test.yml`

```yaml
conformance:
  workflow_id: add-dbt-tests

  steps:
    resolve_model:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - dbt_project_structure
      tools_include:
        - dbt_mcp.get_node_details_dev
        - github_cli.read_file
        - snowflake_mcp.execute_query
      has_instruction: true

    get_column_metadata:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - snowflake_mcp.describe_table
        - snowflake_mcp.execute_query
      dynamic_refs_from:
        - resolve_model
      has_instruction: true

    infer_tests:
      type: ReasonRequest
      persona: analytics_engineer
      tools_count: 0
      knowledge_includes:
        - naming_conventions
      dynamic_refs_from:
        - get_column_metadata
        - resolve_model
      has_instruction: true

    update_schema_yml:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - github_cli.read_file
      dynamic_refs_from:
        - infer_tests
        - resolve_model
      has_instruction: true

    validate:
      type: ReasonRequest
      persona: analytics_engineer
      tools_include:
        - dbt_mcp.test
        - dbt_mcp.parse
      dynamic_refs_from:
        - update_schema_yml
      has_instruction: true
```

### 2.3 — Cassette Files

- [ ] Create `tests/cassettes/add-dbt-tests/resolve_model.json`

```json
{
  "output": {
    "model_path": "models/marts/core/dim_venue.sql",
    "schema_yml_path": "models/marts/core/schema.yml",
    "source_table_fqn": "DW.CORE.VENUE_DIM",
    "existing_tests": [
      {"column": "venue_id", "tests": ["not_null", "unique"]},
      {"column": "venue_name", "tests": ["not_null"]}
    ],
    "columns_without_tests": ["city", "state", "country", "capacity", "venue_type", "created_at", "updated_at", "is_active"]
  },
  "decision_log": {
    "decision": "Found dim_venue model with partial test coverage",
    "rationale": "Model has 10 columns but only 2 (venue_id, venue_name) have tests defined. 8 columns have no tests. schema.yml exists at models/marts/core/schema.yml.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.get_node_details_dev", "github_cli.read_file"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.get_node_details_dev", "input": "dim_venue"},
    {"tool": "github_cli.read_file", "input": "models/marts/core/schema.yml"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT * FROM DW.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'VENUE_DIM'"}
  ],
  "token_usage": {"prompt": 1200, "completion": 400}
}
```

- [ ] Create `tests/cassettes/add-dbt-tests/get_column_metadata.json`

```json
{
  "output": {
    "column_metadata": [
      {"name": "VENUE_ID", "sf_type": "NUMBER(38,0)", "nullable": false, "null_pct": 0.0, "cardinality": 15000, "row_count": 15000},
      {"name": "VENUE_NAME", "sf_type": "VARCHAR(512)", "nullable": false, "null_pct": 0.0, "cardinality": 14850, "row_count": 15000},
      {"name": "CITY", "sf_type": "VARCHAR(256)", "nullable": false, "null_pct": 0.0, "cardinality": 3200, "row_count": 15000},
      {"name": "STATE", "sf_type": "VARCHAR(128)", "nullable": true, "null_pct": 8.5, "cardinality": 52, "row_count": 15000},
      {"name": "COUNTRY", "sf_type": "VARCHAR(64)", "nullable": false, "null_pct": 0.0, "cardinality": 12, "row_count": 15000},
      {"name": "CAPACITY", "sf_type": "NUMBER(10,0)", "nullable": true, "null_pct": 15.2, "cardinality": 450, "row_count": 15000},
      {"name": "VENUE_TYPE", "sf_type": "VARCHAR(64)", "nullable": false, "null_pct": 0.0, "cardinality": 8, "row_count": 15000},
      {"name": "CREATED_AT", "sf_type": "TIMESTAMP_NTZ", "nullable": false, "null_pct": 0.0, "cardinality": 14200, "row_count": 15000},
      {"name": "UPDATED_AT", "sf_type": "TIMESTAMP_NTZ", "nullable": true, "null_pct": 22.0, "cardinality": 8500, "row_count": 15000},
      {"name": "IS_ACTIVE", "sf_type": "BOOLEAN", "nullable": false, "null_pct": 0.0, "cardinality": 2, "row_count": 15000}
    ]
  },
  "decision_log": {
    "decision": "Collected metadata for all 10 columns",
    "rationale": "Queried INFORMATION_SCHEMA.COLUMNS for types and cardinality stats. Notable: COUNTRY has 12 distinct values (good accepted_values candidate), VENUE_TYPE has 8 (also good), IS_ACTIVE is boolean with 2 values.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["INFORMATION_SCHEMA.COLUMNS", "cardinality queries"]
  },
  "tool_calls": [
    {"tool": "snowflake_mcp.describe_table", "input": "DW.CORE.VENUE_DIM"},
    {"tool": "snowflake_mcp.execute_query", "input": "SELECT 'CITY' as col, COUNT(*) as total, COUNT(CITY) as non_null, COUNT(DISTINCT CITY) as distinct_vals FROM DW.CORE.VENUE_DIM UNION ALL SELECT 'STATE', COUNT(*), COUNT(STATE), COUNT(DISTINCT STATE) FROM DW.CORE.VENUE_DIM"}
  ],
  "token_usage": {"prompt": 1400, "completion": 500}
}
```

- [ ] Create `tests/cassettes/add-dbt-tests/infer_tests.json`

```json
{
  "output": {
    "new_tests": [
      {"column": "city", "test_type": "not_null", "rationale": "Non-nullable column (0% nulls) without not_null test"},
      {"column": "country", "test_type": "not_null", "rationale": "Non-nullable column (0% nulls) without not_null test"},
      {"column": "country", "test_type": "accepted_values", "rationale": "Low cardinality (12 distinct values) — good candidate for accepted_values", "config": {"values": ["US", "GB", "CA", "DE", "FR", "AU", "JP", "MX", "BR", "ES", "IT", "NL"]}},
      {"column": "venue_type", "test_type": "not_null", "rationale": "Non-nullable column (0% nulls) without not_null test"},
      {"column": "venue_type", "test_type": "accepted_values", "rationale": "Low cardinality (8 distinct values) — categorical column", "config": {"values": ["Arena", "Stadium", "Theater", "Club", "Amphitheater", "Convention Center", "Festival Grounds", "Other"]}},
      {"column": "created_at", "test_type": "not_null", "rationale": "Non-nullable timestamp column without not_null test"},
      {"column": "is_active", "test_type": "not_null", "rationale": "Non-nullable boolean column without not_null test"},
      {"column": "is_active", "test_type": "accepted_values", "rationale": "Boolean column — should only be true/false", "config": {"values": [true, false]}}
    ],
    "total_tests_to_add": 8,
    "columns_affected": ["city", "country", "venue_type", "created_at", "is_active"]
  },
  "decision_log": {
    "decision": "Inferred 8 new tests across 5 columns",
    "rationale": "Applied rules: not_null for all non-nullable columns without it (city, country, venue_type, created_at, is_active). accepted_values for low-cardinality columns (country=12, venue_type=8, is_active=2). Skipped: state (nullable, 8.5% nulls), capacity (nullable, 15.2% nulls), updated_at (nullable, 22% nulls). Existing tests on venue_id and venue_name preserved.",
    "alternatives_considered": [
      {"option": "Add not_null to state", "reason": "8.5% null rate — intentionally nullable"},
      {"option": "Add accepted_values to city", "reason": "3200 distinct values — too high cardinality"}
    ],
    "confidence": "high",
    "informed_by": ["column_metadata", "existing_tests", "naming_conventions"]
  },
  "tool_calls": [],
  "token_usage": {"prompt": 1800, "completion": 600}
}
```

- [ ] Create `tests/cassettes/add-dbt-tests/update_schema_yml.json`

```json
{
  "output": {
    "modified_yml": "version: 2\n\nmodels:\n  - name: dim_venue\n    description: >\n      Venue dimension table containing details about event venues\n      including location, capacity, and type classification.\n    columns:\n      - name: venue_id\n        description: Unique identifier for each venue.\n        tests:\n          - not_null\n          - unique\n      - name: venue_name\n        description: Display name of the venue.\n        tests:\n          - not_null\n      - name: city\n        description: City where the venue is located.\n        tests:\n          - not_null\n      - name: state\n        description: State or province where the venue is located. May be null for international venues.\n      - name: country\n        description: ISO country code for the venue location.\n        tests:\n          - not_null\n          - accepted_values:\n              values: ['US', 'GB', 'CA', 'DE', 'FR', 'AU', 'JP', 'MX', 'BR', 'ES', 'IT', 'NL']\n      - name: capacity\n        description: Maximum seating capacity of the venue. May be null if unknown.\n      - name: venue_type\n        description: Classification of the venue (e.g., Arena, Stadium, Theater).\n        tests:\n          - not_null\n          - accepted_values:\n              values: ['Arena', 'Stadium', 'Theater', 'Club', 'Amphitheater', 'Convention Center', 'Festival Grounds', 'Other']\n      - name: created_at\n        description: Timestamp when the venue record was created.\n        tests:\n          - not_null\n      - name: updated_at\n        description: Timestamp when the venue record was last updated. Null if never updated.\n      - name: is_active\n        description: Whether the venue is currently active and hosting events.\n        tests:\n          - not_null\n          - accepted_values:\n              values: [true, false]\n",
    "tests_added_count": 8,
    "columns_modified": ["city", "country", "venue_type", "created_at", "is_active"]
  },
  "decision_log": {
    "decision": "Updated schema.yml with 8 new tests, preserved all existing content",
    "rationale": "Added not_null to 5 columns, accepted_values to 3 columns. Existing tests on venue_id (not_null, unique) and venue_name (not_null) preserved. Column order maintained. Added description entries for columns that had none.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["existing schema.yml", "inferred tests"]
  },
  "tool_calls": [
    {"tool": "github_cli.read_file", "input": "models/marts/core/schema.yml"}
  ],
  "token_usage": {"prompt": 2200, "completion": 800}
}
```

- [ ] Create `tests/cassettes/add-dbt-tests/validate.json`

```json
{
  "output": {
    "parse_ok": true,
    "tests_passed": 11,
    "tests_failed": 0,
    "errors": []
  },
  "decision_log": {
    "decision": "All 11 tests pass (3 existing + 8 new)",
    "rationale": "dbt parse completed without errors. dbt test --select dim_venue ran all 11 tests: 3 pre-existing (venue_id not_null, venue_id unique, venue_name not_null) + 8 new. All passed.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["dbt_mcp.parse", "dbt_mcp.test"]
  },
  "tool_calls": [
    {"tool": "dbt_mcp.parse", "input": "dbt parse"},
    {"tool": "dbt_mcp.test", "input": "dbt test --select dim_venue"}
  ],
  "token_usage": {"prompt": 900, "completion": 250}
}
```

### 2.4 — Golden File

- [ ] Create `tests/goldens/add-dbt-tests/test_coverage_output.json`

```json
{
  "model_name": "dim_venue",
  "existing_tests_count": 3,
  "new_tests_count": 8,
  "total_tests_count": 11,
  "columns_with_accepted_values": ["country", "venue_type", "is_active"],
  "columns_with_not_null_added": ["city", "country", "venue_type", "created_at", "is_active"],
  "nullable_columns_skipped": ["state", "capacity", "updated_at"],
  "parse_ok": true,
  "all_tests_passed": true
}
```

### 2.5 — E2E Test

- [ ] Create `tests/test_e2e_add_dbt_tests.py`

```python
"""
End-to-end test for the add-dbt-tests workflow.

Tests the full 5-step workflow with cassette responses, verifying
the engine walks all steps and produces correct test additions.
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

# All 5 steps in execution order
EXPECTED_STEPS = [
    "resolve_model",
    "get_column_metadata",
    "infer_tests",
    "update_schema_yml",
    "validate",
]

# All 5 steps are REASON (no DELEGATE in this workflow)
REASON_STEPS = EXPECTED_STEPS


def load_cassettes(cassette_dir: Path) -> dict[str, dict]:
    """Load all 5 cassettes for the add-dbt-tests test."""
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

    run = engine.start("add-dbt-tests", inputs)
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
            run.record_result(
                request.step_id,
                StepSuccess(output={"approved": True}),
            )

    return run, steps_executed, reason_outputs


class TestAddDbtTests:
    """Add tests to dim_venue model with partial coverage."""

    CASSETTE_DIR = Path(__file__).parent / "cassettes" / "add-dbt-tests"
    GOLDEN_DIR = Path(__file__).parent / "goldens" / "add-dbt-tests"
    INPUTS = {"model_name": "dim_venue"}

    def test_workflow_completes_5_steps(self):
        run, steps_executed, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert run.status == "completed"
        assert len(steps_executed) == 5
        assert steps_executed == EXPECTED_STEPS

    def test_existing_tests_detected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        existing = reason_outputs["resolve_model"]["existing_tests"]
        assert len(existing) == 2
        venue_id_tests = next(t for t in existing if t["column"] == "venue_id")
        assert "not_null" in venue_id_tests["tests"]
        assert "unique" in venue_id_tests["tests"]

    def test_columns_without_tests_identified(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        untested = reason_outputs["resolve_model"]["columns_without_tests"]
        assert len(untested) == 8
        assert "city" in untested
        assert "is_active" in untested

    def test_column_metadata_collected(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        metadata = reason_outputs["get_column_metadata"]["column_metadata"]
        assert len(metadata) == 10
        country = next(c for c in metadata if c["name"] == "COUNTRY")
        assert country["cardinality"] == 12
        assert country["nullable"] is False

    def test_new_tests_inferred(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        new_tests = reason_outputs["infer_tests"]["new_tests"]
        assert reason_outputs["infer_tests"]["total_tests_to_add"] == 8
        # Verify not_null tests
        not_null_cols = [t["column"] for t in new_tests if t["test_type"] == "not_null"]
        assert "city" in not_null_cols
        assert "country" in not_null_cols
        assert "is_active" in not_null_cols

    def test_accepted_values_for_low_cardinality(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        new_tests = reason_outputs["infer_tests"]["new_tests"]
        av_tests = [t for t in new_tests if t["test_type"] == "accepted_values"]
        av_cols = [t["column"] for t in av_tests]
        assert "country" in av_cols
        assert "venue_type" in av_cols
        assert "is_active" in av_cols

    def test_nullable_columns_skipped(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        new_tests = reason_outputs["infer_tests"]["new_tests"]
        tested_cols = [t["column"] for t in new_tests]
        # Nullable columns should NOT get not_null
        not_null_cols = [t["column"] for t in new_tests if t["test_type"] == "not_null"]
        assert "state" not in not_null_cols
        assert "capacity" not in not_null_cols
        assert "updated_at" not in not_null_cols

    def test_schema_yml_updated(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        output = reason_outputs["update_schema_yml"]
        assert output["tests_added_count"] == 8
        assert "dim_venue" in output["modified_yml"]
        assert "accepted_values" in output["modified_yml"]

    def test_existing_tests_preserved(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        yml = reason_outputs["update_schema_yml"]["modified_yml"]
        # venue_id should still have unique test
        assert "unique" in yml
        # venue_name should still have not_null
        assert "venue_name" in yml

    def test_validation_passes(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        assert reason_outputs["validate"]["parse_ok"] is True
        assert reason_outputs["validate"]["tests_passed"] == 11
        assert reason_outputs["validate"]["tests_failed"] == 0

    def test_report_matches_golden(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        with open(self.GOLDEN_DIR / "test_coverage_output.json") as f:
            golden = json.load(f)
        infer = reason_outputs["infer_tests"]
        assert infer["total_tests_to_add"] == golden["new_tests_count"]
        av_cols = [t["column"] for t in infer["new_tests"] if t["test_type"] == "accepted_values"]
        assert set(av_cols) == set(golden["columns_with_accepted_values"])

    def test_trace_has_all_5_steps(self):
        run, _, _ = run_workflow(self.CASSETTE_DIR, self.INPUTS)
        trace = run.get_trace()
        assert trace["workflow_id"] == "add-dbt-tests"
        assert trace["status"] == "completed"
        assert len(trace["steps"]) == 5

    def test_reason_request_has_persona_and_tools(self):
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        run = engine.start("add-dbt-tests", self.INPUTS)

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "resolve_model"
        assert request.persona.id == "analytics_engineer"
        assert len(request.persona.heuristics) > 0
        assert len(request.tools) > 0
        assert request.context.estimated_tokens > 0

    def test_infer_tests_has_no_tools(self):
        """Verify infer_tests is pure reasoning with no tools."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        cassettes = load_cassettes(self.CASSETTE_DIR)
        run = engine.start("add-dbt-tests", self.INPUTS)

        # Walk to infer_tests (step index 2)
        for step_id in EXPECTED_STEPS[:2]:
            request = run.next_step()
            run.record_result(step_id, StepSuccess(output=cassettes[step_id]["output"]))

        request = run.next_step()
        assert isinstance(request, ReasonRequest)
        assert request.step_id == "infer_tests"
        assert len(request.tools) == 0
```

### 2.6 — Conformance Test

- [ ] Create `tests/test_conformance_add_dbt_tests.py`

```python
"""Conformance tests for add-dbt-tests workflow.

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


class TestAddDbtTestsConformance:
    """Validate that add-dbt-tests assembles correct context per step."""

    WORKFLOW_ID = "add-dbt-tests"
    INPUTS = {"model_name": "dim_venue"}

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
                step_outputs = {
                    "resolve_model": {
                        "model_path": "models/marts/core/dim_venue.sql",
                        "schema_yml_path": "models/marts/core/schema.yml",
                        "source_table_fqn": "DW.CORE.VENUE_DIM",
                        "existing_tests": [
                            {"column": "venue_id", "tests": ["not_null", "unique"]}
                        ],
                        "columns_without_tests": ["city", "state", "country"],
                    },
                    "get_column_metadata": {
                        "column_metadata": [
                            {"name": "VENUE_ID", "sf_type": "NUMBER(38,0)", "nullable": False, "null_pct": 0.0, "cardinality": 15000, "row_count": 15000}
                        ],
                    },
                    "infer_tests": {
                        "new_tests": [
                            {"column": "city", "test_type": "not_null", "rationale": "Non-nullable column"}
                        ],
                        "total_tests_to_add": 8,
                        "columns_affected": ["city", "country", "venue_type", "created_at", "is_active"],
                    },
                    "update_schema_yml": {
                        "modified_yml": "version: 2\nmodels:\n  - name: dim_venue\n",
                        "tests_added_count": 8,
                        "columns_modified": ["city", "country", "venue_type", "created_at", "is_active"],
                    },
                    "validate": {
                        "parse_ok": True,
                        "tests_passed": 11,
                        "tests_failed": 0,
                        "errors": [],
                    },
                }
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

### 2.7 — Manifest Update

- [ ] Add entry to `content/workflows/manifest.yml`

Add this entry after the `generate-schema-yml` entry:

```yaml
  - id: add-dbt-tests
    name: Add dbt Tests to Existing Model
    persona: analytics_engineer
    triggers:
      keywords: [add tests, dbt tests, test coverage, add test, improve tests]
      input_pattern: "add tests to {model}"
```

### 2.8 — Verify & Commit

- [ ] Run tests:
```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_conformance_add_dbt_tests.py tests/test_e2e_add_dbt_tests.py -v
```

- [ ] Commit:
```bash
git add \
  content/workflows/add-dbt-tests.yml \
  content/workflows/add-dbt-tests.test.yml \
  content/workflows/manifest.yml \
  tests/cassettes/add-dbt-tests/ \
  tests/goldens/add-dbt-tests/ \
  tests/test_e2e_add_dbt_tests.py \
  tests/test_conformance_add_dbt_tests.py

git commit -m "$(cat <<'EOF'
feat: add add-dbt-tests workflow (5 steps)

Workflow analyzes existing model test coverage, infers missing tests
based on column metadata (not_null, unique, accepted_values, relationships),
and updates schema.yml. Includes conformance spec, cassette-driven E2E tests,
and golden file.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

---

## Final Manifest State

After both tasks, `content/workflows/manifest.yml` should contain:

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

  - id: generate-schema-yml
    name: Generate schema.yml for dbt Model
    persona: analytics_engineer
    triggers:
      keywords: [generate schema, add documentation, document model, schema yml]
      input_pattern: "generate schema for {model}"

  - id: add-dbt-tests
    name: Add dbt Tests to Existing Model
    persona: analytics_engineer
    triggers:
      keywords: [add tests, dbt tests, test coverage, add test, improve tests]
      input_pattern: "add tests to {model}"
```

---

## File Inventory

### Task 1 — generate-schema-yml (8 files)
| File | Type |
|------|------|
| `content/workflows/generate-schema-yml.yml` | Workflow definition |
| `content/workflows/generate-schema-yml.test.yml` | Conformance spec |
| `tests/cassettes/generate-schema-yml/resolve_model.json` | Cassette |
| `tests/cassettes/generate-schema-yml/parse_columns.json` | Cassette |
| `tests/cassettes/generate-schema-yml/describe_columns.json` | Cassette |
| `tests/cassettes/generate-schema-yml/generate_yml.json` | Cassette |
| `tests/cassettes/generate-schema-yml/validate.json` | Cassette |
| `tests/goldens/generate-schema-yml/schema_yml_output.json` | Golden |
| `tests/test_e2e_generate_schema_yml.py` | E2E test |
| `tests/test_conformance_generate_schema_yml.py` | Conformance test |

### Task 2 — add-dbt-tests (8 files)
| File | Type |
|------|------|
| `content/workflows/add-dbt-tests.yml` | Workflow definition |
| `content/workflows/add-dbt-tests.test.yml` | Conformance spec |
| `tests/cassettes/add-dbt-tests/resolve_model.json` | Cassette |
| `tests/cassettes/add-dbt-tests/get_column_metadata.json` | Cassette |
| `tests/cassettes/add-dbt-tests/infer_tests.json` | Cassette |
| `tests/cassettes/add-dbt-tests/update_schema_yml.json` | Cassette |
| `tests/cassettes/add-dbt-tests/validate.json` | Cassette |
| `tests/goldens/add-dbt-tests/test_coverage_output.json` | Golden |
| `tests/test_e2e_add_dbt_tests.py` | E2E test |
| `tests/test_conformance_add_dbt_tests.py` | Conformance test |

### Shared (1 file modified)
| File | Change |
|------|--------|
| `content/workflows/manifest.yml` | +2 workflow entries |

**Total: 19 new files + 1 modified file, across 2 commits.**

---

## Test Commands Summary

```bash
# Run all new tests
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph

# Task 1 tests
python -m pytest tests/test_conformance_generate_schema_yml.py tests/test_e2e_generate_schema_yml.py -v

# Task 2 tests
python -m pytest tests/test_conformance_add_dbt_tests.py tests/test_e2e_add_dbt_tests.py -v

# All tests (including existing)
python -m pytest tests/ -v
```
