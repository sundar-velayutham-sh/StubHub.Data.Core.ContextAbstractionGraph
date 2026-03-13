# DCAG v2 — Engine Features, Workflow Expansion & Context Architecture

> **Status**: Design approved
> **Date**: 2026-03-13
> **Author**: Sundar Velayutham + Claude
> **Scope**: 4 engine features + 6 workflows + 5-layer context architecture
> **Research basis**: Industry survey (7 systems), PR analysis (400+ PRs across 5 repos), Snowflake operational metadata, Shift live integration testing

---

## 1. Problem Statement

StubHub has ~35 data practitioners doing repetitive expert work across 5 repositories. Analysis of 400+ merged PRs shows that 60%+ follow recognizable patterns. Today each engineer carries these patterns in their head — when they leave, the patterns leave. When someone new joins, they learn through months of trial and error.

DCAG turns tribal knowledge into executable AI workflows. An engineer types a request in Slack → Shift matches it to a DCAG workflow → DCAG assembles context per step (knowledge, tools, persona, prior outputs) → Shift reasons and executes → output is a ready-to-merge PR that follows the team's exact conventions.

The compounding effect: every workflow run produces a decision trace. Those traces become knowledge for future runs. The system gets smarter with use.

### Current State

- Engine: DCAGEngine + WorkflowRun + ShiftDriver (125 tests passing)
- Workflows: 2 (add-column-to-model, table-optimizer)
- Personas: 2 (analytics_engineer, data_engineer)
- Knowledge files: 11
- PR coverage: ~4.5% of Astronomer repo volume
- Validated: Both workflows tested end-to-end via Shift with real Snowflake data

### Target State

- Engine: + conditional walker, schema cache, step loops, decision traces
- Workflows: 8 (existing 2 + 6 new)
- Personas: 2 (enriched)
- Knowledge files: ~19 (11 existing + 8 new)
- PR coverage: ~45% of Astronomer repo volume

---

## 2. Industry Context

Survey of 7 systems positions DCAG as "Gen 4" — encoding procedural expertise rather than data catalogs (Gen 1), semantic meaning (Gen 2), or agent memory (Gen 3).

| System | What It Models | DCAG Equivalent |
|--------|---------------|-----------------|
| Graphiti/Zep | What happened (temporal facts) | Decision traces (Feature 4) |
| GraphRAG | What's related (community summaries) | Knowledge files |
| LangGraph | How to orchestrate (state graphs) | Workflow YAML + Walker |
| Temporal.io | How to execute durably (pull model) | next_step/record_result loop |
| CrewAI | Who does what (role-based agents) | Personas |

DCAG uniquely combines all five in one headless engine with tool gating and runtime degradation.

### What DCAG Does That Others Don't
1. Tool gating per step (principle of least authority)
2. Headless pull model (zero LLM code, zero credentials)
3. Persona-as-structured-data (auditable, testable, composable)
4. Runtime degradation (ToolRegistry filters unavailable tools)

### What to Adopt from Others
1. Conditional edges (from LangGraph) → Feature 1
2. Decision trace persistence (from Graphiti) → Feature 4
3. MCP server exposure (ecosystem trend) → Future phase

---

## 3. Four Core Engine Features

### Feature 1: Conditional Walker

**What**: Steps declare transitions that route to different next steps based on output values.

```yaml
- id: classify_bug_type
  mode: reason
  transitions:
    - when: "output.bug_type == 'cast_error'"
      goto: fix_cast_error
    - when: "output.bug_type == 'join_error'"
      goto: fix_join_error
    - default: fix_generic_error
```

**Why #1 priority**: Every workflow is a straight line today. The fix-bug workflow has 3+ bug types needing completely different diagnostic steps. Without branching, you either build 3 separate workflows or cram everything into one mega-prompt that wastes tokens on irrelevant paths.

**Expression evaluator**: Supports ==, !=, >, <, in operators on step output fields. String equality and list membership for v1. No nested expressions.

**Implementation**:
- New file: `src/dcag/_evaluator.py` (~50 lines) — expression parser and evaluator
- Modify: `src/dcag/_walker.py` — `advance()` checks transitions before linear fallback
- Modify: `src/dcag/types.py` — add `transitions` field to `StepDef`
- Modify: `src/dcag/_loaders.py` — parse transitions from YAML
- Tests: ~15 new tests (evaluator unit tests + walker branching tests)
- Effort: ~150 lines

**What it unlocks**: Better add-column (4 intent paths), fix-model-bug (3+ bug types), table-optimizer (SKIP vs CLUSTER implementation), every future workflow with variants.

---

### Feature 2: Schema Cache

**What**: A WorkflowRun-scoped dict populated in step 0 or a dedicated context-warming step. Subsequent steps access cached metadata without MCP calls.

```yaml
- id: discover_column
  context:
    cache: [table_columns, storage_metrics]   # from Schema Cache
    static: [sf_type_mapping]                  # from YAML
    dynamic:
      - from: resolve_model
        select: source_table_fqn
```

**Why critical**: Every workflow queries INFORMATION_SCHEMA 3-5 times per run. Each MCP call is 500-3000ms. A 9-step workflow making 15 redundant metadata queries wastes 10-30 seconds and burns LLM turns.

**Implementation**:
- Modify: `src/dcag/engine.py` — add `_schema_cache: dict` to WorkflowRun, populated from step outputs that declare `cache_keys`
- Modify: `src/dcag/_context.py` — `assemble_reason()` merges cache entries into dynamic context
- Modify: `src/dcag/types.py` — add `cache_keys` to StepDef output_schema
- Tests: ~10 new tests
- Effort: ~80 lines

**What it unlocks**: Faster execution (metadata loaded once), richer context per step (pre-load ALL columns, not just the one being asked about), lower token cost.

---

### Feature 3: Step Loops

**What**: A step declares it operates on a collection from a prior step. The engine executes it N times, once per item.

```yaml
- id: modify_each_model
  mode: reason
  loop:
    over: trace_pipeline.models_in_chain
    as: current_model
  instruction: "Modify {{current_model.path}} to add the column..."
```

**Why it matters**: Pipeline threading modifies 3-11 models. Data share setup generates 50+ files. Without loops, these can't be built as DCAG workflows.

**Implementation**:
- Modify: `src/dcag/_walker.py` — track loop index and item, advance within loop before advancing step
- Modify: `src/dcag/_context.py` — inject `current_model` (or whatever `as` declares) into dynamic context
- Modify: `src/dcag/types.py` — add `loop` field to StepDef
- Modify: `src/dcag/_loaders.py` — parse loop config from YAML
- Tests: ~12 new tests
- Effort: ~100 lines

**Depends on**: Feature 1 (loop termination is a transition type)

**What it unlocks**: thread-field-through-pipeline (Workflow 5), data-share-setup (future), any N-entity workflow.

---

### Feature 4: Decision Trace Persistence

**What**: After a workflow completes, key decisions are persisted as searchable knowledge nodes. Future runs can query: "what did we decide last time for this table?"

```yaml
# Auto-generated after run
# Stored at: data/decisions/DW.RPT.TRANSACTION/dcag-a1b2c3d4.json
{
  "workflow": "table-optimizer",
  "run_id": "dcag-a1b2c3d4",
  "entity": "DW.RPT.TRANSACTION",
  "decided_at": "2026-03-12T14:30:00Z",
  "facts": {
    "load_frequency": "DAILY",
    "strategy": "CLUSTER_BY",
    "clustering_keys": ["SALE_DATE", "EVENT_PARENT_CATEGORY_NAME"]
  },
  "confidence": "high",
  "valid_until": "2026-06-12"
}
```

Future workflows consume decisions:
```yaml
context:
  decisions: [{entity: "{{inputs.table_name}}"}]
```

**Implementation**:
- New file: `src/dcag/_decisions.py` (~80 lines) — write/read/search decision traces
- Modify: `src/dcag/engine.py` — after workflow completes, extract decisions from final step output and persist
- Modify: `src/dcag/_context.py` — `build_decisions()` loads matching traces into dynamic context
- Storage: JSON files in `data/decisions/{entity_name}/` indexed by entity
- Tests: ~10 new tests
- Effort: ~120 lines

**What it unlocks**: Cross-workflow learning, outcome tracking, organizational memory. Compounds with every run.

---

### Feature Dependencies and Build Order

```
Feature 1: Conditional Walker  ← no dependencies, unblocks everything
Feature 2: Schema Cache        ← no dependencies, performance win
Feature 3: Step Loops          ← depends on Feature 1
Feature 4: Decision Traces     ← no dependencies, value grows with usage

Build order: 1 → 2 (parallel with 1) → 3 → 4
```

---

## 4. Six High-Impact Workflows

### PR Analysis Evidence

| Activity | % of PRs | Automatable? | Existing DCAG? |
|----------|----------|-------------|----------------|
| Bug fixes (cast, join, logic) | 19% | Partially | No |
| New model/feature creation | 17.5% | Yes | No |
| Config/parameter updates | 11% | Yes | No |
| Refactor/deprecation | 7.5% | Yes | No |
| Testing | 5% | Yes | No |
| Documentation | 3.5% | Yes | No |
| Column additions | 2.5% | Yes | add-column-to-model |
| Performance optimization | 2% | Yes | table-optimizer |

The 2 existing workflows cover ~4.5% of PR volume. The 6 new workflows bring coverage to ~45%.

---

### Workflow 1: fix-model-bug

**Persona**: analytics_engineer
**Trigger**: "fix the error in {model}" / "why is {model} failing?"
**Frequency**: ~10/week (19% of PRs)
**Engine features needed**: Conditional Walker (routes by bug type)

**Steps** (8, branching):
```
parse_error → read_model_sql → classify_bug_type
                                      │
                    ┌─────────────────┼──────────────────┐
                    ▼                 ▼                   ▼
              fix_cast_error    fix_join_error    fix_logic_error
                    │                 │                   │
                    └─────────────────┼──────────────────┘
                                      ▼
                              validate_fix → show_plan → create_pr
```

**Context**:
- Static: naming_conventions, NEW troubleshooting_patterns.yml, NEW data_quality_checks.yml
- Dynamic: error message, model SQL, upstream source metadata
- MCP: dbt_mcp (compile, test, show), snowflake_mcp (sample data), github_cli (read/write)
- Cache: table column types

**New knowledge files**: troubleshooting_patterns.yml (common error→fix catalog), data_quality_checks.yml

---

### Workflow 2: create-staging-model

**Persona**: analytics_engineer
**Trigger**: "create staging model for {table} from {source}"
**Frequency**: ~4/week
**Engine features needed**: Schema Cache

**Steps** (8, linear):
```
discover_source_table → check_existing_models → choose_materialization
    → generate_model_sql → generate_schema_yml → add_to_sources_yml
    → validate → create_pr
```

**Context**:
- Static: naming_conventions, dbt_project_structure, sf_type_mapping, NEW model_templates.yml
- Dynamic: source table columns, types, row counts
- MCP: snowflake_mcp, dbt_mcp (compile, parse), github_cli
- Cache: full column list from source table (used in steps 4, 5, 6)

**New knowledge files**: model_templates.yml (CTE patterns per materialization type)

---

### Workflow 3: generate-schema-yml

**Persona**: analytics_engineer
**Trigger**: "add documentation for {model}" / "generate schema for {model}"
**Frequency**: ~4/week
**Engine features needed**: None (works with today's DCAG)

**Steps** (6, linear):
```
resolve_model → parse_columns_from_sql → describe_columns_in_snowflake
    → generate_yml → validate → create_pr
```

**Context**:
- Static: naming_conventions, testing_standards, dbt_project_structure
- Dynamic: model SQL, Snowflake column metadata
- MCP: snowflake_mcp (DESCRIBE TABLE), dbt_mcp (parse), github_cli

**New knowledge files**: None

---

### Workflow 4: add-dbt-tests

**Persona**: analytics_engineer
**Trigger**: "add tests to {model}" / "improve test coverage for {model}"
**Frequency**: ~3/week
**Engine features needed**: None (works with today's DCAG)

**Steps** (5, linear):
```
resolve_model → get_column_metadata → infer_tests → update_schema_yml → validate
```

**Context**:
- Static: testing_standards, NEW test_inference_rules.yml
- Dynamic: column types, cardinality stats, existing tests
- MCP: snowflake_mcp (DISTINCT counts, NULL rates), dbt_mcp (test), github_cli

**New knowledge files**: test_inference_rules.yml (column name patterns → test types: PK→unique+not_null, enum→accepted_values, FK→relationships)

---

### Workflow 5: thread-field-through-pipeline

**Persona**: analytics_engineer
**Trigger**: "add {column} through the {model} pipeline"
**Frequency**: ~2/week
**Engine features needed**: Conditional Walker + Step Loops

**Steps** (7, with loops):
```
resolve_source_column → trace_pipeline_lineage → show_plan (GATE)
    → modify_each_model (LOOP) → update_each_schema (LOOP)
    → validate_pipeline → create_pr
```

**Loop**: `trace_pipeline_lineage` produces `models_in_chain: [{path, layer, existing_columns}, ...]`. Steps 4 and 5 loop over this list.

**Context**:
- Static: naming_conventions, NEW pipeline_threading_conventions.yml
- Dynamic: full pipeline lineage, column types at each layer
- MCP: dbt_mcp (get_lineage_dev, compile, test), github_cli, snowflake_mcp
- Cache: column metadata for all models in chain

**New knowledge files**: pipeline_threading_conventions.yml

---

### Workflow 6: configure-ingestion-pipeline

**Persona**: data_engineer
**Trigger**: "add ingestion for {table} from {source_database}"
**Frequency**: ~2/week
**Engine features needed**: Schema Cache

**Steps** (7, linear):
```
discover_source_schema → design_staging_table → generate_ingestion_config
    → configure_load_frequency → validate_connectivity → show_plan → create_pr
```

**Context**:
- Static: ingestion_conventions (enrich existing), sf_type_mapping, NEW database_classes.yml
- Dynamic: source table columns, existing ingestion configs
- MCP: snowflake_mcp, github_cli

**New knowledge files**: database_classes.yml (catalog of source DB classes)

---

### Workflow Summary

| # | Workflow | Frequency | Engine Features | New Knowledge |
|---|---------|-----------|-----------------|---------------|
| 1 | fix-model-bug | ~10/week | Conditional Walker | troubleshooting_patterns, data_quality_checks |
| 2 | create-staging-model | ~4/week | Schema Cache | model_templates |
| 3 | generate-schema-yml | ~4/week | None | None |
| 4 | add-dbt-tests | ~3/week | None | test_inference_rules |
| 5 | thread-field-through-pipeline | ~2/week | Conditional Walker + Loops | pipeline_threading_conventions |
| 6 | configure-ingestion-pipeline | ~2/week | Schema Cache | database_classes |

**Build order**: Workflows 3+4 first (no engine changes needed), then 1+2 (after Features 1+2), then 5+6 (after Feature 3).

---

## 5. Context Architecture — Five Bundle Types

### Bundle 1: Procedural Context — "How to do it"

Conventions, patterns, templates, anti-patterns. Stable knowledge that changes quarterly.

| Property | Value |
|----------|-------|
| Examples | naming_conventions, sf_type_mapping, troubleshooting_patterns |
| Freshness | Weeks to months |
| Source | Expert observation, team agreements |
| Maintainer | Domain leads (human-authored YAML) |
| Scale | 11 files → 19 files → 100+ files (namespaced) |

**Technology**: YAML files in `content/knowledge/` (keep as-is). At 100+ files, add hierarchical namespaces and tags. No database needed.

**Delivery**: Static edges (today) + conditional edges (Feature 1).

---

### Bundle 2: Structural Context — "What exists right now"

Table columns, types, row counts, model metadata. Changes when DDL changes.

| Property | Value |
|----------|-------|
| Examples | INFORMATION_SCHEMA.COLUMNS, TABLE_STORAGE_METRICS, dbt model config |
| Freshness | Hours |
| Source | Snowflake, dbt manifest, GitHub |
| Maintainer | Auto-populated |

**Technology**: In-memory dict on WorkflowRun (Schema Cache — Feature 2). Populated in step 0. Not persisted across runs.

**Delivery**: MCP populates cache in step 0. Steps reference via `context.cache`. Fallback: live MCP call on cache miss.

---

### Bundle 3: Relational Context — "What connects to what"

Model lineage, team ownership, schema-to-domain mapping.

| Property | Value |
|----------|-------|
| Examples | Upstream/downstream models, column lineage, team ownership |
| Freshness | Daily |
| Source | dbt manifest.json, CODEOWNERS |

**Technology (Phase 1)**: dbt_mcp.get_lineage_dev (live queries).
**Technology (Phase 2)**: NetworkX graph from manifest.json (multi-hop traversal).
**Technology (Phase 3)**: Kuzu graph DB (when cross-workflow lineage queries justify it).

**Delivery**: MCP (Phase 1) → in-memory graph (Phase 2) → Kuzu MCP server (Phase 3).

---

### Bundle 4: Temporal Context — "What happened before"

Decision traces from prior runs, failure history, past fixes.

| Property | Value |
|----------|-------|
| Examples | "Last optimization chose CLUSTER_BY", "This model failed 3x due to upstream NULL" |
| Freshness | Real-time (per run) |
| Source | DCAG traces, dbt run results |

**Technology (Phase 1)**: JSON files in `data/decisions/` indexed by entity.
**Technology (Phase 2)**: LanceDB vectors (semantic search over traces).

**Delivery**: Steps declare `context.decisions: [{entity: "{{inputs.table_name}}"}]`. Assembler loads matching traces.

---

### Bundle 5: Operational Context — "What's happening right now"

Live system state: warehouse utilization, DAG status, freshness alerts.

| Property | Value |
|----------|-------|
| Examples | Warehouse capacity, source freshness, running DAGs |
| Freshness | Minutes |
| Source | Snowflake ACCOUNT_USAGE, Airflow API |

**Technology**: MCP tools only. Never cached. snowflake_mcp (existing) + airflow_mcp (future).

**Delivery**: Tool calls within steps. ToolRegistry gates which MCP tools each step can use.

---

### Future Bundles (Not in Scope)

**Bundle 6: Business/Semantic Context** — metric definitions, business glossary, domain knowledge. Needed when building create-semantic-model and create-experiment-metrics workflows. Delivery: RAG over Confluence + dbt descriptions.

**Bundle 7: Policy/Compliance Context** — data classification, RBAC rules, retention policies, approval chains. Needed when building grant-permissions and create-external-share workflows. Delivery: YAML knowledge files + validation enforcement layer.

---

### Context Flow Through a Workflow Step

```
Step N (reason step)
  │
  │ ContextAssembler builds ReasonRequest from:
  │
  │   ALWAYS INCLUDED:
  │   ├── Persona (merged with step overrides)
  │   ├── Static knowledge (from YAML edges)
  │   ├── Dynamic context (from prior step outputs)
  │   └── Tools (filtered by ToolRegistry)
  │
  │   INCLUDED IF DECLARED:
  │   ├── Conditional knowledge (if prior output matches condition)
  │   ├── Cached metadata (from Schema Cache)
  │   └── Decision traces (matching entity)
  │
  │   FETCHED DURING REASONING (by LLM via MCP):
  │   ├── Live Snowflake queries
  │   ├── File reads from GitHub
  │   └── dbt compile/test results
  │
  │ Token budget priority:
  │   Tools → Persona → Static → Dynamic → Cache → Traces
  │   If over budget: trim traces first, then cache, then warn
```

---

### Technology Decision Matrix

| Bundle | Phase 1 (now) | Phase 2 (3-6 months) | Phase 3 (6-12 months) |
|--------|---------------|---------------------|----------------------|
| Procedural | YAML files, flat | YAML, namespaced + tags | YAML + tag search |
| Structural | MCP per call | Schema Cache (dict) | Schema Cache + export |
| Relational | dbt_mcp live | NetworkX from manifest | Kuzu graph DB |
| Temporal | JSON files by entity | JSON + search | LanceDB vectors |
| Operational | snowflake_mcp | + airflow_mcp | + monitoring_mcp |

**Key principle**: Start with the simplest thing that works. Dict before database. Files before vectors. MCP before graph. Upgrade when the pain justifies the complexity.

---

## 6. DCAG vs Shift Responsibility Split

| Responsibility | Owner | Why |
|----------------|-------|-----|
| Workflow definition + traversal | DCAG | Declarative, testable, versionable |
| Context assembly per step | DCAG | Deterministic, token-budgeted |
| Tool gating + degradation | DCAG | Safety, principle of least authority |
| Output validation | DCAG | Schema enforcement without LLM |
| Trace recording + persistence | DCAG | Structured, queryable |
| LLM reasoning | Shift | Shift owns the Claude API key |
| MCP tool execution | Shift | Shift owns MCP connections |
| Human interaction (Slack) | Shift | Shift is the Slack bot |
| PR creation + branch management | Shift | Shift has GitHub access |
| Approval workflows | Shift | Slack thread + button interactions |
| Parallel step dispatch | Shift | Shift manages concurrency |
| Loop step iteration | Shift | Shift drives the loop, DCAG tracks index |

**New Shift capability**: `run.request_context(query, tags)` — mid-step, Shift asks DCAG for additional knowledge. Bridge to Phase 2 semantic retrieval.

---

## 7. New Knowledge Files

| File | Used By | Priority |
|------|---------|----------|
| troubleshooting_patterns.yml | fix-model-bug | High |
| model_templates.yml | create-staging-model | High |
| test_inference_rules.yml | add-dbt-tests | High |
| pipeline_threading_conventions.yml | thread-field-through-pipeline | High |
| database_classes.yml | configure-ingestion-pipeline | High |
| data_quality_checks.yml | fix-model-bug | Medium |
| sla_contracts.yml | future freshness workflows | Medium |
| dag_catalog.yml | future scheduling workflows | Medium |

---

## 8. What Problems This Solves

| Problem | Today | With DCAG v2 |
|---------|-------|-------------|
| Engineer leaves, patterns leave | Knowledge loss | Patterns are in YAML, survive turnover |
| New hire takes 3 months to learn | Slow onboarding | DCAG enforces conventions from day 1 |
| Same bug fixed 10 different ways | Inconsistency | fix-model-bug applies the canonical fix |
| 11-file pipeline change done manually | Error-prone, slow | thread-field-through-pipeline automates lineage tracing |
| Models deployed without tests or docs | Quality gaps | add-dbt-tests + generate-schema-yml close the gap |
| Ingestion setup requires tribal knowledge | Bottleneck on 2-3 people | configure-ingestion-pipeline is self-service |
| No memory of past decisions | Every problem solved from scratch | Decision traces compound knowledge |
| 60% of PRs follow patterns | Manual execution of known patterns | 45% automated via DCAG workflows |

---

## 9. Implementation Roadmap

| Sprint | Duration | Engine Features | Workflows | Knowledge Files |
|--------|----------|----------------|-----------|-----------------|
| Sprint 1 | 2 weeks | — | generate-schema-yml, add-dbt-tests | test_inference_rules |
| Sprint 2 | 2 weeks | Conditional Walker, Schema Cache | — | — |
| Sprint 3 | 2 weeks | — | fix-model-bug, create-staging-model | troubleshooting_patterns, model_templates, data_quality_checks |
| Sprint 4 | 2 weeks | Step Loops | thread-field-through-pipeline | pipeline_threading_conventions |
| Sprint 5 | 2 weeks | Decision Traces | configure-ingestion-pipeline | database_classes |
| Sprint 6 | 1 week | — | Hardening, Shift integration testing | — |

**Sprint 1** delivers immediate value (2 workflows that work with today's engine).
**Sprints 2-5** build engine features and unlock progressively more complex workflows.
**Sprint 6** validates everything end-to-end via Shift.

Total: ~11 weeks, ~600 lines of engine code, 6 workflow YAMLs, 8 knowledge files, ~80 new tests.

---

## 10. Scaling Path (Beyond This Spec)

| Phase | Timeline | Scale | Key Addition |
|-------|----------|-------|-------------|
| Capture (now) | 0-3 months | 8 workflows, 2 personas | This spec |
| Expand | 3-6 months | 15 workflows, 5 personas | + Business/Semantic Context (RAG), + NetworkX lineage |
| Accumulate | 6-12 months | 30 workflows, 10 personas | + Policy/Compliance Context, + Kuzu graph, + airflow_mcp |
| Compound | 12-18 months | 50+ workflows | + DCAG as MCP server, + cross-workflow decision sharing |
| Autonomous | 18-24 months | Self-evolving | + Agent-proposed workflow refinements (human-approved) |

---

## Sources

- Context Graphs in Agentic AI Research: `docs/context-graphs-in-agentic-ai-research.md`
- StubHub Data Team Workflow Analysis: `docs/stubhub-data-team-workflow-analysis.md`
- Shift Live Integration Test Results (2026-03-12): Both workflows passed E2E
- PR Analysis: 400+ PRs across Astronomer.Core.Data, Snowflake.Schema, Snowflake.Infrastructure, Fintech.Astronomer.Dbt, CDC.Ingestion
- Snowflake Operational Data: 3,676 dbt models, 21.4M SELECT queries/month, 5 warehouse tiers
