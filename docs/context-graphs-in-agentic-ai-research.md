# Context Graphs in Agentic AI Systems: Industry Survey and DCAG Analysis

**Author**: Sundar Velayutham + Claude
**Date**: 2026-03-12
**Scope**: Industry landscape, taxonomy, DCAG positioning, scaling strategy

---

## Part 1: Industry Survey of Context Graphs

### 1.1 Graphiti / Zep — Temporal Knowledge Graphs for Agent Memory

Zep's open-source engine **Graphiti** is the most architecturally relevant system to compare with DCAG, though they solve different problems. Graphiti builds a **temporal context graph** where every edge carries a validity window (when a fact became true, when it was superseded). The core loop is: entity extraction from conversation -> graph update with conflict resolution -> retrieval at query time.

Key architectural properties:
- **Bi-temporal model**: tracks both when an event occurred and when it was ingested. When conflicts arise, temporal metadata determines which facts to invalidate.
- **No LLM at query time**: Unlike GraphRAG, Graphiti pre-processes facts into the graph during ingestion, so retrieval is fast (no LLM-driven summarization at read time). This mirrors DCAG's design principle of separating the graph from the reasoning engine.
- **Entity + relationship + fact nodes**: The graph stores entities (people, concepts), relationships between them, and discrete facts with temporal bounds.
- **94.8% accuracy on Deep Memory Retrieval benchmark** vs 93.4% for MemGPT.

**Relevance to DCAG**: Graphiti answers "what happened / what changed" while DCAG answers "how should an expert work." They are complementary — DCAG could consume Graphiti-style decision traces as knowledge inputs.

### 1.2 Microsoft GraphRAG — Graph-Based Retrieval Augmented Generation

GraphRAG builds a knowledge graph from source documents in two stages: (1) extract an entity knowledge graph, then (2) pre-generate community summaries for clusters of related entities using bottom-up hierarchical clustering.

Key architectural properties:
- **Community summaries**: The graph is partitioned into semantic clusters. Each community gets a pre-generated summary that captures main entities, relationships, and key claims.
- **Global search vs local search**: Global search reasons over community summaries for corpus-wide questions. Local search fans out from specific entities to their neighbors.
- **Multi-hop reasoning**: The graph structure enables traversals that vector-only RAG cannot perform — connecting concepts across multiple relationship hops.
- **Designed for 1M+ token corpora** where conventional RAG produces shallow answers.

**Relevance to DCAG**: GraphRAG's community summaries are analogous to DCAG's knowledge files, but GraphRAG auto-generates them while DCAG's are hand-authored by domain experts. As DCAG scales to 1000+ knowledge files, GraphRAG's clustering approach could auto-organize knowledge and detect gaps.

### 1.3 LangGraph — Agent Orchestration with State Graphs

LangGraph models agent workflows as **directed graphs** where nodes are computation steps and edges (including conditional edges) determine control flow. State is centralized and persists throughout the workflow.

Key architectural properties:
- **Centralized state**: A shared state object is accessible to all nodes for reading and updating. This is fundamentally different from DCAG's dynamic context, where each step sees only what it explicitly declares.
- **Conditional edges**: Evaluate current state to decide the next execution path — agent confidence scores, external system statuses, or any state property.
- **Fan-out / fan-in**: A single node can trigger multiple downstream nodes (parallel execution), then converge on a single target.
- **Human-in-the-loop**: Built-in interrupt/approval patterns.

**Relevance to DCAG**: LangGraph is DCAG's closest structural analog — both model workflows as graphs with typed steps. The critical difference: LangGraph nodes contain the LLM calls (push model), while DCAG nodes emit typed requests that a driver fulfills (pull model). LangGraph's conditional edges are the most significant feature DCAG should adopt for its Walker v2.

### 1.4 Google Vertex AI Agent Builder — Structured Agent Pipelines

Google's approach centralizes tool governance, memory, and orchestration in a managed platform.

Key architectural properties:
- **Agent Development Kit (ADK)**: Deterministic guardrails and orchestration controls with a visual canvas for designing agent flows.
- **Cloud API Registry**: Centralized tool governance — view and manage MCP servers and tools an agent can access. This is the enterprise-scale version of DCAG's ToolRegistry.
- **Topic-based memory**: Agents recall user preferences across weeks or months using topic-scoped memory banks (now GA).
- **Self-healing tool use**: A plugin that detects tool call failures and automatically retries with different parameters.
- **Agent-level tracing**: Real-time debugging with tool auditing and orchestrator visualization.

**Relevance to DCAG**: Google's Cloud API Registry validates DCAG's ToolRegistry concept but at platform scale. The self-healing tool use pattern (detect failure, retry differently) could inform DCAG's fallback_on_failure handling. Topic-based memory aligns with DCAG's knowledge file organization.

### 1.5 Temporal.io — Durable Workflow Execution

Temporal provides the strongest architectural precedent for DCAG's pull model.

Key architectural properties:
- **Pull-based task distribution**: Clients start workflows, workflows queue tasks, workers pull tasks. The matching service distributes work to available workers.
- **Durable execution**: State is captured at every step. On failure, execution resumes exactly where it left off — no lost progress, no orphaned processes.
- **Event sourcing**: Workflows replay from event history. Commands generated during replay are checked against existing history.
- **Replaces infrastructure patterns**: Eliminates the need for developers to build their own queuing, sagas, cron jobs, state machines, circuit breakers, or transactional outboxes.

**Relevance to DCAG**: DCAG's `next_step()` / `record_result()` loop is essentially Temporal's pull model for AI workflows. Temporal proves this pattern scales to enterprise workloads. DCAG should consider event sourcing for replay/debugging — the trace writer is a partial implementation of this, but lacks replay capability.

### 1.6 CrewAI / AutoGen — Multi-Agent Collaboration

CrewAI assigns distinct roles to agents, creating specialized teams that mimic real-world organizations.

Key architectural properties:
- **Role-based specialization**: Manager agents oversee task distribution, Worker agents execute specific tasks, Researcher agents gather information.
- **Context propagation**: Agents maintain memory of interactions and use context from previous tasks — they don't restart from zero at each step.
- **Delegation**: Agents can delegate sub-tasks to other agents.
- **45,900+ GitHub stars** and 100,000+ certified developers by early 2026.

**Relevance to DCAG**: CrewAI's role model maps to DCAG's persona concept, but CrewAI roles are implicit (in the prompt) while DCAG personas are explicit (structured YAML with heuristics, anti-patterns, quality standards). DCAG's approach is more auditable. Multi-agent coordination is the biggest gap — DCAG is currently single-workflow, single-agent.

### 1.7 Anthropic's Tool Use Patterns

Anthropic's guidance on agentic tool use directly informed DCAG's design:

- **Tool definitions deserve prompt engineering attention**: JSON schemas define structural validity but not usage patterns. DCAG's `usage_pattern` field on ToolDirective addresses this directly.
- **Tool Search Tool**: Access thousands of tools without consuming context window. Relevant for DCAG scaling — at 200+ workflows with diverse tool sets, a tool search capability would prevent context bloat.
- **Programmatic tool calling**: Invoke tools in a code execution environment to reduce context window impact.
- **Agent Skills specification**: Modular capabilities that package instructions + metadata + resources. DCAG workflows are effectively "skills" in Anthropic's taxonomy, but with graph-structured context rather than flat instruction text.

### 1.8 Emerging Patterns (2025-2026)

**Context engines as infrastructure layer**: In 2026, teams are building knowledge graphs, ontologies, and metadata-driven maps that teach AI how their business works. This is exactly what DCAG does for data engineering workflows.

**Dynamic tool routing (AutoTool)**: Research showing 4-7% gains from dynamic tool selection based on context, data type, and past performance. DCAG's static tool declarations per step are a strength (deterministic) but limit adaptability.

**MCP as universal tool protocol**: Multiple knowledge graph memory MCP servers have shipped in early 2026 with semantic search, tiered context retrieval, and event-sourced memory. DCAG could expose its context graph as an MCP server.

**Semantic foundations maturing**: Microsoft Fabric IQ, ontology-driven semantic layers, and knowledge graphs are converging. The trend is toward AI systems that understand not just data, but how the business uses that data.

---

## Part 2: Taxonomy of Context Graph Architectures

| Generation | Era | What It Models | Example Systems | How Agents Use It | Node Types | Edge Semantics |
|------------|-----|----------------|-----------------|-------------------|------------|----------------|
| **Gen 1** | 2018-2022 | What data exists | DataHub, Amundsen, Apache Atlas | Passive catalog lookup — human queries, agent reads | Tables, Columns, Dashboards, Users | owns, depends_on, tagged_with |
| **Gen 2** | 2022-2024 | What data means | dbt MetricFlow, Cube.dev, Looker semantic layer | NL-to-SQL translation — agent maps question to metric definitions | Metrics, Dimensions, Entities, Semantic Models | measures, groups_by, filters_on |
| **Gen 3** | 2024-2025 | What happened / was decided | Graphiti/Zep, GraphRAG, LangMem | Read/write agent memory — agent extracts facts, writes them, retrieves later | Entities, Facts, Episodes, Communities | occurred_at, supersedes, relates_to |
| **Gen 4** | 2025+ | How experts work | DCAG, emerging | Workflow + context + tool orchestration — graph prescribes behavior | Workflows, Steps, Personas, Knowledge, Tools, Decisions | step->knowledge, step->tool, persona->step |

### Gen 1 Limitations (drove Gen 2)
Context flows to the LLM as flat metadata catalogs. The LLM knows table X has column Y, but not what "revenue" means across different tables or how to aggregate it. No semantic layer means every LLM call re-derives business logic.

### Gen 2 Limitations (drove Gen 3)
Semantic models define what metrics mean, but they are static snapshots. No memory of prior decisions, no learning from past queries, no context accumulation. Every conversation starts from zero.

### Gen 3 Limitations (drove Gen 4)
Memory graphs remember what happened but don't prescribe how to work. An agent with Graphiti knows "last time we optimized table X, we chose clustering key Y" but doesn't have a structured workflow for table optimization. The knowledge is episodic, not procedural.

### Gen 4: What DCAG Adds
DCAG encodes **procedural expertise** — not just facts and memories, but the sequence of reasoning steps, the tools needed at each step, the heuristics an expert applies, the anti-patterns they avoid, and the quality criteria they check against. This is the difference between "knowing about Snowflake clustering" (Gen 2/3) and "knowing how a senior data engineer evaluates and implements clustering" (Gen 4).

---

## Part 3: How DCAG Maps to This Landscape

### 3.1 What DCAG Does That Other Systems Don't

**1. Tool gating per step.** No other system reviewed enforces tool availability at the graph level. LangGraph gives all tools to all nodes. CrewAI assigns tools per agent role but not per step. DCAG's workflow YAML declares exactly which tools each step may use, and the ToolRegistry filters at runtime based on detected capabilities. This is the "principle of least authority" applied to AI tool use.

**2. Headless / pull model separation.** DCAG contains zero LLM code, zero API credentials, zero UI. It is a pure orchestration library that emits typed requests (`ReasonRequest`, `DelegateRequest`, `ExecuteScriptRequest`). The driver (Shift, or any other consumer) decides how to fulfill them. This is architecturally unique — LangGraph, CrewAI, and Google ADK all embed the LLM calls within the framework. Temporal.io is the closest precedent, but for general distributed systems rather than AI workflows.

**3. Persona-as-structured-data.** CrewAI and LangGraph encode personas as prompt text. DCAG's persona is a structured YAML bundle with distinct fields: `domain_knowledge`, `default_heuristics`, `default_anti_patterns`, `quality_standards`. These merge with step-level overrides. This makes personas auditable, testable, and composable.

**4. Step-level context scoping with dynamic resolution.** Each step declares its context requirements explicitly: static knowledge files, dynamic references to prior step outputs (with field-level selection), and knowledge overlays. The ContextAssembler resolves these into a ContextBundle with token estimation. No other system reviewed provides this level of declarative context scoping.

**5. Graceful degradation.** The ToolRegistry + `fallback_on_failure` pattern enables workflows that adapt at runtime. If dbt MCP is unavailable, steps that need it degrade to Snowflake-only mode. This is more sophisticated than LangGraph's error handling and closer to Google ADK's self-healing, but defined declaratively rather than programmatically.

### 3.2 What Other Systems Do That DCAG Should Adopt

| Feature | Source System | Gap in DCAG | Priority |
|---------|--------------|-------------|----------|
| **Conditional edges** | LangGraph | Walker is linear; no branching based on step output | High — needed for workflows that fork (e.g., passthrough vs join intent in add-column) |
| **Parallel step execution** | LangGraph, Temporal | Steps run sequentially; independent steps could parallelize | Medium — table-optimizer steps 2-4 could run concurrently |
| **Temporal fact memory** | Graphiti/Zep | No cross-run memory; each workflow starts from zero | Medium — decision traces from prior runs should inform future runs |
| **Auto-generated knowledge** | GraphRAG | Knowledge files are hand-authored | Low (short-term), High (at scale) — manual authoring won't scale to 1000 files |
| **Multi-agent coordination** | CrewAI | Single workflow, single agent | Medium — complex tasks need multiple workflows sharing context |
| **Event sourcing / replay** | Temporal.io | Trace is write-only; cannot replay a run | Low — useful for debugging but not blocking |
| **Self-healing tool use** | Google ADK | Tool failures halt the step; no automatic retry with different parameters | Medium — would improve reliability |
| **MCP server exposure** | Ecosystem trend | DCAG is a library, not a server | Future — enables DCAG as a context source for any MCP-compatible agent |

### 3.3 Key Architectural Differences

**Pull vs Push**: DCAG and Temporal.io use pull models (consumer drives the loop). LangGraph, CrewAI, and Google ADK use push models (framework drives execution). Pull models are more testable (you can inject mock responses), more portable (swap drivers without changing workflows), and more observable (every request/response boundary is an instrumentation point). The tradeoff is more integration code in the driver.

**Headless vs Embedded**: DCAG has zero runtime dependencies beyond Python stdlib + PyYAML. LangGraph requires LangChain. CrewAI requires its runtime. Google ADK requires Google Cloud. This makes DCAG embeddable in any system — Shift today, but potentially VS Code extensions, CI/CD pipelines, or Jupyter notebooks tomorrow.

**Declarative vs Programmatic**: DCAG workflows are YAML. LangGraph workflows are Python code. CrewAI workflows are Python classes. Declarative workflows are easier to audit, version, diff, and validate — but harder to express complex conditional logic. This is why conditional edges in the Walker are the highest-priority adoption.

### 3.4 Competence Graph vs Memory Graph

| Dimension | Memory Graph (Graphiti) | Competence Graph (DCAG) |
|-----------|------------------------|------------------------|
| **Primary question** | What do I know? | How should I work? |
| **Node types** | Entities, facts, episodes | Steps, tools, knowledge, personas |
| **Temporal dimension** | When facts were true | Not yet (but should add: when procedures were valid) |
| **Write pattern** | Agent writes during conversation | Human expert authors in YAML |
| **Read pattern** | Retrieval at query time | Assembly at step execution time |
| **Grows from** | Conversations and events | Expert observation and iteration |
| **Compounding** | Facts accumulate over time | Procedures refine over iterations |

The two are complementary. A mature system would use DCAG for procedural knowledge ("how to optimize a table") and Graphiti for episodic knowledge ("last time we optimized TRANSACTION, we chose EVENT_DATE as clustering key because 85% of queries filter on it").

---

## Part 4: Scaling Context Graphs for Enterprise Data Teams

### 4.1 Knowledge Organization at Scale

DCAG currently has 11 knowledge files in a flat directory. At 1000 files, this breaks.

**Proposed: Hierarchical namespace with semantic tags.**

```
content/knowledge/
  snowflake/
    optimization/
      clustering_guide.yml      # tags: [clustering, performance, snowflake]
      sos_guide.yml             # tags: [search-optimization, performance, snowflake]
    security/
      rbac_patterns.yml         # tags: [security, rbac, snowflake]
  dbt/
    project_structure.yml       # tags: [dbt, structure, conventions]
    incremental_patterns.yml    # tags: [dbt, incremental, performance]
  ingestion/
    cdc_patterns.yml            # tags: [cdc, ingestion, kafka]
    batch_patterns.yml          # tags: [batch, ingestion, azure]
```

**Conflict prevention**: Each knowledge file should declare a `supersedes` field (similar to Graphiti's temporal edges). When `clustering_guide_v2.yml` supersedes `clustering_guide.yml`, the loader resolves to the latest. This prevents contradictory guidance from reaching the LLM.

**Versioning**: Knowledge files should carry a `valid_from` / `valid_until` date range. When Snowflake releases a new feature that changes optimization guidance, the old knowledge is archived, not deleted.

### 4.2 Dynamic Context Retrieval

Today DCAG uses **static edges** — each step declares its knowledge refs in YAML. This is precise but rigid.

**Three-tier retrieval model:**

| Tier | Mechanism | When to Use | Precision | Recall |
|------|-----------|-------------|-----------|--------|
| **Static** | YAML declaration (`context.static: [clustering_guide]`) | Known requirements that never change | Very high | Low |
| **Conditional** | Step output triggers knowledge load (`if intent == "join" then load join_patterns`) | Requirements that depend on prior reasoning | High | Medium |
| **Semantic** | Embedding-based search over knowledge corpus | Open-ended steps or novel situations | Medium | High |

Implementation path:
1. **Now**: Static edges (already implemented)
2. **Next**: Conditional edges via Walker v2 (step transitions based on output values trigger different knowledge loads)
3. **Future**: Semantic retrieval via a knowledge index (LanceDB, similar to DAX's AKS index) that the ContextAssembler queries when a step declares `context.semantic: "query patterns for {table_type}"`

### 4.3 Cross-Workflow Knowledge Sharing

When the table-optimizer workflow discovers that `TRANSACTION` is loaded hourly, that fact is useful for any future workflow that touches `TRANSACTION`. Today this knowledge dies when the workflow run ends.

**Decision trace as knowledge source:**

```yaml
# Auto-generated after workflow run
decision:
  workflow: table-optimizer
  run_id: dcag-a1b2c3d4
  table: DW.RPT.TRANSACTION
  decided_at: 2026-03-12T14:30:00Z
  facts:
    - load_frequency: HOURLY
    - clustering_recommendation: SKIP
    - rationale: "Hourly loads cause micro-partition churn"
  confidence: high
  valid_until: 2026-06-12  # re-evaluate in 90 days
```

These decision traces become knowledge nodes in the graph. Future workflows can declare `context.decisions: [{table: "{{inputs.table_name}}"}]` to pull in prior decisions about the same entity. This is where Graphiti's temporal model directly applies — decisions have validity windows and can be superseded.

### 4.4 Context Graph as MCP Server

The most transformative scaling pattern: instead of pre-loading all knowledge into each step, expose DCAG's context graph as an MCP server that the LLM can query on demand.

**How it would work:**

```
MCP Server: dcag-context
  Tools:
    - dcag.search_knowledge(query: str, domain?: str) -> list[KnowledgeFile]
    - dcag.get_decisions(entity: str, workflow?: str) -> list[Decision]
    - dcag.get_persona(id: str) -> PersonaBundle
    - dcag.list_workflows(intent?: str) -> list[ManifestEntry]
    - dcag.get_step_context(workflow_id: str, step_id: str) -> ContextBundle
```

This inverts the current flow. Instead of DCAG assembling all context upfront (push), the LLM pulls context as needed during reasoning. Benefits:
- **Token efficiency**: Only the knowledge actually needed is loaded.
- **Discoverability**: The LLM can explore what knowledge exists ("what do you know about CDC patterns?").
- **Composability**: Any MCP-compatible agent (Claude, GPT, Gemini) can use DCAG's knowledge.

The risk is non-determinism — the LLM might not request critical knowledge. Mitigation: keep static edges for required context, use MCP for supplementary context.

### 4.5 Multi-Agent Context Coordination

When multiple agents work on related tasks (e.g., one optimizing a table while another refactors the dbt model that builds it), they need shared context.

**Three models:**

| Model | How It Works | Tradeoff |
|-------|-------------|----------|
| **Shared graph** | All agents read/write the same context graph | Simple, but contention risks (conflicting writes) |
| **Isolated + merge** | Each agent has its own context; merge at completion | No contention, but merge conflicts |
| **Event bus** | Agents publish decisions to a stream; others subscribe | Loose coupling, but eventual consistency |

DCAG's current architecture (single workflow, single agent) naturally extends to the event bus model. Each WorkflowRun already produces a trace. Publishing that trace to a message bus would enable other workflows to subscribe to decisions relevant to their domain. The `decision trace as knowledge source` pattern (4.3) is the foundation for this.

---

## Part 5: The Vision — Context Graph as Organizational Brain

### What a Mature Enterprise Context Graph Looks Like

```
                    ┌─────────────────────────────────────────────┐
                    │          Organizational Context Graph        │
                    ├─────────────────────────────────────────────┤
                    │                                             │
                    │  Procedural Layer (DCAG)                    │
                    │  ├── 200 workflows                         │
                    │  ├── 20 personas                           │
                    │  └── encoded expert processes               │
                    │                                             │
                    │  Knowledge Layer (Knowledge Files)          │
                    │  ├── 1000+ knowledge files (namespaced)    │
                    │  ├── semantic index for retrieval           │
                    │  └── versioned with validity windows        │
                    │                                             │
                    │  Memory Layer (Decision Traces)             │
                    │  ├── temporal facts from prior runs         │
                    │  ├── entity-indexed (table, model, team)   │
                    │  └── 90-day validity with auto-refresh     │
                    │                                             │
                    │  Semantic Layer (existing: dbt, DAX)       │
                    │  ├── metric definitions                    │
                    │  ├── model relationships                   │
                    │  └── data lineage                          │
                    │                                             │
                    │  Tool Layer (MCP + ToolRegistry)           │
                    │  ├── available tools with capabilities      │
                    │  ├── runtime degradation matrix            │
                    │  └── usage patterns and anti-patterns       │
                    │                                             │
                    └─────────────────────────────────────────────┘
```

### How It Compounds Knowledge Over Time

1. **Expert observes** a data engineer optimizing a table. Encodes the process as a DCAG workflow.
2. **Workflow runs** 50 times across different tables. Each run produces a decision trace.
3. **Decision traces accumulate** — the graph now knows that hourly tables should skip clustering, that `EVENT_DATE` is the dominant filter column for transaction tables, that tables in the `RPT` schema are typically 10-100GB.
4. **Knowledge files are refined** based on decision trace patterns. Auto-generated summaries (GraphRAG-style) identify clusters: "optimization patterns for streaming tables" vs "optimization patterns for dimension tables."
5. **New workflows reference prior decisions** — the add-column workflow checks decision traces before modifying a table that was recently optimized.
6. **Personas evolve** — the data engineer persona's heuristics are validated or updated based on outcome tracking.

### How It Scales from 1 Expert to N Agents

The path from "Sundar's expertise in YAML" to "organizational competence graph":

| Phase | Scale | Knowledge Source | DCAG Role |
|-------|-------|-----------------|-----------|
| **1. Capture** (now) | 2 workflows, 2 personas | Expert observation, hand-authored YAML | Encode and execute expert processes |
| **2. Validate** (next) | 10 workflows, 5 personas | Decision traces from production runs | Prove that encoded processes produce expert-quality outcomes |
| **3. Accumulate** (6mo) | 50 workflows, 10 personas | Decision traces + auto-generated knowledge | Cross-workflow knowledge sharing, temporal memory |
| **4. Compound** (1yr) | 200 workflows, 20 personas | Full memory layer + semantic retrieval | Context graph as MCP server, multi-agent coordination |
| **5. Autonomous** (2yr) | Self-evolving graph | Agent-proposed workflow refinements (human-approved) | Agents propose new heuristics, anti-patterns, and knowledge based on outcome data |

### The Path from DCAG (2 workflows) to Organizational Competence Graph

**Immediate (0-3 months):**
- Add conditional edges to Walker (branching based on step output)
- Add parallel step execution (independent steps run concurrently)
- Implement decision trace persistence (write traces as knowledge nodes)

**Near-term (3-6 months):**
- Build knowledge namespace hierarchy with tags and versioning
- Implement conditional context loading (dynamic knowledge based on prior step output)
- Add 8-10 more workflows covering ingestion, monitoring, incident response

**Medium-term (6-12 months):**
- Build semantic knowledge index (embedding-based retrieval for open-ended steps)
- Expose DCAG as MCP server (context graph as a queryable tool)
- Implement cross-workflow decision sharing

**Long-term (12-24 months):**
- Multi-agent coordination via event bus
- Auto-generated knowledge from decision trace clustering
- Outcome tracking: did the optimized table actually perform better? Feed back into knowledge.
- Agent-proposed workflow refinements (human-approved)

---

## Key Takeaway

DCAG occupies a genuinely novel position in the context graph landscape. While the industry has converged on memory graphs (Graphiti), retrieval graphs (GraphRAG), and orchestration graphs (LangGraph), DCAG is the only system reviewed that models **procedural expertise** — how experts work, not just what they know or remember. The headless pull model, tool gating, and persona-as-data architecture are defensible design choices validated by Temporal.io's success in general distributed systems.

The critical scaling investments are: (1) conditional edges in the Walker, (2) decision trace persistence for cross-run memory, and (3) the MCP server exposure to make the context graph a first-class tool in any agent's toolkit. These three capabilities transform DCAG from a workflow engine into an organizational competence graph.

---

## Sources

- [Zep: A Temporal Knowledge Graph Architecture for Agent Memory (arXiv)](https://arxiv.org/abs/2501.13956)
- [Graphiti: Build Real-Time Knowledge Graphs for AI Agents (GitHub)](https://github.com/getzep/graphiti)
- [Graphiti: Knowledge Graph Memory for an Agentic World (Neo4j)](https://neo4j.com/blog/developer/graphiti-knowledge-graph-memory/)
- [Microsoft GraphRAG: From Local to Global (arXiv)](https://arxiv.org/abs/2404.16130)
- [Microsoft GraphRAG Project](https://www.microsoft.com/en-us/research/project/graphrag/)
- [LangGraph: Agent Orchestration Framework](https://www.langchain.com/langgraph)
- [LangGraph Multi-Agent Orchestration Guide 2025](https://latenode.com/blog/ai-frameworks-technical-infrastructure/langgraph-multi-agent-orchestration/langgraph-multi-agent-orchestration-complete-framework-guide-architecture-analysis-2025)
- [Temporal.io: How the Platform Works](https://temporal.io/how-it-works)
- [Temporal: Beyond State Machines](https://temporal.io/blog/temporal-replaces-state-machines-for-distributed-applications)
- [Orchestrating Ambient Agents with Temporal](https://temporal.io/blog/orchestrating-ambient-agents-with-temporal)
- [CrewAI: The Open Source Multi-Agent Orchestration Framework](https://crewai.com/open-source)
- [CrewAI Framework 2025 Review](https://latenode.com/blog/ai-frameworks-technical-infrastructure/crewai-framework/crewai-framework-2025-complete-review-of-the-open-source-multi-agent-ai-platform)
- [Anthropic: Building Effective Agents](https://www.anthropic.com/research/building-effective-agents)
- [Anthropic: Advanced Tool Use](https://www.anthropic.com/engineering/advanced-tool-use)
- [Anthropic: How to Implement Tool Use](https://platform.claude.com/docs/en/agents-and-tools/tool-use/implement-tool-use)
- [Google Vertex AI Agent Builder: Enhanced Tool Governance](https://cloud.google.com/blog/products/ai-machine-learning/new-enhanced-tool-governance-in-vertex-ai-agent-builder)
- [Google: Build Multi-System Agents with Vertex AI](https://cloud.google.com/blog/products/ai-machine-learning/build-and-manage-multi-system-agents-with-vertex-ai)
- [AutoTool: Dynamic Tool Selection for Agentic Reasoning (arXiv)](https://arxiv.org/html/2512.13278v1)
- [MCP Specification 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25)
- [Neo4j: What Is Model Context Protocol (MCP)?](https://neo4j.com/blog/genai/what-is-model-context-protocol-mcp/)
- [Enhancing MCP with Context-Aware Server Collaboration (arXiv)](https://arxiv.org/html/2601.11595v2)
- [Microsoft Fabric IQ: The Semantic Foundation for Enterprise AI](https://blog.fabric.microsoft.com/en-US/blog/introducing-fabric-iq-the-semantic-foundation-for-enterprise-ai/)
- [Six Data Shifts That Will Shape Enterprise AI in 2026 (VentureBeat)](https://venturebeat.com/data/six-data-shifts-that-will-shape-enterprise-ai-in-2026/)
- [5 Ways Knowledge Graphs Are Reshaping AI Workflows (beam.ai)](https://beam.ai/agentic-insights/5-ways-knowledge-graphs-are-quietly-reshaping-ai-workflows-in-2025-2026)
- [Scaling AI Agents via Contextual Intelligence (SiliconANGLE)](https://siliconangle.com/2026/01/18/2026-data-predictions-scaling-ai-agents-via-contextual-intelligence/)
