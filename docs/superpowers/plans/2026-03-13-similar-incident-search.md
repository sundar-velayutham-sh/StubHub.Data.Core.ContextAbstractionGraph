# Similar Incident Search — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Slack thread search for similar past incidents to the triage-ae-alert workflow, so the triage report includes "last time this happened, here's what was found and how it was fixed."

**Architecture:** Two new `reason` steps inserted after `check_prior_remediation`: `search_similar_incidents` (searches #ae-alerts and #dw-alerts via Slack MCP) and `analyze_similar_incidents` (reads matching threads, extracts structured findings). Seven existing steps get new dynamic context wiring.

**Tech Stack:** YAML workflow definition, JSON cassettes, pytest

**Spec:** `docs/superpowers/specs/2026-03-13-similar-incident-search-design.md`

---

## Chunk 1: Cassettes and Tests First

### Task 1: Create cassette files for the two new steps

Each of the 4 branch paths needs cassettes for `search_similar_incidents` and `analyze_similar_incidents`. We create them with realistic data that matches the existing cassette scenarios.

**Files:**
- Create: `tests/cassettes/triage-ae-alert-code-error/search_similar_incidents.json`
- Create: `tests/cassettes/triage-ae-alert-code-error/analyze_similar_incidents.json`
- Create: `tests/cassettes/triage-ae-alert-data-issue/search_similar_incidents.json`
- Create: `tests/cassettes/triage-ae-alert-data-issue/analyze_similar_incidents.json`
- Create: `tests/cassettes/triage-ae-alert-infrastructure/search_similar_incidents.json`
- Create: `tests/cassettes/triage-ae-alert-infrastructure/analyze_similar_incidents.json`
- Create: `tests/cassettes/triage-ae-alert-known-issue/search_similar_incidents.json`
- Create: `tests/cassettes/triage-ae-alert-known-issue/analyze_similar_incidents.json`

- [ ] **Step 1: Create `search_similar_incidents` cassette for data-issue path (has matches)**

This is the richest path — the data-issue scenario for `gpm_repeat_buyer_dim` should find a prior thread about Cole's investigation.

```json
{
  "output": {
    "matches_found": 2,
    "match_strategy": "same_model",
    "candidate_threads": [
      {
        "channel_id": "C0590MFQN1W",
        "thread_ts": "1741348200.000100",
        "match_type": "same_model",
        "preview": "gpm_repeat_buyer_dim failed in transform_pricing__daily: Duplicate row detected during DML action"
      },
      {
        "channel_id": "C0590MFQN1W",
        "thread_ts": "1741002600.000200",
        "match_type": "same_model",
        "preview": "gpm_repeat_buyer_dim test failure: unique test on repeat_buyer_key got 12 results"
      }
    ]
  },
  "decision_log": {
    "decision": "Found 2 prior incidents for same model in #ae-alerts",
    "rationale": "Channel history scan found 2 threads mentioning gpm_repeat_buyer_dim in the last 30 days",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["slack_mcp.get_channel_history"]
  },
  "tool_calls": [
    {"tool": "slack_mcp.get_channel_history", "input": "channel=C0590MFQN1W, limit=100"},
    {"tool": "slack_mcp.get_channel_history", "input": "channel=C040SRYF9HS, limit=100"}
  ],
  "token_usage": {"prompt": 3000, "completion": 800}
}
```

Write to: `tests/cassettes/triage-ae-alert-data-issue/search_similar_incidents.json`

- [ ] **Step 2: Create `analyze_similar_incidents` cassette for data-issue path (has incidents)**

```json
{
  "output": {
    "similar_incidents_found": true,
    "incidents": [
      {
        "model_name": "gpm_repeat_buyer_dim",
        "error_type": "duplicate_row",
        "root_cause": "Stale anon_to_resolved_user mapping (3.5-day refresh gap) caused inconsistent session counts, producing duplicate SCD2 rows",
        "resolution": "Manual DELETE of bad rows, then rerun via on_demand_dag from affected date (3/08 -> 3/12)",
        "resolved_by": "Cole Romano",
        "time_to_resolve_hours": 4.5,
        "relevance": "high"
      },
      {
        "model_name": "gpm_repeat_buyer_dim",
        "error_type": "test_failure",
        "root_cause": "Volume filter threshold was static, excluding users who crossed the boundary mid-period",
        "resolution": "Made high-volume filter dynamic based on number of days in incremental run",
        "resolved_by": "Cole Romano",
        "time_to_resolve_hours": 2.0,
        "relevance": "medium"
      }
    ],
    "summary": "This model has had 2 similar incidents in the last 30 days, both involving SCD2 data integrity. The most relevant: a 3.5-day refresh gap in anon_to_resolved_user caused stale identity mappings, producing duplicate rows. Cole Romano resolved it by manually cleaning bad rows and rerunning from the affected date via on_demand_dag. A retry alone will NOT fix this pattern."
  },
  "decision_log": {
    "decision": "2 similar incidents found with high relevance",
    "rationale": "Both threads describe the same model with SCD2-related failures. The duplicate_row thread is a direct match for the current alert.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["slack_mcp.get_thread_replies"]
  },
  "tool_calls": [
    {"tool": "slack_mcp.get_thread_replies", "input": "channel=C0590MFQN1W, ts=1741348200.000100"},
    {"tool": "slack_mcp.get_thread_replies", "input": "channel=C0590MFQN1W, ts=1741002600.000200"}
  ],
  "token_usage": {"prompt": 5000, "completion": 1200}
}
```

Write to: `tests/cassettes/triage-ae-alert-data-issue/analyze_similar_incidents.json`

- [ ] **Step 3: Create `search_similar_incidents` cassette for code-error path (no matches)**

The code-error scenario (`cs_chatbot_conversation_agg`) has no prior incidents.

```json
{
  "output": {
    "matches_found": 0,
    "match_strategy": "none",
    "candidate_threads": []
  },
  "decision_log": {
    "decision": "No similar incidents found",
    "rationale": "Scanned #ae-alerts and #dw-alerts history (200 messages each). No messages mention cs_chatbot_conversation_agg. Broadened to error pattern 'invalid identifier' — no relevant threads found.",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": ["slack_mcp.get_channel_history"]
  },
  "tool_calls": [
    {"tool": "slack_mcp.get_channel_history", "input": "channel=C0590MFQN1W, limit=100"},
    {"tool": "slack_mcp.get_channel_history", "input": "channel=C040SRYF9HS, limit=100"}
  ],
  "token_usage": {"prompt": 3000, "completion": 400}
}
```

Write to: `tests/cassettes/triage-ae-alert-code-error/search_similar_incidents.json`

- [ ] **Step 4: Create `analyze_similar_incidents` cassette for code-error path (no matches — short circuit)**

```json
{
  "output": {
    "similar_incidents_found": false,
    "incidents": [],
    "summary": "No similar incidents found in the last 30 days."
  },
  "decision_log": {
    "decision": "Short-circuited — no candidate threads to analyze",
    "rationale": "search_similar_incidents returned 0 candidates, skipping thread analysis",
    "alternatives_considered": [],
    "confidence": "high",
    "informed_by": []
  },
  "tool_calls": [],
  "token_usage": {"prompt": 500, "completion": 100}
}
```

Write to: `tests/cassettes/triage-ae-alert-code-error/analyze_similar_incidents.json`

- [ ] **Step 5: Create cassettes for infrastructure path (no matches)**

Copy the code-error cassettes (no matches) to infrastructure path — `seller_event_day_listing_agg` is a new model with no prior incidents.

Write to:
- `tests/cassettes/triage-ae-alert-infrastructure/search_similar_incidents.json`
- `tests/cassettes/triage-ae-alert-infrastructure/analyze_similar_incidents.json`

Same content as code-error cassettes, but update the `rationale` to mention `seller_event_day_listing_agg` and `internal_error` / `timeout` as the broadened search pattern.

- [ ] **Step 6: Create cassettes for known-issue path (1 match via error pattern)**

The known-issue scenario (`apex_participation_matches_participation_fact_enriched_12`) should find a match via error pattern (Pass 2) — a different model had a similar recurring transient failure.

`search_similar_incidents.json`:
```json
{
  "output": {
    "matches_found": 1,
    "match_strategy": "same_error_pattern",
    "candidate_threads": [
      {
        "channel_id": "C0590MFQN1W",
        "thread_ts": "1740657000.000300",
        "match_type": "same_error_pattern",
        "preview": "listing_event_day_agg test failure: Got 891 results, configured to fail if != 0 — recurring transient, suppressing"
      }
    ]
  },
  "decision_log": {
    "decision": "Found 1 similar incident via error pattern broadening",
    "rationale": "No threads mention apex_participation_matches_participation_fact_enriched_12. Broadened to 'Got N results, configured to fail' pattern — found 1 match for different model with same recurring transient pattern.",
    "alternatives_considered": [],
    "confidence": "medium",
    "informed_by": ["slack_mcp.get_channel_history"]
  },
  "tool_calls": [
    {"tool": "slack_mcp.get_channel_history", "input": "channel=C0590MFQN1W, limit=100"},
    {"tool": "slack_mcp.get_channel_history", "input": "channel=C040SRYF9HS, limit=100"}
  ],
  "token_usage": {"prompt": 3000, "completion": 600}
}
```

`analyze_similar_incidents.json`:
```json
{
  "output": {
    "similar_incidents_found": true,
    "incidents": [
      {
        "model_name": "listing_event_day_agg",
        "error_type": "recurring_transient",
        "root_cause": "Timing-dependent test that catches in-flight data during hourly refresh window",
        "resolution": "Suppressed alert — added to known-transient list, passes on next scheduled run",
        "resolved_by": "unknown",
        "time_to_resolve_hours": null,
        "relevance": "medium"
      }
    ],
    "summary": "Found 1 similar incident via error pattern match: listing_event_day_agg had the same recurring test failure pattern. It was identified as a timing-dependent transient and suppressed. This suggests the current alert may also be a transient that self-resolves."
  },
  "decision_log": {
    "decision": "1 similar incident found via error pattern with medium relevance",
    "rationale": "Different model but same 'Got N results' test failure pattern, both classified as recurring transient",
    "alternatives_considered": [],
    "confidence": "medium",
    "informed_by": ["slack_mcp.get_thread_replies"]
  },
  "tool_calls": [
    {"tool": "slack_mcp.get_thread_replies", "input": "channel=C0590MFQN1W, ts=1740657000.000300"}
  ],
  "token_usage": {"prompt": 2500, "completion": 600}
}
```

Write both to `tests/cassettes/triage-ae-alert-known-issue/`

- [ ] **Step 7: Commit cassettes**

```bash
git add tests/cassettes/triage-ae-alert-code-error/search_similar_incidents.json \
        tests/cassettes/triage-ae-alert-code-error/analyze_similar_incidents.json \
        tests/cassettes/triage-ae-alert-data-issue/search_similar_incidents.json \
        tests/cassettes/triage-ae-alert-data-issue/analyze_similar_incidents.json \
        tests/cassettes/triage-ae-alert-infrastructure/search_similar_incidents.json \
        tests/cassettes/triage-ae-alert-infrastructure/analyze_similar_incidents.json \
        tests/cassettes/triage-ae-alert-known-issue/search_similar_incidents.json \
        tests/cassettes/triage-ae-alert-known-issue/analyze_similar_incidents.json
git commit -m "test: add cassettes for search_similar_incidents and analyze_similar_incidents steps"
```

---

### Task 2: Update test step constants and assertions

**Files:**
- Modify: `tests/test_e2e_triage_ae_alert.py`

- [ ] **Step 1: Update all 4 step-order constants to include the 2 new steps**

Insert `"search_similar_incidents"` and `"analyze_similar_incidents"` after `"check_prior_remediation"` in each list.

Before (line 27-38, CODE_ERROR_STEPS as example):
```python
CODE_ERROR_STEPS = [
    "parse_alert",
    "check_failure_history",
    "check_prior_remediation",
    "check_cascade",
    ...
]
```

After:
```python
CODE_ERROR_STEPS = [
    "parse_alert",
    "check_failure_history",
    "check_prior_remediation",
    "search_similar_incidents",
    "analyze_similar_incidents",
    "check_cascade",
    "get_model_context",
    "classify_alert",
    "diagnose_code_error",
    "determine_resolution",
    "generate_triage_report",
    "post_to_thread",
]
```

Apply the same change to `DATA_ISSUE_STEPS`, `INFRASTRUCTURE_STEPS`, and `KNOWN_ISSUE_STEPS`. Each goes from 10 to 12 entries.

- [ ] **Step 2: Update step-count assertions from 10 to 12, rename test methods**

4 test methods need updating — change assertion AND rename methods to be count-agnostic:

- `TestTriageCodeError.test_code_error_path_has_9_steps` → rename to `test_code_error_path_step_count`, change to `assert len(steps_executed) == 12`
- `TestTriageDataIssue.test_data_issue_path_has_9_steps` → rename to `test_data_issue_path_step_count`, change to `assert len(steps_executed) == 12`
- `TestTriageInfrastructure.test_infrastructure_path_has_9_steps` → rename to `test_infrastructure_path_step_count`, change to `assert len(steps_executed) == 12`
- `TestTriageKnownIssue.test_known_issue_path_has_9_steps` → rename to `test_known_issue_path_step_count`, change to `assert len(steps_executed) == 12`

- [ ] **Step 3: Update the docstring**

Line 1-9: Change "full 9-step workflow" to "full workflow" and update the comment about the step count.

- [ ] **Step 4: Run tests to verify they FAIL (workflow YAML not updated yet)**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_e2e_triage_ae_alert.py -v --tb=short 2>&1 | head -60
```

Expected: All tests FAIL because the workflow YAML still has 10 steps — the engine won't emit `search_similar_incidents` or `analyze_similar_incidents`, so step order won't match.

- [ ] **Step 5: Commit test updates**

```bash
git add tests/test_e2e_triage_ae_alert.py
git commit -m "test: update triage E2E tests for 2 new similar-incident steps (will fail until workflow is updated)"
```

---

## Chunk 2: Workflow YAML Changes

### Task 3: Add the two new steps to the workflow YAML

**Files:**
- Modify: `content/workflows/triage-ae-alert.yml`

- [ ] **Step 1: Add `search_similar_incidents` step after `check_prior_remediation`**

Insert after the `check_prior_remediation` step (after line 144 in the current file), before `check_cascade`:

```yaml
    # Step: Search Slack for similar past incidents
    - id: search_similar_incidents
      mode: reason
      instruction: |
        Search #ae-alerts and #dw-alerts Slack channels for similar past incidents
        in the last 30 days. Use a two-pass strategy:

        PASS 1 — Same model:
        Pull channel history from both channels (limit 200 messages each).
        Scan messages for any mention of the model name: '{model_name}'.
        Identify messages that are alert threads (have reply_count > 0).

        PASS 2 — Same error pattern (only if Pass 1 returns < 2 matches):
        Re-scan the already-fetched messages for error pattern keywords from
        the current error message (e.g., 'duplicate_row', 'invalid identifier',
        'timeout', 'internal error', 'test failure').
        No additional API calls needed — reuse the history from Pass 1.

        Cap at 5 candidate threads maximum. For each candidate, capture the
        channel_id, thread_ts (for later retrieval), match_type, and a preview
        of the first message.

        Channel IDs:
        - #ae-alerts: C0590MFQN1W
        - #dw-alerts: C040SRYF9HS
      tools:
        - name: slack_mcp.get_channel_history
          instruction: "Pull channel message history (limit 100 per call, paginate if needed)"
          usage_pattern: |
            1. Get #ae-alerts history: channel=C0590MFQN1W, limit=100
            2. Get #dw-alerts history: channel=C040SRYF9HS, limit=100
            3. Paginate with cursor if more messages available (up to 200 per channel)
      context:
        static: [on_call_conventions]
        dynamic:
          - from: parse_alert
            select: [model_name, error_message]
          - from: check_failure_history
            select: pattern
      output_schema:
        type: object
        required: [matches_found, candidate_threads]
        properties:
          matches_found:
            type: integer
          match_strategy:
            type: string
            enum: [same_model, same_error_pattern, both, none]
          candidate_threads:
            type: array
            items:
              type: object
              required: [channel_id, thread_ts, match_type, preview]
      budget:
        max_llm_turns: 6
        max_tokens: 10000
```

- [ ] **Step 2: Add `analyze_similar_incidents` step after `search_similar_incidents`**

Insert immediately after the step above:

```yaml
    # Step: Analyze matching Slack threads for root cause and resolution
    - id: analyze_similar_incidents
      mode: reason
      instruction: |
        Analyze the candidate threads found by search_similar_incidents.

        SHORT-CIRCUIT: If candidate_threads is empty, skip thread fetching
        entirely. Return similar_incidents_found=false, empty incidents array,
        and summary="No similar incidents found in the last 30 days."

        For each candidate thread:
        1. Read the full thread replies using get_thread_replies
        2. Extract: model_name, error_type, root_cause, resolution
        3. Extract if available: resolved_by (Slack display name), time_to_resolve_hours
        4. Assign relevance:
           - high: same model + same error type as current alert
           - medium: same model + different error type
           - low: different model + same error pattern

        Rank incidents by relevance (high first).

        Write a one-paragraph summary synthesizing the findings for downstream
        steps. Focus on: what was the root cause, how was it fixed, and is the
        current alert likely the same issue.
      tools:
        - name: slack_mcp.get_thread_replies
          instruction: "Read full thread conversation for each candidate"
          usage_pattern: |
            1. For each candidate: channel={channel_id}, ts={thread_ts}
      context:
        static: [troubleshooting_patterns]
        dynamic:
          - from: parse_alert
            select: [model_name, error_message]
          - from: search_similar_incidents
            select: candidate_threads
      output_schema:
        type: object
        required: [similar_incidents_found, incidents, summary]
        properties:
          similar_incidents_found:
            type: boolean
          incidents:
            type: array
            items:
              type: object
              required: [model_name, error_type, root_cause, resolution, relevance]
          summary:
            type: string
      budget:
        max_llm_turns: 5
        max_tokens: 12000
```

- [ ] **Step 3: Run tests to verify they still FAIL (downstream wiring not done yet)**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_e2e_triage_ae_alert.py::TestTriageCodeError::test_workflow_takes_code_error_path -v --tb=short
```

Expected: Should now get further (the engine emits the new steps) but may fail on cassette loading or context assembly since downstream wiring isn't done yet. If it passes, great — move on.

- [ ] **Step 4: Commit new steps**

```bash
git add content/workflows/triage-ae-alert.yml
git commit -m "feat: add search_similar_incidents and analyze_similar_incidents steps to triage workflow"
```

---

### Task 4: Wire downstream steps to consume similar-incident context

**Files:**
- Modify: `content/workflows/triage-ae-alert.yml`

- [ ] **Step 1: Add dynamic context to `classify_alert`**

Find the `classify_alert` step's `context.dynamic` section. Add at the end of the dynamic list:

```yaml
          - from: analyze_similar_incidents
            select: [similar_incidents_found, summary]
```

- [ ] **Step 2: Add dynamic context to all 4 diagnose steps**

For each of `diagnose_code_error`, `diagnose_data_issue`, `diagnose_infrastructure`, `diagnose_known_issue`, add to their `context.dynamic`:

```yaml
          - from: analyze_similar_incidents
            select: [incidents, summary]
```

- [ ] **Step 3: Add dynamic context to `determine_resolution`**

Add to `determine_resolution`'s `context.dynamic`:

```yaml
          - from: analyze_similar_incidents
            select: [similar_incidents_found, incidents, summary]
```

- [ ] **Step 4: Add dynamic context to `generate_triage_report`**

Add to `generate_triage_report`'s `context.dynamic`:

```yaml
          - from: analyze_similar_incidents
```

- [ ] **Step 5: Update `generate_triage_report` instruction to include Prior Incidents section**

Add to the instruction text, after the existing report format:

```yaml
        Include a "Prior Similar Incidents" section in the investigation_report:
        - If similar_incidents_found is true, list each incident with model name,
          date, root cause, resolution, and who resolved it
        - If false, write "No similar incidents found in the last 30 days."

        In the thread_summary, if similar incidents were found, add a line:
        "Prior: {N} similar incident(s) found — see investigation for details"
```

- [ ] **Step 6: Run ALL tests**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_e2e_triage_ae_alert.py -v --tb=short
```

Expected: All 4 test classes pass. All step-order assertions match. Step counts are 12.

- [ ] **Step 7: Commit downstream wiring**

```bash
git add content/workflows/triage-ae-alert.yml
git commit -m "feat: wire similar-incident context into classify, diagnose, resolution, and report steps"
```

---

## Chunk 3: Conformance Spec and Test Updates

### Task 5: Update the conformance spec YAML

**Files:**
- Modify: `content/workflows/triage-ae-alert.test.yml`

- [ ] **Step 1: Add `search_similar_incidents` entry to conformance spec**

Insert after the `check_prior_remediation` entry (after line 33), before `check_cascade`:

```yaml
    search_similar_incidents:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - on_call_conventions
      tools_include:
        - slack_mcp.get_channel_history
      dynamic_refs_from:
        - parse_alert
        - check_failure_history
      has_instruction: true
```

- [ ] **Step 2: Add `analyze_similar_incidents` entry to conformance spec**

Insert immediately after:

```yaml
    analyze_similar_incidents:
      type: ReasonRequest
      persona: analytics_engineer
      knowledge_includes:
        - troubleshooting_patterns
      tools_include:
        - slack_mcp.get_thread_replies
      dynamic_refs_from:
        - parse_alert
        - search_similar_incidents
      has_instruction: true
```

- [ ] **Step 3: Update downstream step `dynamic_refs_from` entries**

Add `analyze_similar_incidents` to `dynamic_refs_from` for these existing entries:

- `classify_alert`: add `- analyze_similar_incidents`
- `diagnose_code_error`: add `- analyze_similar_incidents`
- `diagnose_data_issue`: add `- analyze_similar_incidents`
- `diagnose_infrastructure`: add `- analyze_similar_incidents`
- `diagnose_known_issue`: add `- analyze_similar_incidents`
- `determine_resolution`: add `- analyze_similar_incidents`
- `generate_triage_report`: add `- analyze_similar_incidents`

- [ ] **Step 4: Commit conformance spec**

```bash
git add content/workflows/triage-ae-alert.test.yml
git commit -m "test: update conformance spec with search_similar_incidents and analyze_similar_incidents"
```

---

### Task 6: Update the conformance test file

**Files:**
- Modify: `tests/test_conformance_triage_ae_alert.py`

- [ ] **Step 1: Add the 2 new steps to `code_error_path` list**

Insert after `"check_prior_remediation"` (line 56), before `"check_cascade"`:

```python
        code_error_path = [
            "parse_alert",
            "check_failure_history",
            "check_prior_remediation",
            "search_similar_incidents",
            "analyze_similar_incidents",
            "check_cascade",
            "get_model_context",
            "classify_alert",
            "diagnose_code_error",
            "determine_resolution",
            "generate_triage_report",
            "post_to_thread",
        ]
```

- [ ] **Step 2: Add mock outputs for the 2 new steps to `step_outputs` dict**

Insert after the `check_prior_remediation` entry (after line 89):

```python
            "search_similar_incidents": {
                "matches_found": 0,
                "match_strategy": "none",
                "candidate_threads": [],
            },
            "analyze_similar_incidents": {
                "similar_incidents_found": False,
                "incidents": [],
                "summary": "No similar incidents found in the last 30 days.",
            },
```

- [ ] **Step 3: Run conformance tests**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_conformance_triage_ae_alert.py -v --tb=short
```

Expected: Both conformance tests pass — `test_all_steps_on_code_error_path_match_spec` and `test_conformance_covers_all_steps`.

- [ ] **Step 4: Commit conformance test updates**

```bash
git add tests/test_conformance_triage_ae_alert.py
git commit -m "test: update conformance test with 2 new similar-incident steps"
```

---

## Chunk 4: New E2E Tests for Similar-Incident Behavior

### Task 7: Add E2E tests that verify similar-incident outputs

**Files:**
- Modify: `tests/test_e2e_triage_ae_alert.py`

- [ ] **Step 1: Add test to `TestTriageDataIssue` for similar incidents found**

```python
    def test_similar_incidents_found_in_data_issue_path(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        sim = reason_outputs["analyze_similar_incidents"]
        assert sim["similar_incidents_found"] is True
        assert len(sim["incidents"]) == 2
        assert sim["incidents"][0]["relevance"] == "high"
        assert "on_demand_dag" in sim["incidents"][0]["resolution"]

    def test_search_found_candidates_via_same_model(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        search = reason_outputs["search_similar_incidents"]
        assert search["matches_found"] == 2
        assert search["match_strategy"] == "same_model"
```

- [ ] **Step 2: Add test to `TestTriageCodeError` for no similar incidents**

```python
    def test_no_similar_incidents_for_code_error(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        sim = reason_outputs["analyze_similar_incidents"]
        assert sim["similar_incidents_found"] is False
        assert sim["incidents"] == []
        assert "No similar incidents" in sim["summary"]
```

- [ ] **Step 3: Add test to `TestTriageKnownIssue` for error-pattern match**

```python
    def test_similar_incident_found_via_error_pattern(self):
        _, _, reason_outputs = run_workflow(self.CASSETTE_DIR, self.INPUTS, self.REASON_STEPS)
        search = reason_outputs["search_similar_incidents"]
        assert search["match_strategy"] == "same_error_pattern"
        sim = reason_outputs["analyze_similar_incidents"]
        assert sim["similar_incidents_found"] is True
        assert sim["incidents"][0]["relevance"] == "medium"
        assert sim["incidents"][0]["model_name"] != "apex_participation_matches_participation_fact_enriched_12"
```

- [ ] **Step 4: Run ALL tests**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/test_e2e_triage_ae_alert.py -v --tb=short
```

Expected: All tests pass, including the new similar-incident assertions.

- [ ] **Step 5: Commit new tests**

```bash
git add tests/test_e2e_triage_ae_alert.py
git commit -m "test: add assertions for similar-incident search across all branch paths"
```

---

## Chunk 5: Run Full Test Suite and Final Verification

### Task 8: Run the full test suite and verify nothing is broken

**Files:** None (verification only)

- [ ] **Step 1: Run the full test suite**

```bash
cd /Users/sundar.velayutham/code/StubHub.Data.Core.ContextAbstractionGraph
python -m pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected: All tests pass. No regressions in other workflows (add-column, fix-model-bug, etc.).

- [ ] **Step 2: Run the conformance tests specifically**

```bash
python -m pytest tests/test_conformance_triage_ae_alert.py -v --tb=short
```

Expected: Conformance tests pass. The workflow structure is still valid after adding 2 steps.

- [ ] **Step 3: Verify workflow loads cleanly**

```bash
python -c "
from pathlib import Path
from dcag import DCAGEngine
engine = DCAGEngine(content_dir=Path('content'))
wf = engine._registry.get_workflow('triage-ae-alert')
print(f'Steps: {len(wf.steps)}')
print(f'Step IDs: {[s.id for s in wf.steps]}')
assert len(wf.steps) == 15  # was 13 (all defined steps incl. 4 diagnose branches), now 15 with 2 new steps
print('OK')
"
```

- [ ] **Step 4: Final commit with all passing**

Only if any fixups were needed during verification:

```bash
git add content/workflows/ tests/
git commit -m "fix: address test/workflow issues found during verification"
```
