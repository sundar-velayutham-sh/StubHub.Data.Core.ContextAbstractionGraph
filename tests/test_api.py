"""Unit tests for the DCAG REST API.

Uses FastAPI's TestClient to verify the step-at-a-time enforcement:
- List workflows
- Start a run (get first step)
- Submit results sequentially for all 9 table-optimizer steps
- Verify completed status and full trace
- Error cases: 404 unknown run, 409 wrong step_id
"""
import os

os.environ.setdefault("DCAG_API_USER", "test-user")
os.environ.setdefault("DCAG_API_PASS", "test-pass")

import base64

from starlette.testclient import TestClient

from dcag.api import app

client = TestClient(app)
AUTH_HEADER = {"Authorization": "Basic " + base64.b64encode(b"test-user:test-pass").decode()}

# All 9 steps in table-optimizer execution order
EXPECTED_STEPS = [
    "identify_table",
    "detect_load_frequency",
    "analyze_query_patterns",
    "assess_clustering",
    "check_partitioning",
    "analyze_materialization",
    "show_recommendations",
    "generate_report",
    "apply_changes",
]

# Minimal mock outputs for each step (enough to satisfy downstream context refs)
MOCK_OUTPUTS = {
    "identify_table": {
        "table_fqn": "DW.RPT.TRANSACTION",
        "row_count": 1000000,
        "size_bytes": 500000000,
        "columns": [{"name": "EVENT_DATE", "type": "DATE"}, {"name": "GMS", "type": "NUMBER"}],
        "storage_metrics": {"active_bytes": 400000000, "time_travel_bytes": 50000000},
    },
    "detect_load_frequency": {
        "load_frequency": "DAILY",
        "avg_runs_per_day": 1,
        "skip_clustering": False,
        "is_dbt_managed": True,
        "tags": ["daily"],
    },
    "analyze_query_patterns": {
        "query_count": 250,
        "filter_columns": [
            {"column": "EVENT_DATE", "frequency": 0.85, "pattern": "range_scan"},
        ],
        "access_patterns": [{"type": "range_scan", "columns": ["EVENT_DATE"]}],
        "avg_prune_ratio": 0.45,
    },
    "assess_clustering": {
        "current_clustering": "NONE",
        "optimal_clustering": ["EVENT_DATE"],
        "clustering_depth": 0,
        "recommendation": "Add clustering key on EVENT_DATE",
    },
    "check_partitioning": {
        "total_partitions": 5000,
        "avg_partition_size": 100000,
        "pruning_efficiency": 0.45,
        "recommendation": "Clustering on EVENT_DATE would improve pruning to ~85%",
    },
    "analyze_materialization": {
        "current_materialization": "table",
        "recommended_materialization": "incremental",
        "is_dbt_managed": True,
        "rationale": "Table is large and loaded daily — incremental reduces rebuild time",
    },
    "show_recommendations": {
        "approved": True,
    },
    "generate_report": {
        "strategy": "CLUSTER_BY",
        "report": "Table would benefit from clustering on EVENT_DATE",
        "implementation_sql": "ALTER TABLE DW.RPT.TRANSACTION CLUSTER BY (EVENT_DATE);",
        "expected_improvement": "Estimated 40% scan reduction",
        "caveats": ["Monitor after applying — recluster may take time on large tables"],
    },
    "apply_changes": {
        "approved": True,
    },
}


class TestListWorkflows:
    def test_returns_list_with_table_optimizer(self):
        resp = client.get("/api/v1/workflows", headers=AUTH_HEADER)
        assert resp.status_code == 200
        workflows = resp.json()
        assert isinstance(workflows, list)
        ids = [w["id"] for w in workflows]
        assert "table-optimizer" in ids


class TestStartRun:
    def test_starts_and_returns_first_step(self):
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "table-optimizer",
            "inputs": {"table_name": "TRANSACTION"},
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "run_id" in data
        assert data["status"] == "running"
        assert data["step"] is not None
        assert data["step"]["step_id"] == "identify_table"
        assert data["step"]["mode"] == "reason"
        assert data["progress"]["completed_steps"] == 0
        assert data["progress"]["total_steps"] == 9

    def test_unknown_workflow_returns_404(self):
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "nonexistent-workflow",
            "inputs": {},
        })
        assert resp.status_code == 404


class TestSubmitResults:
    def test_sequential_step_submission(self):
        """Walk through all 9 steps, submitting mock results one at a time."""
        # Start the run
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "table-optimizer",
            "inputs": {"table_name": "TRANSACTION"},
        })
        assert resp.status_code == 200
        data = resp.json()
        run_id = data["run_id"]

        # Walk through each step
        for i, step_id in enumerate(EXPECTED_STEPS):
            if i == 0:
                # First step comes from start_run
                assert data["step"]["step_id"] == step_id
            else:
                # Subsequent steps come from submit_result
                assert data["step"]["step_id"] == step_id, (
                    f"Expected step '{step_id}' at position {i}, "
                    f"got '{data['step']['step_id']}'"
                )

            # Submit result for current step
            resp = client.post(f"/api/v1/runs/{run_id}/results", headers=AUTH_HEADER, json={
                "step_id": step_id,
                "output": MOCK_OUTPUTS[step_id],
            })
            assert resp.status_code == 200
            data = resp.json()

            if i < len(EXPECTED_STEPS) - 1:
                # Not the last step — should have a next step
                assert data["status"] == "running"
                assert data["step"] is not None
                assert data["progress"]["completed_steps"] == i + 1
            else:
                # Last step — should be completed
                assert data["status"] == "completed"
                assert data["step"] is None
                assert data["progress"]["completed_steps"] == 9

    def test_completed_run_has_full_trace(self):
        """After completing all steps, trace should contain all 9 steps."""
        # Start and complete the run
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "table-optimizer",
            "inputs": {"table_name": "TRANSACTION"},
        })
        run_id = resp.json()["run_id"]

        for step_id in EXPECTED_STEPS:
            client.post(f"/api/v1/runs/{run_id}/results", headers=AUTH_HEADER, json={
                "step_id": step_id,
                "output": MOCK_OUTPUTS[step_id],
            })

        # Get the run status
        resp = client.get(f"/api/v1/runs/{run_id}", headers=AUTH_HEADER)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "completed"
        trace_steps = data["trace"]["steps"]
        assert len(trace_steps) == 9
        trace_ids = [s["step_id"] for s in trace_steps]
        assert trace_ids == EXPECTED_STEPS


class TestErrorCases:
    def test_unknown_run_id_returns_404(self):
        resp = client.post("/api/v1/runs/bad-run-id/results", headers=AUTH_HEADER, json={
            "step_id": "identify_table",
            "output": {},
        })
        assert resp.status_code == 404

    def test_wrong_step_id_returns_409(self):
        # Start a run
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "table-optimizer",
            "inputs": {"table_name": "TRANSACTION"},
        })
        run_id = resp.json()["run_id"]

        # Try to submit for the WRONG step (skip ahead)
        resp = client.post(f"/api/v1/runs/{run_id}/results", headers=AUTH_HEADER, json={
            "step_id": "detect_load_frequency",
            "output": {},
        })
        assert resp.status_code == 409
        assert "Expected step 'identify_table'" in resp.json()["detail"]

    def test_get_unknown_run_returns_404(self):
        resp = client.get("/api/v1/runs/nonexistent", headers=AUTH_HEADER)
        assert resp.status_code == 404

    def test_submit_after_completed_returns_409(self):
        """Submitting a result after the run is completed should return 409."""
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "table-optimizer",
            "inputs": {"table_name": "TRANSACTION"},
        })
        run_id = resp.json()["run_id"]

        # Complete all steps
        for step_id in EXPECTED_STEPS:
            client.post(f"/api/v1/runs/{run_id}/results", headers=AUTH_HEADER, json={
                "step_id": step_id,
                "output": MOCK_OUTPUTS[step_id],
            })

        # Try to submit again
        resp = client.post(f"/api/v1/runs/{run_id}/results", headers=AUTH_HEADER, json={
            "step_id": "identify_table",
            "output": {},
        })
        assert resp.status_code == 409


class TestStepSerialization:
    def test_reason_step_has_expected_fields(self):
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "table-optimizer",
            "inputs": {"table_name": "TRANSACTION"},
        })
        step = resp.json()["step"]
        assert step["mode"] == "reason"
        assert "instruction" in step
        assert "tools" in step
        assert isinstance(step["tools"], list)
        assert "context" in step
        assert "static" in step["context"]
        assert "dynamic" in step["context"]
        assert "output_schema" in step
        assert "budget" in step
        assert "max_llm_turns" in step["budget"]
        assert "max_tokens" in step["budget"]

    def test_delegate_step_has_expected_fields(self):
        """Walk to show_recommendations (step 6) and verify delegate format."""
        resp = client.post("/api/v1/runs", headers=AUTH_HEADER, json={
            "workflow_id": "table-optimizer",
            "inputs": {"table_name": "TRANSACTION"},
        })
        run_id = resp.json()["run_id"]

        # Submit first 6 reason steps to reach the delegate step
        reason_steps = EXPECTED_STEPS[:6]
        for step_id in reason_steps:
            resp = client.post(f"/api/v1/runs/{run_id}/results", headers=AUTH_HEADER, json={
                "step_id": step_id,
                "output": MOCK_OUTPUTS[step_id],
            })

        step = resp.json()["step"]
        assert step["mode"] == "delegate"
        assert step["step_id"] == "show_recommendations"
        assert "capability" in step
        assert "requires_approval" in step
        assert step["requires_approval"] is True
        assert "inputs" in step
