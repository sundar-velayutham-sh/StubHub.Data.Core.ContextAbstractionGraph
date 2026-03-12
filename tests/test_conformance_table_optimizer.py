"""Conformance tests for table-optimizer workflow.

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


class TestTableOptimizerConformance:
    """Validate that table-optimizer assembles correct context per step."""

    WORKFLOW_ID = "table-optimizer"
    INPUTS = {"table_name": "TRANSACTION", "database": "DW", "schema": "RPT"}

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
                    "identify_table": {
                        "table_fqn": "DW.RPT.TRANSACTION",
                        "row_count": 150000000,
                        "size_bytes": 12500000000,
                        "columns": [],
                        "storage_metrics": {"active_bytes": 12500000000},
                    },
                    "detect_load_frequency": {
                        "load_frequency": "DAILY",
                        "avg_runs_per_day": 3.0,
                        "skip_clustering": False,
                        "is_dbt_managed": True,
                        "tags": ["core", "daily"],
                    },
                    "analyze_query_patterns": {
                        "query_count": 1842,
                        "filter_columns": [{"name": "EVENT_DATE", "frequency_pct": 92, "pattern": "range_scan"}],
                        "access_patterns": {"range_scans": 85, "point_lookups": 15},
                        "avg_prune_ratio": 0.35,
                    },
                    "assess_clustering": {
                        "current_clustering": None,
                        "optimal_clustering": ["EVENT_DATE", "VENUE_ID"],
                        "clustering_depth": 4.2,
                        "recommendation": "CLUSTER_BY",
                    },
                    "check_partitioning": {
                        "total_partitions": 8500,
                        "avg_partition_size": 1470588,
                        "pruning_efficiency": 0.35,
                        "recommendation": "Poor pruning",
                    },
                    "analyze_materialization": {
                        "current_materialization": "incremental",
                        "recommended_materialization": "incremental",
                        "is_dbt_managed": True,
                        "rationale": "Keep incremental",
                    },
                    "generate_report": {
                        "strategy": "CLUSTER_BY",
                        "report": {},
                        "implementation_sql": "ALTER TABLE DW.RPT.TRANSACTION CLUSTER BY (EVENT_DATE, VENUE_ID);",
                        "expected_improvement": {"scan_reduction_pct": 55},
                        "caveats": ["dbt-managed table"],
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
