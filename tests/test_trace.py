"""Tests for JSONL trace writer."""
import json
import tempfile
from pathlib import Path

from dcag._trace import TraceWriter


class TestTraceWriter:
    def test_write_and_consolidate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = TraceWriter(run_id="test-001", output_dir=Path(tmpdir))
            writer.record_start(workflow_id="add-column", inputs={"col": "pcid"}, config_hash="sha256:abc")
            writer.record_step(step_id="step1", mode="reason", status="completed", duration_ms=100, output={"key": "val"})
            writer.record_step(step_id="step2", mode="execute", status="completed", duration_ms=10, output="SQL text")
            writer.record_end(status="completed")

            trace = writer.consolidate()
            assert trace["run_id"] == "test-001"
            assert trace["workflow_id"] == "add-column"
            assert trace["status"] == "completed"
            assert len(trace["steps"]) == 2

    def test_jsonl_survives_partial(self):
        """JSONL file is valid even if end is never written (crash)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = TraceWriter(run_id="test-002", output_dir=Path(tmpdir))
            writer.record_start(workflow_id="test", inputs={}, config_hash="")
            writer.record_step(step_id="s1", mode="reason", status="completed", duration_ms=50, output={})
            # No record_end — simulating a crash

            # JSONL should still be readable
            jsonl_path = Path(tmpdir) / "test-002.jsonl"
            assert jsonl_path.exists()
            lines = jsonl_path.read_text().strip().split("\n")
            assert len(lines) == 2  # start + 1 step
            for line in lines:
                json.loads(line)  # each line is valid JSON
