"""JSONL streaming trace writer. Crash-resilient."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dcag._snapshot import ContextSnapshot


class TraceWriter:
    """Appends trace events as JSON Lines. Consolidates on completion."""

    def __init__(self, run_id: str, output_dir: Path):
        self._run_id = run_id
        self._path = output_dir / f"{run_id}.jsonl"
        output_dir.mkdir(parents=True, exist_ok=True)

    def record_start(self, workflow_id: str, inputs: dict, config_hash: str) -> None:
        self._append({
            "event": "start",
            "run_id": self._run_id,
            "workflow_id": workflow_id,
            "inputs": inputs,
            "config_snapshot": config_hash,
            "timestamp": self._now(),
        })

    def record_step(
        self, step_id: str, mode: str, status: str,
        duration_ms: int, output: Any,
        decision_log: dict | None = None,
        tool_calls: list[dict] | None = None,
        token_usage: dict | None = None,
        error: str | None = None,
    ) -> None:
        self._append({
            "event": "step",
            "step_id": step_id,
            "mode": mode,
            "status": status,
            "duration_ms": duration_ms,
            "output": output if not isinstance(output, str) else {"rendered": output},
            "decision_log": decision_log,
            "tool_calls": tool_calls,
            "token_usage": token_usage,
            "error": error,
            "timestamp": self._now(),
        })

    def record_end(self, status: str) -> None:
        self._append({
            "event": "end",
            "status": status,
            "timestamp": self._now(),
        })

    def consolidate(self) -> dict:
        """Read JSONL and produce a single JSON trace dict."""
        events = []
        with open(self._path) as f:
            for line in f:
                events.append(json.loads(line))

        start = next((e for e in events if e["event"] == "start"), {})
        end = next((e for e in events if e["event"] == "end"), {})
        steps = [e for e in events if e["event"] == "step"]

        return {
            "run_id": start.get("run_id", self._run_id),
            "workflow_id": start.get("workflow_id", ""),
            "status": end.get("status", "incomplete"),
            "inputs": start.get("inputs", {}),
            "started_at": start.get("timestamp", ""),
            "completed_at": end.get("timestamp"),
            "steps": steps,
            "config_snapshot": start.get("config_snapshot", ""),
        }

    def _append(self, event: dict) -> None:
        with open(self._path, "a") as f:
            f.write(json.dumps(event, default=str) + "\n")

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


class ObservabilityEvent:
    """Typed event emitted during workflow execution."""

    @staticmethod
    def step_started(step_id: str, mode: str) -> dict:
        return {"type": "step_started", "step_id": step_id, "mode": mode, "timestamp": _now()}

    @staticmethod
    def context_assembled(step_id: str, snapshot: ContextSnapshot) -> dict:
        return {"type": "context_assembled", "step_id": step_id, "snapshot": asdict(snapshot), "timestamp": _now()}

    @staticmethod
    def tool_resolved(step_id: str, requested: list[str], available: list[str]) -> dict:
        return {"type": "tool_resolved", "step_id": step_id, "requested": requested, "available": available, "timestamp": _now()}

    @staticmethod
    def request_returned(step_id: str, request_type: str) -> dict:
        return {"type": "request_returned", "step_id": step_id, "request_type": request_type, "timestamp": _now()}

    @staticmethod
    def result_recorded(step_id: str, status: str, duration_ms: int) -> dict:
        return {"type": "result_recorded", "step_id": step_id, "status": status, "duration_ms": duration_ms, "timestamp": _now()}

    @staticmethod
    def workflow_complete(run_id: str, steps_executed: int, total_ms: int) -> dict:
        return {"type": "workflow_complete", "run_id": run_id, "steps_executed": steps_executed, "total_ms": total_ms, "timestamp": _now()}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
