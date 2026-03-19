"""DCAG REST API — FastAPI wrapper for Level 2 step-at-a-time enforcement.

Shift gets ONE step at a time and cannot skip ahead. The API is a thin wrapper
around the existing DCAGEngine — all workflow logic lives in the engine.
"""
from __future__ import annotations

import logging
import os
import secrets
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

from dcag.engine import DCAGEngine, WorkflowRun
from dcag.types import (
    DelegateRequest,
    ExecuteScriptRequest,
    ExecuteTemplateRequest,
    ReasonRequest,
    StepSuccess,
)

logger = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────

CONTENT_DIR = Path(__file__).resolve().parent.parent.parent / "content"

app = FastAPI(title="DCAG API", version="0.1.0")

# ── Basic auth (set DCAG_API_USER / DCAG_API_PASS env vars) ──
_security = HTTPBasic(auto_error=False)
API_USER = os.environ.get("DCAG_API_USER")
API_PASS = os.environ.get("DCAG_API_PASS")

if not API_USER or not API_PASS:
    import warnings
    warnings.warn(
        "DCAG_API_USER and DCAG_API_PASS not set — API auth is DISABLED. "
        "Set both env vars to enable authentication.",
        stacklevel=2,
    )


def verify_auth(credentials: HTTPBasicCredentials | None = Depends(_security)):
    """Verify basic auth credentials. Skips if auth is not configured."""
    if not API_USER or not API_PASS:
        return "anonymous"
    if credentials is None:
        raise HTTPException(status_code=401, detail="Credentials required")
    correct_user = secrets.compare_digest(credentials.username, API_USER)
    correct_pass = secrets.compare_digest(credentials.password, API_PASS)
    if not (correct_user and correct_pass):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return credentials.username


_cors_origins_raw = os.environ.get("DCAG_CORS_ORIGINS", "")
_cors_origins = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins or ["*"],
    allow_credentials=bool(_cors_origins),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

engine = DCAGEngine(content_dir=CONTENT_DIR)

# In-memory run store
_runs: dict[str, WorkflowRun] = {}


# ── Request / Response models ─────────────────────────────

class StartRequest(BaseModel):
    workflow_id: str
    inputs: dict[str, Any] = {}


class SubmitResultRequest(BaseModel):
    step_id: str
    output: dict[str, Any] | str


# ── Serialisation helpers ─────────────────────────────────

def _serialize_step(request: ReasonRequest | ExecuteTemplateRequest | DelegateRequest | ExecuteScriptRequest) -> dict[str, Any]:
    """Serialize a typed StepRequest into a JSON-safe dict."""
    if isinstance(request, ReasonRequest):
        return {
            "mode": "reason",
            "step_id": request.step_id,
            "instruction": request.instruction,
            "tools": [
                {
                    "name": t.name,
                    "instruction": t.instruction,
                    "usage_pattern": t.usage_pattern,
                }
                for t in request.tools
            ],
            "context": {
                "static": request.context.static,
                "dynamic": request.context.dynamic,
            },
            "output_schema": request.output_schema,
            "budget": {
                "max_llm_turns": request.budget.max_llm_turns,
                "max_tokens": request.budget.max_tokens,
            },
        }
    elif isinstance(request, DelegateRequest):
        return {
            "mode": "delegate",
            "step_id": request.step_id,
            "capability": request.capability,
            "requires_approval": request.requires_approval,
            "inputs": request.inputs,
        }
    elif isinstance(request, ExecuteScriptRequest):
        return {
            "mode": "script",
            "step_id": request.step_id,
            "script": request.script,
        }
    elif isinstance(request, ExecuteTemplateRequest):
        return {
            "mode": "template",
            "step_id": request.step_id,
            "rendered_output": request.rendered_output,
            "artifacts": request.artifacts,
        }
    else:
        raise ValueError(f"Unknown step request type: {type(request)}")


def _run_progress(run: WorkflowRun) -> dict[str, Any]:
    """Compute progress from the run's trace."""
    trace = run.get_trace()
    completed = len(trace.get("steps", []))
    # Total steps from the workflow definition
    total = len(run._workflow.steps)
    return {"completed_steps": completed, "total_steps": total}


def _get_next_step(run: WorkflowRun) -> dict[str, Any] | None:
    """Get the next step from the run, serialized."""
    request = run.next_step()
    if request is None:
        return None
    return _serialize_step(request)


# ── Endpoints ─────────────────────────────────────────────

@app.get("/api/v1/workflows")
def list_workflows(user: str = Depends(verify_auth)) -> list[dict[str, Any]]:
    """List available workflows from the manifest."""
    entries = engine.list_workflows()
    return [
        {
            "id": e.id,
            "name": e.name,
            "persona": e.persona,
            "keywords": e.keywords,
            "input_pattern": e.input_pattern,
        }
        for e in entries
    ]


@app.post("/api/v1/runs")
def start_run(body: StartRequest, user: str = Depends(verify_auth)) -> dict[str, Any]:
    """Start a new workflow run. Returns run_id, first step, and progress."""
    try:
        run = engine.start(body.workflow_id, body.inputs)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Workflow '{body.workflow_id}' not found")

    _runs[run.run_id] = run

    step = _get_next_step(run)
    progress = _run_progress(run)

    return {
        "run_id": run.run_id,
        "status": run.status,
        "step": step,
        "progress": progress,
    }


@app.post("/api/v1/runs/{run_id}/results")
def submit_result(run_id: str, body: SubmitResultRequest, user: str = Depends(verify_auth)) -> dict[str, Any]:
    """Submit a step result and get the next step (or completed status)."""
    run = _runs.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")

    # Verify step_id matches current step
    if run.status != "running":
        raise HTTPException(
            status_code=409,
            detail=f"Run is '{run.status}', not running",
        )

    try:
        current = run._walker.current()
    except (IndexError, AttributeError):
        raise HTTPException(status_code=409, detail="No current step available")

    if current.id != body.step_id:
        raise HTTPException(
            status_code=409,
            detail=f"Expected step '{current.id}', got '{body.step_id}'",
        )

    # Record the result
    run.record_result(body.step_id, StepSuccess(output=body.output))

    # Get next step (or None if completed)
    next_step = _get_next_step(run)
    progress = _run_progress(run)

    return {
        "run_id": run_id,
        "status": run.status,
        "step": next_step,
        "progress": progress,
    }


@app.get("/api/v1/runs/{run_id}")
def get_run(run_id: str, user: str = Depends(verify_auth)) -> dict[str, Any]:
    """Get run status and execution trace."""
    run = _runs.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")

    return {
        "run_id": run_id,
        "status": run.status,
        "trace": run.get_trace(),
        "progress": _run_progress(run),
    }
