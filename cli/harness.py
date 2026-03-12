"""CLI test harness — simulates Shift driving the DCAG engine."""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dcag import DCAGEngine, ReasonRequest, ExecuteScriptRequest, ExecuteTemplateRequest, DelegateRequest, StepSuccess


def main():
    content_dir = Path(__file__).parent.parent / "content"
    engine = DCAGEngine(content_dir=content_dir)

    # Show available workflows
    print("Available workflows:")
    for wf in engine.list_workflows():
        print(f"  - {wf.id}: {wf.name} (triggers: {', '.join(wf.keywords)})")

    # Get workflow inputs
    workflow_id = input("\nWorkflow ID: ").strip() or "add-column-to-model"
    model_name = input("Model name: ").strip() or "src_pricing_analytics_events"
    column_name = input("Column name: ").strip() or "pcid"

    run = engine.start(workflow_id, {"model_name": model_name, "column_name": column_name})
    print(f"\nStarted run: {run.run_id}")
    print(f"Status: {run.status}\n")

    while run.status == "running":
        request = run.next_step()
        if request is None:
            break

        if isinstance(request, ExecuteScriptRequest):
            print(f"[EXECUTE/SCRIPT] Step: {request.step_id}")
            print(f"  Script: {request.script[:100]}...")
            if request.fallback_on_failure:
                print(f"  Fallback: {request.fallback_on_failure}")
            print()
            # Simulate successful script execution
            run.record_result(request.step_id, StepSuccess(output={"dbt_project_path": "/tmp/astronomer-core-data", "setup_mode": "full", "dbt_available": True, "dbt_mcp_available": True, "fallback_mode": "full"}))
            print(f"  -> Recorded. Status: {run.status}\n")

        elif isinstance(request, ReasonRequest):
            print(f"[REASON] Step: {request.step_id}")
            print(f"  Persona: {request.persona.name}")
            print(f"  Tools: {[t.name for t in request.tools]}")
            print(f"  Heuristics: {len(request.persona.heuristics)} items")
            print(f"  Context tokens: ~{request.context.estimated_tokens}")
            print(f"  Instruction: {request.instruction[:100]}...")
            print()

            # In real usage, Shift would make an LLM call here.
            # For the harness, prompt user for JSON response or use a default.
            user_input = input("  Paste JSON output (or press Enter for mock): ").strip()
            if user_input:
                output = json.loads(user_input)
            else:
                output = {"mock": True, "step_id": request.step_id}

            run.record_result(request.step_id, StepSuccess(output=output))
            print(f"  -> Recorded. Status: {run.status}\n")

        elif isinstance(request, ExecuteTemplateRequest):
            print(f"[EXECUTE/TEMPLATE] Step: {request.step_id}")
            print(f"  Rendered output:\n{request.rendered_output[:200]}...")
            print()
            run.record_result(request.step_id, StepSuccess(output=request.rendered_output))
            print(f"  -> Recorded. Status: {run.status}\n")

        elif isinstance(request, DelegateRequest):
            print(f"[DELEGATE] Step: {request.step_id}")
            print(f"  Capability: {request.capability}")
            print(f"  Requires approval: {request.requires_approval}")
            if request.requires_approval:
                approval = input("  Approve? (y/n): ").strip().lower()
                if approval != "y":
                    from dcag.types import StepFailure
                    run.record_result(request.step_id, StepFailure(error="User rejected"))
                    continue
            run.record_result(request.step_id, StepSuccess(output={"pr_url": "mock://pr"}))
            print(f"  -> Recorded. Status: {run.status}\n")

    print(f"\nWorkflow {run.status}.")
    trace = run.get_trace()
    print(f"Steps completed: {len(trace['steps'])}")
    print(f"Trace: {json.dumps(trace, indent=2, default=str)[:500]}...")


if __name__ == "__main__":
    main()
