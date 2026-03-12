"""
Live integration test for Shift <> DCAG.

Runs the table-optimizer workflow with real Anthropic API calls.
Skipped by default -- enable with: DCAG_LIVE_TEST=1 pytest tests/integration/ -v

This test validates the full driver loop:
  engine.start -> next_step -> ShiftDriver.assemble_prompt -> Anthropic API -> record_result
"""
import json
import os
from pathlib import Path

import pytest

from dcag import DCAGEngine
from dcag.drivers.shift import ShiftDriver
from dcag.types import ReasonRequest, DelegateRequest, StepSuccess


CONTENT_DIR = Path(__file__).parent.parent.parent / "content"


@pytest.mark.live
class TestShiftLiveIntegration:
    """Run table-optimizer workflow with real LLM calls."""

    def _call_anthropic(self, prompt: str, output_schema: dict | None = None) -> dict:
        """Call Anthropic API with assembled prompt, return parsed JSON."""
        import anthropic

        client = anthropic.Anthropic()  # uses ANTHROPIC_API_KEY env var

        system = "You are a Snowflake data engineering expert. Always respond with valid JSON."
        messages = [{"role": "user", "content": prompt}]

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=4096,
            system=system,
            messages=messages,
        )

        # Extract text content
        text = response.content[0].text

        # Parse JSON from response (handle markdown code blocks)
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        return json.loads(text.strip())

    def test_table_optimizer_live(self):
        """Run full table-optimizer workflow with real LLM."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        driver = ShiftDriver()

        run = engine.start("table-optimizer", {"table_name": "TRANSACTION"})
        assert run.status == "running"

        steps_completed = []
        reason_outputs = {}

        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            steps_completed.append(request.step_id)

            if isinstance(request, ReasonRequest):
                prompt = driver.assemble_prompt(request)
                assert len(prompt) > 100, f"Prompt too short for {request.step_id}"

                # Call real LLM
                output = self._call_anthropic(prompt, request.output_schema)
                reason_outputs[request.step_id] = output
                run.record_result(request.step_id, StepSuccess(output=output))

            elif isinstance(request, DelegateRequest):
                action = driver.route_delegate(request)
                assert action["capability"] in ("show_plan", "create_pr")

                # Auto-approve in test mode
                if request.step_id == "show_recommendations":
                    run.record_result(request.step_id, StepSuccess(output={"approved": True}))
                elif request.step_id == "apply_changes":
                    run.record_result(
                        request.step_id,
                        StepSuccess(output={"pr_url": "https://github.com/test/pr/1", "applied": False}),
                    )

        # Workflow completed all 9 steps
        assert run.status == "completed", f"Workflow ended with status: {run.status}"
        assert len(steps_completed) == 9, f"Expected 9 steps, got {len(steps_completed)}"

        # Verify report structure
        report = reason_outputs.get("generate_report", {})
        assert "strategy" in report, "Report missing 'strategy' field"
        assert report["strategy"] in (
            "SKIP", "ORDER_BY", "CLUSTER_BY", "CLUSTER_BY_AND_SOS", "MONITOR",
        )

        # Verify load frequency was detected
        freq = reason_outputs.get("detect_load_frequency", {})
        assert "load_frequency" in freq, "Missing load_frequency"

        # Verify trace
        trace = run.get_trace()
        assert len(trace["steps"]) == 9

    def test_driver_prompt_quality(self):
        """Verify prompt assembly produces well-structured prompts for each step."""
        engine = DCAGEngine(content_dir=CONTENT_DIR)
        driver = ShiftDriver()

        run = engine.start("table-optimizer", {"table_name": "VENUE_DIM"})
        prompts = {}

        while run.status == "running":
            request = run.next_step()
            if request is None:
                break

            if isinstance(request, ReasonRequest):
                prompt = driver.assemble_prompt(request)
                prompts[request.step_id] = prompt

                # Verify prompt structure
                assert "[TOOLS" in prompt, f"{request.step_id}: missing TOOLS section"
                assert "[PERSONA]" in prompt, f"{request.step_id}: missing PERSONA section"
                assert "[TASK]" in prompt, f"{request.step_id}: missing TASK section"

                # Use dummy output to advance
                run.record_result(request.step_id, StepSuccess(output={"placeholder": True}))
            elif isinstance(request, DelegateRequest):
                run.record_result(request.step_id, StepSuccess(output={"approved": True}))

        assert len(prompts) == 7, "Expected 7 reason steps with prompts"
