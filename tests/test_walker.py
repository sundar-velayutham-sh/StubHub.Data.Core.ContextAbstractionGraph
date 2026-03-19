"""Tests for DAG walker."""
import pytest

from dcag._walker import Walker
from dcag.types import StepDef, ToolDirective


def make_step(id: str, mode: str = "reason") -> StepDef:
    """Helper to create a minimal StepDef."""
    return StepDef(
        id=id, mode=mode, execute_type="template" if mode == "execute" else None,
        template=None, script=None, delegate=None,
        tools=[ToolDirective("t", "i")] if mode == "reason" else [],
        instruction=None, context_static=[], context_dynamic=[], context_knowledge=[],
        heuristics=[], anti_patterns=[], quality_criteria=[], output_schema=None,
        validation=[], requires_approval=False, budget=None, transitions=None,
    )


class TestWalker:
    def test_linear_walk(self):
        steps = [make_step("a"), make_step("b"), make_step("c")]
        walker = Walker(steps)
        assert walker.current().id == "a"
        walker.advance()
        assert walker.current().id == "b"
        walker.advance()
        assert walker.current().id == "c"
        walker.advance()
        assert walker.is_complete()

    def test_is_complete_initially_false(self):
        walker = Walker([make_step("a")])
        assert not walker.is_complete()

    def test_empty_steps(self):
        walker = Walker([])
        assert walker.is_complete()


def make_step_with_transitions(id: str, transitions: list[dict] | None = None) -> StepDef:
    """Helper to create a StepDef with transitions."""
    return StepDef(
        id=id, mode="reason", execute_type=None,
        template=None, script=None, delegate=None,
        tools=[ToolDirective("t", "i")],
        instruction=None, context_static=[], context_dynamic=[], context_knowledge=[],
        heuristics=[], anti_patterns=[], quality_criteria=[], output_schema=None,
        validation=[], requires_approval=False, budget=None,
        transitions=transitions,
    )


class TestConditionalWalker:
    """Tests for conditional transitions in Walker."""

    def test_transition_routes_to_matching_step(self):
        """When output matches a transition, Walker jumps to the goto step."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
                {"when": "output.bug_type == 'join_error'", "goto": "fix_join"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("fix_join"),
        ]
        walker = Walker(steps)
        assert walker.current().id == "classify"
        walker.advance(step_output={"output": {"bug_type": "cast_error"}})
        assert walker.current().id == "fix_cast"

    def test_transition_second_match(self):
        """Second transition matches when first doesn't."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
                {"when": "output.bug_type == 'join_error'", "goto": "fix_join"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("fix_join"),
        ]
        walker = Walker(steps)
        walker.advance(step_output={"output": {"bug_type": "join_error"}})
        assert walker.current().id == "fix_join"

    def test_transition_default_fallback(self):
        """When no 'when' matches, 'default' transition is used."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
                {"default": "fix_generic"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("fix_generic"),
        ]
        walker = Walker(steps)
        walker.advance(step_output={"output": {"bug_type": "unknown_error"}})
        assert walker.current().id == "fix_generic"

    def test_no_transitions_linear_advance(self):
        """Without transitions, advance() still works linearly (backward compat)."""
        steps = [make_step("a"), make_step("b"), make_step("c")]
        walker = Walker(steps)
        walker.advance()
        assert walker.current().id == "b"

    def test_advance_no_output_linear(self):
        """advance() without step_output falls through to linear even with transitions."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("next_step"),
        ]
        walker = Walker(steps)
        walker.advance()  # no output -> linear
        assert walker.current().id == "fix_cast"

    def test_transition_goto_unknown_step_raises(self):
        """If goto references a step that doesn't exist, raise ValueError."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "nonexistent"},
            ]),
        ]
        walker = Walker(steps)
        with pytest.raises(ValueError, match="nonexistent"):
            walker.advance(step_output={"output": {"bug_type": "cast_error"}})

    def test_transition_no_match_no_default_linear(self):
        """If transitions exist but none match and no default, fall through to linear."""
        steps = [
            make_step_with_transitions("classify", transitions=[
                {"when": "output.bug_type == 'cast_error'", "goto": "fix_cast"},
            ]),
            make_step_with_transitions("fix_cast"),
            make_step_with_transitions("linear_next"),
        ]
        walker = Walker(steps)
        walker.advance(step_output={"output": {"bug_type": "unknown"}})
        assert walker.current().id == "fix_cast"  # linear: next in list
