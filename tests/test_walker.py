"""Tests for DAG walker."""
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
