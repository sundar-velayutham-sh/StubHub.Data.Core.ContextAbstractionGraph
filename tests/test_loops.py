"""Tests for step loop functionality."""
from dcag._walker import Walker
from dcag.types import StepDef, ToolDirective


def make_step(id: str, mode: str = "reason", loop: dict | None = None,
              transitions: list[dict] | None = None) -> StepDef:
    """Helper to create a StepDef with optional loop."""
    return StepDef(
        id=id, mode=mode, execute_type="template" if mode == "execute" else None,
        template=None, script=None, delegate=None,
        tools=[ToolDirective("t", "i")] if mode == "reason" else [],
        instruction=None, context_static=[], context_dynamic=[], context_knowledge=[],
        heuristics=[], anti_patterns=[], quality_criteria=[], output_schema=None,
        validation=[], requires_approval=False, budget=None,
        transitions=transitions, loop=loop,
    )


class TestStepLoops:
    """Tests for loop iteration in Walker."""

    def test_loop_iterates_correct_number(self):
        """Loop step executes once per item in the collection."""
        steps = [
            make_step("setup"),
            make_step("modify_each", loop={"over": "setup.models", "as": "current_model"}),
            make_step("validate"),
        ]
        walker = Walker(steps)

        # Advance past setup
        walker.advance(step_output={
            "output": {"models": ["model_a", "model_b", "model_c"]}
        })
        assert walker.current().id == "modify_each"

        # Set loop items (engine does this before first iteration)
        walker.set_loop_items(["model_a", "model_b", "model_c"])

        # First iteration
        assert walker.current().id == "modify_each"
        assert walker.current_loop_item() == "model_a"
        assert walker.loop_index() == 0
        walker.advance()

        # Second iteration -- same step
        assert walker.current().id == "modify_each"
        assert walker.current_loop_item() == "model_b"
        assert walker.loop_index() == 1
        walker.advance()

        # Third iteration -- same step
        assert walker.current().id == "modify_each"
        assert walker.current_loop_item() == "model_c"
        assert walker.loop_index() == 2
        walker.advance()

        # Loop done -- moves to next step
        assert walker.current().id == "validate"

    def test_loop_single_item(self):
        """Loop with one item executes once then advances."""
        steps = [
            make_step("modify_each", loop={"over": "setup.models", "as": "m"}),
            make_step("done"),
        ]
        walker = Walker(steps)
        walker.set_loop_items(["only_one"])

        assert walker.current_loop_item() == "only_one"
        walker.advance()
        assert walker.current().id == "done"

    def test_loop_empty_collection_skips(self):
        """Loop with empty collection advances to next step immediately."""
        steps = [
            make_step("modify_each", loop={"over": "setup.models", "as": "m"}),
            make_step("done"),
        ]
        walker = Walker(steps)
        walker.set_loop_items([])
        walker.advance()
        assert walker.current().id == "done"

    def test_no_loop_normal_advance(self):
        """Steps without loop advance normally."""
        steps = [make_step("a"), make_step("b")]
        walker = Walker(steps)
        assert walker.current_loop_item() is None
        walker.advance()
        assert walker.current().id == "b"

    def test_loop_index_resets_after_loop(self):
        """After loop completes, loop state is cleared."""
        steps = [
            make_step("loop_step", loop={"over": "x.items", "as": "item"}),
            make_step("after"),
        ]
        walker = Walker(steps)
        walker.set_loop_items(["a", "b"])
        walker.advance()  # a -> b
        walker.advance()  # b -> done -> next step
        assert walker.current().id == "after"
        assert walker.current_loop_item() is None
        assert walker.loop_index() == -1

    def test_is_in_loop(self):
        """is_in_loop() returns True during loop iteration."""
        steps = [
            make_step("loop_step", loop={"over": "x.items", "as": "item"}),
            make_step("after"),
        ]
        walker = Walker(steps)
        assert walker.is_in_loop() is False
        walker.set_loop_items(["a"])
        assert walker.is_in_loop() is True
        walker.advance()
        assert walker.is_in_loop() is False

    def test_loop_variable_name(self):
        """loop_variable_name() returns the 'as' field."""
        steps = [
            make_step("loop_step", loop={"over": "x.items", "as": "current_model"}),
        ]
        walker = Walker(steps)
        assert walker.loop_variable_name() == "current_model"

    def test_no_loop_variable_name_returns_none(self):
        """loop_variable_name() returns None for non-loop steps."""
        steps = [make_step("normal")]
        walker = Walker(steps)
        assert walker.loop_variable_name() is None
