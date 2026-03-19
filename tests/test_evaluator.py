"""Tests for the expression evaluator."""
from dcag._evaluator import evaluate


class TestEvaluate:
    """Expression evaluator unit tests."""

    def test_equality_string(self):
        ctx = {"output": {"bug_type": "cast_error"}}
        assert evaluate("output.bug_type == 'cast_error'", ctx) is True

    def test_equality_string_false(self):
        ctx = {"output": {"bug_type": "join_error"}}
        assert evaluate("output.bug_type == 'cast_error'", ctx) is False

    def test_inequality(self):
        ctx = {"output": {"bug_type": "join_error"}}
        assert evaluate("output.bug_type != 'cast_error'", ctx) is True

    def test_greater_than(self):
        ctx = {"output": {"row_count": 1000}}
        assert evaluate("output.row_count > 500", ctx) is True

    def test_greater_than_false(self):
        ctx = {"output": {"row_count": 100}}
        assert evaluate("output.row_count > 500", ctx) is False

    def test_less_than(self):
        ctx = {"output": {"row_count": 100}}
        assert evaluate("output.row_count < 500", ctx) is True

    def test_in_operator_list(self):
        ctx = {"output": {"strategy": "CLUSTER_BY"}}
        assert evaluate("output.strategy in ['CLUSTER_BY', 'SOS']", ctx) is True

    def test_in_operator_not_found(self):
        ctx = {"output": {"strategy": "SKIP"}}
        assert evaluate("output.strategy in ['CLUSTER_BY', 'SOS']", ctx) is False

    def test_nested_dot_path(self):
        ctx = {"output": {"column_info": {"sf_type": "VARCHAR"}}}
        assert evaluate("output.column_info.sf_type == 'VARCHAR'", ctx) is True

    def test_top_level_key(self):
        ctx = {"status": "ready"}
        assert evaluate("status == 'ready'", ctx) is True

    def test_integer_comparison(self):
        ctx = {"output": {"size_gb": 15}}
        assert evaluate("output.size_gb > 10", ctx) is True

    def test_equality_integer(self):
        ctx = {"output": {"count": 0}}
        assert evaluate("output.count == 0", ctx) is True

    def test_missing_path_returns_false(self):
        ctx = {"output": {}}
        assert evaluate("output.nonexistent == 'value'", ctx) is False

    def test_empty_context_returns_false(self):
        assert evaluate("output.field == 'value'", {}) is False

    def test_bool_value(self):
        ctx = {"output": {"is_valid": True}}
        assert evaluate("output.is_valid == True", ctx) is True
