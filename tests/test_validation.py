"""Tests for structural validators."""
from dcag._validation import validate_structural


class TestValidateStructural:
    def test_output_has_passes(self):
        rules = [{"output_has": "column_info"}]
        output = {"column_info": {"name": "pcid"}}
        assert validate_structural(output, rules) == []

    def test_output_has_fails(self):
        rules = [{"output_has": "column_info"}]
        output = {"other_field": "value"}
        errors = validate_structural(output, rules)
        assert len(errors) == 1
        assert "column_info" in errors[0]

    def test_output_has_empty_value_fails(self):
        rules = [{"output_has": "column_info"}]
        output = {"column_info": None}
        errors = validate_structural(output, rules)
        assert len(errors) == 1

    def test_multiple_rules(self):
        rules = [{"output_has": "a"}, {"output_has": "b"}]
        output = {"a": 1}
        errors = validate_structural(output, rules)
        assert len(errors) == 1

    def test_no_rules(self):
        assert validate_structural({"any": "thing"}, []) == []
