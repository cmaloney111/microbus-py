"""Tests for claims/evaluator.py — boolean expression engine.

Operator grammar mirrors fabric@v1.27.1/frame/ifactor.go:
    ==, !=, =~ (regex), !~, >, <, >=, <=, &&, ||, !, parens, dot path.
Arrays are treated as sets: ``roles == "admin"`` matches when "admin" is in roles.
"""

from __future__ import annotations

import pytest

from microbus_py.claims.evaluator import compile_expr


class TestEqOperators:
    def test_string_eq_true(self) -> None:
        assert compile_expr('iss == "issuer"').evaluate({"iss": "issuer"})

    def test_string_eq_false(self) -> None:
        assert not compile_expr('iss == "issuer"').evaluate({"iss": "other"})

    def test_string_neq(self) -> None:
        assert compile_expr('iss != "x"').evaluate({"iss": "y"})

    def test_double_quoted_value(self) -> None:
        assert compile_expr("iss == 'issuer'").evaluate({"iss": "issuer"})


class TestRegexOperators:
    def test_regex_match(self) -> None:
        assert compile_expr('iss =~ "access.token.*"').evaluate({"iss": "access.token.core"})

    def test_regex_negative_match(self) -> None:
        assert compile_expr('iss !~ "bad.*"').evaluate({"iss": "good"})

    def test_regex_no_match(self) -> None:
        assert not compile_expr('iss =~ "bad.*"').evaluate({"iss": "good"})


class TestNumericOperators:
    @pytest.mark.parametrize(
        ("op", "left", "right", "expected"),
        [
            (">", 5, 3, True),
            (">", 3, 5, False),
            ("<", 3, 5, True),
            (">=", 5, 5, True),
            ("<=", 5, 5, True),
            (">=", 4, 5, False),
        ],
    )
    def test_numeric_comparison(self, op: str, left: int, right: int, expected: bool) -> None:
        expr = compile_expr(f"v {op} {right}")
        assert expr.evaluate({"v": left}) is expected


class TestLogicalOperators:
    def test_and_true(self) -> None:
        assert compile_expr("a && b").evaluate({"a": True, "b": True})

    def test_and_false(self) -> None:
        assert not compile_expr("a && b").evaluate({"a": True, "b": False})

    def test_or(self) -> None:
        assert compile_expr("a || b").evaluate({"a": False, "b": True})

    def test_not(self) -> None:
        assert compile_expr("!a").evaluate({"a": False})

    def test_precedence_and_over_or(self) -> None:
        # a || b && c  ≡  a || (b && c)
        assert compile_expr("a || b && c").evaluate({"a": True, "b": False, "c": False})

    def test_paren_grouping(self) -> None:
        # (a || b) && c
        assert not compile_expr("(a || b) && c").evaluate({"a": True, "b": True, "c": False})


class TestDotPath:
    def test_dot_path_navigation(self) -> None:
        assert compile_expr('user.name == "alice"').evaluate({"user": {"name": "alice"}})

    def test_dot_path_missing(self) -> None:
        assert not compile_expr('user.email == "x"').evaluate({"user": {}})


class TestArraySetSemantics:
    def test_array_membership(self) -> None:
        # roles == "admin" matches ["user", "admin"]
        assert compile_expr('roles == "admin"').evaluate({"roles": ["user", "admin"]})

    def test_array_no_membership(self) -> None:
        assert not compile_expr('roles == "admin"').evaluate({"roles": ["user"]})

    def test_dot_path_into_array_membership(self) -> None:
        # roles.admin → "admin" in roles list
        assert compile_expr("roles.admin").evaluate({"roles": ["user", "admin"]})


class TestCompileCaching:
    def test_repeated_compile_returns_same_object(self) -> None:
        a = compile_expr('iss == "x"')
        b = compile_expr('iss == "x"')
        assert a is b

    def test_distinct_sources_distinct_objects(self) -> None:
        a = compile_expr('iss == "x"')
        b = compile_expr('iss == "y"')
        assert a is not b


class TestEdgeCases:
    def test_empty_expression_rejected(self) -> None:
        with pytest.raises(ValueError):
            compile_expr("")

    def test_unbalanced_parens_rejected(self) -> None:
        with pytest.raises(ValueError):
            compile_expr("(a && b")

    def test_unknown_operator_rejected(self) -> None:
        with pytest.raises(ValueError):
            compile_expr("a ~~ b")

    def test_bare_identifier_truthy(self) -> None:
        # Standalone identifier is truthy if value present and truthy
        assert compile_expr("admin").evaluate({"admin": True})
        assert not compile_expr("admin").evaluate({"admin": False})
        assert not compile_expr("admin").evaluate({})

    def test_unknown_character_rejected(self) -> None:
        with pytest.raises(ValueError, match="unexpected character"):
            compile_expr("@#$")

    def test_dangling_operator_rejected(self) -> None:
        with pytest.raises(ValueError):
            compile_expr("a ==")

    def test_extra_token_after_expr_rejected(self) -> None:
        with pytest.raises(ValueError):
            compile_expr("a == 1 garbage")

    def test_numeric_value_with_decimal(self) -> None:
        assert compile_expr("v < 3.5").evaluate({"v": 3})
        assert not compile_expr("v < 3.5").evaluate({"v": 4})

    def test_negative_numeric_literal(self) -> None:
        assert compile_expr("v == -5").evaluate({"v": -5})

    def test_regex_against_non_string_value_is_false(self) -> None:
        assert not compile_expr('v =~ "x"').evaluate({"v": 42})
        # Negative regex against non-string value is true (no match)
        assert compile_expr('v !~ "x"').evaluate({"v": 42})

    def test_comparison_with_missing_lhs_is_false(self) -> None:
        assert not compile_expr("v > 5").evaluate({})

    def test_identifier_value_in_comparison(self) -> None:
        # An RHS that is an unquoted identifier compares string-equal
        assert compile_expr("a == admin").evaluate({"a": "admin"})

    def test_dot_path_into_dict_truthy(self) -> None:
        assert compile_expr("user.active").evaluate({"user": {"active": True}})
        assert not compile_expr("user.active").evaluate({"user": {"active": False}})
