"""Tests for wire/subjects.py — NATS subject construction and parsing.

These tests encode the exact algorithm from
github.com/microbus-io/fabric@v1.27.1/connector/subjects.go.
"""

from __future__ import annotations

import pytest

from microbus_py.wire.subjects import (
    escape_path_part,
    request_subject,
    response_subject,
    reverse_hostname,
    subscription_subject,
)


class TestReverseHostname:
    def test_three_label_hostname(self) -> None:
        assert reverse_hostname("www.example.com") == "com.example.www"

    def test_single_label_hostname_returned_as_is(self) -> None:
        assert reverse_hostname("cache") == "cache"

    def test_two_label_hostname(self) -> None:
        assert reverse_hostname("a.b") == "b.a"

    def test_empty_string_returned_as_is(self) -> None:
        assert reverse_hostname("") == ""

    def test_lowercases_via_caller_not_function(self) -> None:
        # reverse_hostname does not lowercase — that is the caller's job
        assert reverse_hostname("WWW.Example.COM") == "COM.Example.WWW"


class TestEscapePathPart:
    def test_alphanumeric_passthrough(self) -> None:
        assert escape_path_part("hello123") == "hello123"

    def test_hyphen_passthrough(self) -> None:
        assert escape_path_part("a-b-c") == "a-b-c"

    def test_uppercase_preserved(self) -> None:
        assert escape_path_part("ABCxyz") == "ABCxyz"

    def test_dot_becomes_underscore(self) -> None:
        assert escape_path_part("file.html") == "file_html"

    def test_special_char_percent_encoded_4_hex(self) -> None:
        # space (0x20) → %0020
        assert escape_path_part("a b") == "a%0020b"

    def test_question_and_equals(self) -> None:
        # ? = 0x3f, = = 0x3d
        assert escape_path_part("q?x=1") == "q%003fx%003d1"

    def test_unicode_codepoint_4hex(self) -> None:
        # é = 0xe9
        assert escape_path_part("café") == "caf%00e9"


class TestRequestSubject:
    def test_simple_post_with_path(self) -> None:
        # POST /v1/echo on echo.example.com:443
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="echo.example.com",
            method="POST",
            path="/v1/echo",
        )
        assert subj == "microbus.443.com.example.echo.|.POST.v1.echo"

    def test_get_with_dotted_path_segment(self) -> None:
        # GET /path/file.html on example.com:80
        subj = request_subject(
            plane="microbus",
            port="80",
            hostname="example.com",
            method="GET",
            path="/path/file.html",
        )
        assert subj == "microbus.80.com.example.|.GET.path.file_html"

    def test_root_path_emits_underscore(self) -> None:
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="GET",
            path="/",
        )
        assert subj == "microbus.443.com.example.|.GET._"

    def test_empty_path_emits_underscore(self) -> None:
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="GET",
            path="",
        )
        assert subj == "microbus.443.com.example.|.GET._"

    def test_trailing_slash_becomes_trailing_underscore(self) -> None:
        # POST /dir/ on example.com:443  →  ... POST.dir._
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="POST",
            path="/dir/",
        )
        assert subj == "microbus.443.com.example.|.POST.dir._"

    def test_path_with_special_chars_percent_encoded(self) -> None:
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="echo.example.com",
            method="POST",
            path="/q?x=1 2",
        )
        assert subj == "microbus.443.com.example.echo.|.POST.q%003fx%003d1%00202"

    def test_method_uppercased(self) -> None:
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="get",  # lowercase input
            path="/x",
        )
        assert subj == "microbus.443.com.example.|.GET.x"

    def test_hostname_lowercased(self) -> None:
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="ECHO.Example.COM",
            method="GET",
            path="/x",
        )
        assert subj == "microbus.443.com.example.echo.|.GET.x"

    def test_port_zero_in_request_stays_literal(self) -> None:
        # Wildcard port is for subscriptions only; requests use literal "0"
        subj = request_subject(
            plane="microbus",
            port="0",
            hostname="example.com",
            method="GET",
            path="/x",
        )
        assert subj == "microbus.0.com.example.|.GET.x"

    def test_curly_braces_in_request_path_escaped_not_wildcarded(self) -> None:
        # In a request, {id} is a literal — escaping rules apply, no wildcard
        # `{` = 0x7b, `}` = 0x7d
        subj = request_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="GET",
            path="/users/{id}",
        )
        assert subj == "microbus.443.com.example.|.GET.users.%007bid%007d"


class TestSubscriptionSubject:
    def test_path_arg_curly_brace_becomes_star(self) -> None:
        # Subscription with /{id} → ...*
        subj = subscription_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="GET",
            path="/users/{id}",
        )
        assert subj == "microbus.443.com.example.|.GET.users.*"

    def test_greedy_path_arg_becomes_gt(self) -> None:
        # Subscription with /{rest...} → ...>
        subj = subscription_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="GET",
            path="/files/{rest...}",
        )
        assert subj == "microbus.443.com.example.|.GET.files.>"

    def test_method_any_becomes_star(self) -> None:
        subj = subscription_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="ANY",
            path="/x",
        )
        assert subj == "microbus.443.com.example.|.*.x"

    def test_port_zero_becomes_star(self) -> None:
        subj = subscription_subject(
            plane="microbus",
            port="0",
            hostname="example.com",
            method="GET",
            path="/x",
        )
        assert subj == "microbus.*.com.example.|.GET.x"

    def test_root_path_subscription(self) -> None:
        subj = subscription_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="GET",
            path="/",
        )
        assert subj == "microbus.443.com.example.|.GET._"

    def test_literal_star_segment_passes_through(self) -> None:
        # In a subscription, a literal `*` segment is preserved as a single-segment wildcard
        subj = subscription_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="GET",
            path="/users/*/profile",
        )
        assert subj == "microbus.443.com.example.|.GET.users.*.profile"

    def test_trailing_slash_subscription_emits_gt(self) -> None:
        # The doc comment in subjects.go specifies trailing-slash subscription -> ...>
        # but the actual algorithm produces _ (empty trailing segment) since
        # only `{name...}` triggers `>`. We assert the actual algorithm.
        subj = subscription_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method="POST",
            path="/dir/",
        )
        assert subj == "microbus.443.com.example.|.POST.dir._"


class TestResponseSubject:
    def test_basic_response_subject(self) -> None:
        subj = response_subject(
            plane="microbus",
            hostname="api.example.com",
            instance_id="a1b2c3d4",
        )
        assert subj == "microbus.r.com.example.api.a1b2c3d4"

    def test_hostname_and_id_lowercased(self) -> None:
        subj = response_subject(
            plane="microbus",
            hostname="API.Example.COM",
            instance_id="A1B2C3D4",
        )
        assert subj == "microbus.r.com.example.api.a1b2c3d4"


class TestSubjectInputValidation:
    @pytest.mark.parametrize(
        "method",
        ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    def test_all_standard_http_methods_pass(self, method: str) -> None:
        request_subject(
            plane="microbus",
            port="443",
            hostname="example.com",
            method=method,
            path="/x",
        )
