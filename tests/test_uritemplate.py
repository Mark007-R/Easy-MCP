"""URI templates for resources: the RFC 6570 subset, matching and the traversal guard."""

from __future__ import annotations

import pytest

from easy_mcp import RegistrationError
from easy_mcp.uritemplate import UriTemplate, is_template, validate_uri


def test_parses_simple_and_reserved_expressions() -> None:
    template = UriTemplate("docs://{section}/guides/{+path}")
    assert template.template == "docs://{section}/guides/{+path}"
    assert template.variables == ("section", "path")
    assert template.reserved == frozenset({"path"})
    assert template.literal_length == len("docs://") + len("/guides/")
    assert repr(template) == "UriTemplate('docs://{section}/guides/{+path}')"
    assert is_template("users://{id}") and not is_template("config://app")


@pytest.mark.parametrize(
    "text",
    [
        "x://{#a}",
        "x://{.a}",
        "x://{/a}",
        "x://{;a}",
        "x://{?a}",
        "x://{&a}",
        "x://{a,b}",
        "x://{a:3}",
        "x://{a*}",
        "x://{}",
        "x://{+}",
        "x://{a",
        "x://a}",
        "x://{{a}}",
        "x://{a}}",
        "x://static",
    ],
)
def test_unsupported_operators_are_refused(text: str) -> None:
    with pytest.raises(RegistrationError):
        UriTemplate(text)


def test_variable_names_must_be_unique_identifiers() -> None:
    for text in ("x://{a}/{a}", "x://{a}/{+a}", "x://{1a}", "x://{a-b}", "x://{a b}"):
        with pytest.raises(RegistrationError):
            UriTemplate(text)
    assert UriTemplate("x://{_a1}/{b_2}").variables == ("_a1", "b_2")


def test_simple_variable_stays_in_one_segment() -> None:
    template = UriTemplate("users://{id}/avatar")
    assert template.match("users://42/avatar") == {"id": "42"}
    assert template.match("users://a/b/avatar") is None
    assert template.match("users://a?b/avatar") is None
    assert template.match("users://a#b/avatar") is None


def test_reserved_variable_spans_segments() -> None:
    template = UriTemplate("docs://guides/{+path}")
    assert template.match("docs://guides/a/b/c.md") == {"path": "a/b/c.md"}
    assert template.match("docs://guides/a?x") is None


def test_values_are_percent_decoded() -> None:
    template = UriTemplate("cafe://{name}")
    assert template.match("cafe://caf%C3%A9") == {"name": "café"}
    assert template.match("cafe://a%20b") == {"name": "a b"}


def test_bad_percent_encoding_or_utf8_does_not_match() -> None:
    template = UriTemplate("x://{v}")
    for uri in ("x://%ZZ", "x://%E", "x://%C3%28", "x://%FF", "x://a%"):
        assert template.match(uri) is None, uri


@pytest.mark.parametrize(
    ("template", "uri"),
    [
        ("x://{v}", "x://.."),
        ("x://{v}", "x://."),
        ("x://{v}", "x://%2E%2E"),
        ("x://{v}", "x://a%2Fb"),
        ("x://{v}", "x://a%5Cb"),
        ("x://{v}", "x://a\\b"),
        ("x://{v}", "x://a%00b"),
        ("x://{v}", "x://a%0Ab"),
        ("x://{v}", "x://a%7Fb"),
        ("x://{v}", "x://a%C2%85b"),
        ("x://{+v}", "x://a/../b"),
        ("x://{+v}", "x://a/./b"),
        ("x://{+v}", "x://../etc/passwd"),
        ("x://{+v}", "x://a/%2E%2E/b"),
        ("x://{+v}", "x:///etc/passwd"),
        ("x://{+v}", "x://%2Fetc"),
        ("x://{+v}", "x://a%5C..%5Cb"),
        ("x://{+v}", "x://a%00"),
        ("x://{+v}", "x://a\nb"),
    ],
)
def test_traversal_values_never_match(template: str, uri: str) -> None:
    assert UriTemplate(template).match(uri) is None


def test_empty_values_never_match() -> None:
    assert UriTemplate("x://{a}/y").match("x:///y") is None
    assert UriTemplate("x://y/{+a}").match("x://y/") is None


def test_full_match_only() -> None:
    template = UriTemplate("users://{id}/avatar")
    assert template.match("users://1/avatar.png") is None
    assert template.match("xusers://1/avatar") is None
    assert template.match("users://1/avatar") == {"id": "1"}


def test_literals_are_matched_literally() -> None:
    template = UriTemplate("a.b+c://{x}")
    assert template.match("a.b+c://1") == {"x": "1"}
    assert template.match("aXb+c://1") is None


@pytest.mark.parametrize(
    "uri",
    ["", "no-scheme", "1x://a", "x://a b", "x://a\tb", "x://\x00", "x://\x85", "x://" + "a" * 2050],
)
def test_uri_validation(uri: str) -> None:
    with pytest.raises(RegistrationError):
        validate_uri(uri)


def test_valid_uris_pass() -> None:
    for uri in ("config://app", "file:///etc/x", "urn:isbn:0451450523", "https://e.x/a?b=c#d"):
        validate_uri(uri)
