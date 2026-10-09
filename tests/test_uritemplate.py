"""URI templates for resources: the RFC 6570 subset, matching and the traversal guard."""

from __future__ import annotations

import random
import re
import time

import pytest

from easy_mcp import RegistrationError
from easy_mcp.uritemplate import MAX_URI_LENGTH, UriTemplate, is_template, validate_uri

MIB = 1024 * 1024


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


def test_greedy_splits() -> None:
    assert UriTemplate("files://{name}.{ext}").match("files://a.b.c") == {
        "name": "a.b",
        "ext": "c",
    }
    assert UriTemplate("x://{a}.{+b}").match("x://p.q/r.s") == {"a": "p", "b": "q/r.s"}
    assert UriTemplate("x://{a}{b}.json").match("x://abc.json") == {"a": "ab", "b": "c"}
    assert UriTemplate("pkg://{n}/{a}.{b}.{c}").match("pkg://x/1.2.3.4") == {
        "n": "x",
        "a": "1.2",
        "b": "3",
        "c": "4",
    }
    assert UriTemplate("x://{a}/b{+c}").match("x://a/bb/bc") == {"a": "a", "c": "b/bc"}


def test_may_overlap_compares_the_literal_ends() -> None:
    docs = UriTemplate("docs://{name}")
    staff = UriTemplate("docs://staff-{name}")
    assert docs.may_overlap(staff) and staff.may_overlap(docs)
    assert not docs.may_overlap(UriTemplate("wiki://{name}"))
    assert not UriTemplate("x://{a}.md").may_overlap(UriTemplate("x://{a}.txt"))
    assert UriTemplate("x://{a}.md").may_overlap(UriTemplate("x://{+a}"))


def _hostile(prefix: str, body: str, tail: str, length: int) -> str:
    """*prefix*, then *body* repeated, then *tail*: *length* characters in all."""
    room = length - len(prefix) - len(tail)
    return prefix + (body * (room // len(body) + 1))[:room] + tail


# Templates whose regular expression would backtrack polynomially on a URI
# that almost matches, and such URIs: a backtracking matcher takes over 10 s
# on the first at 2048 characters, and hours at 1 MiB.  The last one makes
# the linear matcher walk the whole URI (about 0.5 s at 1 MiB).
HOSTILE = [
    ("pkg://{n}/{a}.{b}.{c}", "pkg://a/", ".", "/"),
    ("files://{a}.{b}", "files://", ".", "/"),
    ("files://{a}.{b}", "files://", "a.", "/"),
    ("repo://{+a}/{+b}.md", "repo://", "/", "?.md"),
    ("repo://{+a}/{+b}.md", "repo://", "a/", "b"),
    ("x://{a}{b}.json", "x://", "a", "/.json"),
    ("x://{+a}.{b}.{+c}", "x://", "/q.", "q"),
]


@pytest.mark.parametrize(("length", "bound"), [(MAX_URI_LENGTH, 1.0), (MIB, 5.0)])
def test_hostile_uris_are_refused_in_linear_time(length: int, bound: float) -> None:
    for text, prefix, body, tail in HOSTILE:
        template = UriTemplate(text)
        uri = _hostile(prefix, body, tail, length)
        assert len(uri) == length
        started = time.perf_counter()
        assert template.match(uri) is None, text
        elapsed = time.perf_counter() - started
        assert elapsed < bound, f"{text} took {elapsed:.2f}s on {length} characters"


def _reference(template: UriTemplate, uri: str) -> list[str] | None:
    """The split a greedy backtracking regular expression finds: what matching must keep."""
    parts: list[str] = []
    position = 0
    for found in re.finditer(r"\{(\+?)([^{}]*)\}", template.template):
        parts.append(re.escape(template.template[position : found.start()]))
        parts.append("([^?#]+)" if found.group(1) else "([^/?#]+)")
        position = found.end()
    parts.append(re.escape(template.template[position:]))
    found = re.fullmatch("".join(parts), uri, re.DOTALL)
    return None if found is None else list(found.groups())


def test_split_is_the_greedy_regular_expression_split() -> None:
    rng = random.Random(6570)
    pieces = ["", "", ".", "/", "-", "a", "?", "#", "./", "a."]
    alphabet = "a./-?#%"
    for _ in range(4000):
        count = rng.randint(1, 4)
        text = "x:" + rng.choice(pieces)
        for index in range(count):
            text += "{" + rng.choice(["", "+"]) + f"v{index}" + "}" + rng.choice(pieces)
        template = UriTemplate(text)
        for _ in range(10):
            if rng.random() < 0.5:
                uri = "x:" + "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 14)))
            else:
                # An expansion, mutated a little, so that many of them match.
                uri = re.sub(
                    r"\{\+?[^{}]*\}",
                    lambda _: "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 4))),
                    text,
                )
                if rng.random() < 0.3:
                    at = rng.randrange(len(uri) + 1)
                    uri = uri[:at] + rng.choice(alphabet) + uri[at:]
            assert template._split(uri) == _reference(template, uri), (text, uri)


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
