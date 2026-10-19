"""What the package declares and ships: dependency floors, the lockfile, the PyPI page."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
REPOSITORY = "https://github.com/Mark007-R/Easy-MCP"

_REQUIREMENT = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[([^\]]*)\])?\s*(.*)$")


def project() -> dict[str, Any]:
    with (ROOT / "pyproject.toml").open("rb") as file:
        data: dict[str, Any] = tomllib.load(file)["project"]
    return data


def parse(requirement: str) -> tuple[str, tuple[str, ...], str]:
    """``(normalized name, extras, specifier)`` of a requirement without markers."""
    match = _REQUIREMENT.match(requirement.strip())
    assert match is not None, requirement
    name, extras, specifier = match.groups()
    names = tuple(sorted(extra.strip() for extra in (extras or "").split(",") if extra.strip()))
    return re.sub(r"[-_.]+", "-", name).lower(), names, specifier.replace(" ", "")


def floor(requirements: list[str], name: str) -> tuple[int, ...]:
    for requirement in requirements:
        found, _, specifier = parse(requirement)
        if found == name:
            assert specifier.startswith(">="), requirement
            return tuple(int(part) for part in specifier[2:].split("."))
    raise AssertionError(f"{name} is not required")


def test_the_redis_floor_skips_the_pool_that_deadlocks() -> None:
    # In redis-py 5.0.1, BlockingConnectionPool releases a connection that
    # failed to connect while still holding its own lock, so RedisStore.start()
    # hangs on a Redis it cannot reach, and the worker never answers /healthz.
    extras = project()["optional-dependencies"]
    declared = floor(extras["redis"], "redis")
    assert declared >= (5, 0, 2)
    assert floor(extras["dev"], "redis") == declared
    version = ".".join(str(part) for part in declared)
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert f"| `[redis]` | redis-py {version}+ |" in readme
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert f"(redis-py {version} or later)" in changelog


def test_the_lockfile_matches_the_project() -> None:
    # What `uv lock --check` compares: the extras and every requirement.
    with (ROOT / "uv.lock").open("rb") as file:
        packages = tomllib.load(file)["package"]
    locked = next(package for package in packages if package["name"] == "easy-mcp-kit")
    declared = project()
    extras = declared["optional-dependencies"]
    assert sorted(locked["metadata"]["provides-extras"]) == sorted(extras)
    expected = {(*parse(requirement), None) for requirement in declared["dependencies"]}
    for extra, requirements in extras.items():
        expected |= {(*parse(requirement), extra) for requirement in requirements}
    found = set()
    for entry in locked["metadata"]["requires-dist"]:
        marker = entry.get("marker")
        extra = None
        if marker is not None:
            match = re.fullmatch(r"extra == '([^']+)'", marker)
            assert match is not None, marker
            extra = match.group(1)
        name = re.sub(r"[-_.]+", "-", entry["name"]).lower()
        names = tuple(sorted(entry.get("extras", [])))
        found.add((name, names, entry.get("specifier", "").replace(" ", ""), extra))
    assert found == expected


def test_the_readme_links_work_on_pypi() -> None:
    # The README is the PyPI description, where a relative link resolves under
    # pypi.org/project/... and answers 404.
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    prose = re.sub(r"^```.*?^```", "", readme, flags=re.MULTILINE | re.DOTALL)
    prose = re.sub(r"`[^`\n]*`", "", prose)
    targets = re.findall(r"\]\(([^)\s]+)", prose)
    targets += re.findall(r"^\[[^\]]+\]:\s*(\S+)", prose, flags=re.MULTILINE)
    targets += re.findall(r"(?:src|href)=\"([^\"]+)\"", prose)
    assert targets
    relative = [t for t in targets if not t.startswith(("https://", "#"))]
    assert relative == []
    # The files they name are the repository's.
    for target in targets:
        if target.startswith(f"{REPOSITORY}/blob/main/"):
            assert (ROOT / target.removeprefix(f"{REPOSITORY}/blob/main/")).is_file(), target
        elif target.startswith(f"{REPOSITORY}/tree/main/"):
            assert (ROOT / target.removeprefix(f"{REPOSITORY}/tree/main/")).is_dir(), target
