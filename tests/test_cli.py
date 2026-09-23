"""The ``easy-mcp run`` command: resolving a target and launching it."""

from __future__ import annotations

import importlib
import itertools
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from easy_mcp import MCPServer
from easy_mcp.cli import TargetError, load_server, main, parse_target

_counter = itertools.count()


def write_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str) -> str:
    """Write an importable module into *tmp_path* and return its name."""
    name = f"cli_target_{next(_counter)}"
    (tmp_path / f"{name}.py").write_text(body, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    importlib.invalidate_caches()
    return name


SERVER_MODULE = '''
from easy_mcp import MCPServer

server = MCPServer(port=1234, host="10.0.0.1")

@server.tool
def ping() -> str:
    """Say pong."""
    return "pong"
'''


# ---------------------------------------------------- module entry points


@pytest.mark.parametrize("target", ["easy_mcp", "easy_mcp.cli"])
def test_runnable_through_python_m(target: str) -> None:
    # The console script is a generated .exe on Windows, which application
    # control sometimes refuses to launch out of a fresh virtualenv; python -m
    # goes through the interpreter that is already running.
    result = subprocess.run(
        [sys.executable, "-m", target, "--version"], capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "easy-mcp-kit" in result.stdout


def test_python_m_reports_a_bad_target(tmp_path: Path) -> None:
    # Proves main() actually runs rather than the module importing and exiting.
    result = subprocess.run(
        [sys.executable, "-m", "easy_mcp", "run", "definitely_not_a_module"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert result.returncode == 2
    assert "cannot import" in result.stderr


# ----------------------------------------------------------------- targets


def test_parse_target_splits_module_and_attribute() -> None:
    assert parse_target("pkg.mod:app") == ("pkg.mod", "app")


def test_parse_target_defaults_the_attribute() -> None:
    assert parse_target("pkg.mod") == ("pkg.mod", "server")


def test_parse_target_accepts_a_filename() -> None:
    # What people type when the file is sitting right there.
    assert parse_target("my_tools.py") == ("my_tools", "server")
    assert parse_target("my_tools.py:api") == ("my_tools", "api")


def test_parse_target_rejects_nonsense() -> None:
    with pytest.raises(TargetError):
        parse_target("")
    with pytest.raises(TargetError, match="names no attribute"):
        parse_target("pkg.mod:")


# ------------------------------------------------------------------ loading


def test_load_server_finds_the_server(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    name = write_module(tmp_path, monkeypatch, SERVER_MODULE)
    server = load_server(name)
    assert isinstance(server, MCPServer)
    assert [definition.name for definition in server.tools] == ["ping"]


def test_load_server_calls_a_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    name = write_module(
        tmp_path,
        monkeypatch,
        """
from easy_mcp import MCPServer

def build():
    return MCPServer(port=4321)
""",
    )
    assert load_server(f"{name}:build").port == 4321


def test_load_server_reports_a_missing_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(TargetError, match="cannot import"):
        load_server("no_such_module_anywhere:server")


def test_load_server_reports_a_missing_attribute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    name = write_module(tmp_path, monkeypatch, SERVER_MODULE)
    with pytest.raises(TargetError, match="has no attribute 'nope'"):
        load_server(f"{name}:nope")


def test_load_server_rejects_the_wrong_kind_of_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    name = write_module(tmp_path, monkeypatch, "server = 42\n")
    with pytest.raises(TargetError, match="not an MCPServer"):
        load_server(name)


# ------------------------------------------------------------------ running


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record what ``run`` was called with instead of serving."""
    calls: list[Any] = []

    def record(self: MCPServer, transport: Any = None) -> None:
        calls.append((self, transport))

    monkeypatch.setattr(MCPServer, "run", record)
    return calls


def test_run_serves_the_named_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, captured: list[Any]
) -> None:
    name = write_module(tmp_path, monkeypatch, SERVER_MODULE)
    main(["run", name])
    (server, transport) = captured[0]
    assert transport == "http"
    # Untouched: the module's own constructor arguments stand.
    assert (server.host, server.port, server.debug) == ("10.0.0.1", 1234, False)


def test_run_overrides_only_what_was_asked_for(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, captured: list[Any]
) -> None:
    name = write_module(tmp_path, monkeypatch, SERVER_MODULE)
    main(["run", name, "--port", "9000", "--debug", "--transport", "stdio"])
    (server, transport) = captured[0]
    assert transport == "stdio"
    assert server.port == 9000
    assert server.debug is True
    assert server.host == "10.0.0.1"  # not asked for, so not changed


def test_run_reports_a_bad_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as excinfo:
        main(["run", "definitely_not_a_module"])
    assert excinfo.value.code == 2  # argparse's usage-error exit code
