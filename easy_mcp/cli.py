"""The ``easy-mcp`` command: serve a server someone else already wrote.

``easy-mcp run my_tools:server`` turns a module full of ``@server.tool``
functions into a running MCP server, so nothing needs a ``__main__`` block just
to be launchable -- and the transport can be chosen per host (stdio for a
desktop client, HTTP for everything else) without editing the code.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from collections.abc import Sequence

from ._version import __version__
from .connectors._cli import TRANSPORTS
from .server import MCPServer


class TargetError(Exception):
    """The ``module:attribute`` target could not be turned into a server."""


def parse_target(target: str) -> tuple[str, str]:
    """Split ``"package.module:server"`` into ``("package.module", "server")``.

    The attribute defaults to ``server``.  A path like ``my_tools.py`` is
    accepted as well and read as the module ``my_tools``, because that is what
    people type when the file is sitting right there.
    """
    module_name, separator, attribute = target.partition(":")
    if module_name.endswith(".py"):
        module_name = module_name[:-3].replace(os.sep, ".").replace("/", ".")
    module_name = module_name.strip(".")
    if not module_name:
        raise TargetError(f"{target!r} does not name a module")
    if separator and not attribute:
        raise TargetError(f"{target!r} ends with ':' but names no attribute")
    return module_name, attribute or "server"


def load_server(target: str) -> MCPServer:
    """Import *target* and return the :class:`MCPServer` it names.

    The attribute may be a server or a callable returning one, so a factory
    that reads configuration at startup works the same way.

    Raises:
        TargetError: The module will not import, or holds no server there.
    """
    module_name, attribute = parse_target(target)
    # A console script does not get the working directory on sys.path, but the
    # module being named is almost always right here.
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise TargetError(f"cannot import {module_name!r}: {exc}") from exc
    try:
        candidate = getattr(module, attribute)
    except AttributeError:
        raise TargetError(f"{module_name!r} has no attribute {attribute!r}") from None
    if not isinstance(candidate, MCPServer) and callable(candidate):
        candidate = candidate()
    if not isinstance(candidate, MCPServer):
        raise TargetError(
            f"{module_name}:{attribute} is a {type(candidate).__name__}, not an MCPServer"
        )
    return candidate


def build_parser() -> argparse.ArgumentParser:
    """The ``easy-mcp`` parser."""
    parser = argparse.ArgumentParser(
        prog="easy-mcp",
        description="Serve an MCPServer defined in your own module.",
    )
    parser.add_argument("--version", action="version", version=f"easy-mcp-kit {__version__}")
    subcommands = parser.add_subparsers(dest="command", required=True)

    run = subcommands.add_parser(
        "run",
        help="import a module and serve the server it defines",
        description=(
            "Import TARGET and serve it. Importing runs the module, so only "
            "point this at code you trust."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run.add_argument(
        "target",
        metavar="TARGET",
        help="module:attribute, e.g. my_tools:server (attribute defaults to 'server')",
    )
    run.add_argument(
        "--transport",
        choices=TRANSPORTS,
        default="http",
        help="http serves Streamable HTTP at /mcp (+ legacy SSE); "
        "stdio serves the parent process over stdin/stdout",
    )
    run.add_argument("--host", default=None, help="override the server's own host (http/sse)")
    run.add_argument(
        "--port", type=int, default=None, help="override the server's own port (http/sse)"
    )
    run.add_argument(
        "--debug",
        action="store_true",
        help="send tracebacks to clients (development only)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Entry point for the ``easy-mcp`` console script."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        server = load_server(args.target)
    except TargetError as exc:
        parser.error(str(exc))
    # Only override what was actually asked for: the server's own constructor
    # arguments are the defaults, not this parser's.
    if args.host is not None:
        server.host = args.host
    if args.port is not None:
        server.port = args.port
    if args.debug:
        server.debug = True
    server.run(args.transport)
