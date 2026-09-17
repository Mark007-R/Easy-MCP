"""Shared command-line plumbing for the ready-made connectors."""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable, Sequence
from typing import Any

from ..security.auth import APIKeyAuth
from ..server import MCPServer

TRANSPORTS = ("http", "sse", "stdio")


def build_parser(description: str) -> argparse.ArgumentParser:
    """An argument parser with the options every connector shares."""
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--transport",
        choices=TRANSPORTS,
        default="http",
        help="http serves Streamable HTTP at /mcp (+ legacy SSE); "
        "stdio serves the parent process over stdin/stdout",
    )
    parser.add_argument("--host", default="127.0.0.1", help="interface to bind (http/sse)")
    parser.add_argument("--port", type=int, default=8000, help="port to bind (http/sse)")
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=120,
        metavar="N",
        help="requests per minute per client; 0 disables",
    )
    parser.add_argument(
        "--debug", action="store_true", help="send tracebacks to clients (development only)"
    )
    return parser


def auth_from_env(var: str = "EASY_MCP_API_KEYS") -> APIKeyAuth | None:
    """``APIKeyAuth`` from the environment, or ``None`` when the variable is unset."""
    if not os.environ.get(var):
        return None
    return APIKeyAuth.from_env(var)


def run(
    build: Callable[[argparse.Namespace], MCPServer],
    parser: argparse.ArgumentParser,
    argv: Sequence[str] | None = None,
) -> None:
    """Parse *argv*, build the server, and serve it on the chosen transport."""
    args = parser.parse_args(argv)
    try:
        server = build(args)
    except (ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    server.run(args.transport)


def server_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    """``MCPServer`` keyword arguments derived from the shared options."""
    return {
        "host": args.host,
        "port": args.port,
        "debug": args.debug,
        "rate_limit_per_minute": args.rate_limit or None,
        "auth": auth_from_env(),
    }
