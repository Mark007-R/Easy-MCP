"""The switch for interop tests against the official MCP Python SDK client (not collected).

Every SDK interop test, whatever feature it covers, is skipped unless
``EASY_MCP_LIVE_SDK_CLIENT=1`` is set and the ``mcp`` package can be imported
(``pip install mcp``).  One switch, so a release run turns them all on at once.
"""

from __future__ import annotations

import os
from types import ModuleType

import pytest

ENV_VAR = "EASY_MCP_LIVE_SDK_CLIENT"

#: Apply to a test module (``pytestmark = live_sdk.marker``) or a single test.
marker = pytest.mark.skipif(os.environ.get(ENV_VAR) != "1", reason=f"{ENV_VAR}=1 is not set")


def require_sdk() -> ModuleType:
    """The ``mcp`` package, or skip the test when it is not installed."""
    return pytest.importorskip("mcp", reason="the official MCP Python SDK is not installed")
