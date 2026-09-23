"""``python -m easy_mcp`` — the same entry point as the ``easy-mcp`` command.

A console script is a generated ``.exe`` on Windows, and application-control
policies sometimes refuse to launch one out of a freshly created virtualenv.
``python -m`` goes through the interpreter that is already running, so it keeps
working where the launcher does not — which is also why the ready-made
connectors have always been runnable that way.
"""

from .cli import main

if __name__ == "__main__":
    main()
