"""GitHub connector: repositories, issues, pull requests and file contents.

Read-only by default.  The write tools (``create_issue``,
``comment_on_issue``) are registered only when ``enable_write=True`` (CLI:
``--allow-write``) and are additionally gated by the ``github:write`` scope,
so a client needs an API key carrying that scope to see or call them.

Credentials: ``GITHUB_TOKEN`` (a fine-grained personal access token or an
installation token).  The token travels only in the ``Authorization`` header
to ``GITHUB_API_URL`` (default ``https://api.github.com``); it is never
logged and never part of an error message.  Without a token the connector
still works for public data, subject to GitHub's low anonymous rate limit.

Only the standard library is used for HTTP, so the connector adds no
dependency to the package.

Launch::

    GITHUB_TOKEN=github_pat_... easy-mcp-github --transport stdio
    python -m easy_mcp.connectors.github --port 8010 --allow-write
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from typing import Any, Literal

from ..exceptions import ToolError
from ..server import MCPServer
from . import _cli

TOKEN_ENV_VAR = "GITHUB_TOKEN"
API_URL_ENV_VAR = "GITHUB_API_URL"
DEFAULT_API_URL = "https://api.github.com"
WRITE_SCOPE = "github:write"

MAX_LIMIT = 100
MAX_FILE_CHARS = 200_000
REQUEST_TIMEOUT = 20.0

_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class GitHubClient:
    """A minimal GitHub REST client on :mod:`urllib` (sync; tools run in a thread)."""

    def __init__(self, token: str | None, api_url: str = DEFAULT_API_URL) -> None:
        self._token = token
        self._api_url = api_url.rstrip("/")

    @property
    def authenticated(self) -> bool:
        return bool(self._token)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        """Perform one API call and return the decoded JSON body.

        Raises:
            ToolError: With a client-safe message for any HTTP or network error.
        """
        url = self._api_url + path
        if params:
            query = {k: v for k, v in params.items() if v is not None}
            if query:
                url += "?" + urllib.parse.urlencode(query)
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "easy-mcp-github",
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raise ToolError(_describe_http_error(exc)) from None
        except urllib.error.URLError as exc:
            raise ToolError(f"GitHub API unreachable: {exc.reason}") from None
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ToolError("GitHub API returned a non-JSON response") from None


def _describe_http_error(exc: urllib.error.HTTPError) -> str:
    detail = ""
    try:
        payload = json.loads(exc.read().decode("utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("message"), str):
            detail = payload["message"]
    except Exception:  # any parse failure just loses the detail
        detail = ""
    if exc.code in (403, 429) and exc.headers.get("X-RateLimit-Remaining") == "0":
        reset = exc.headers.get("X-RateLimit-Reset", "")
        return f"GitHub rate limit exceeded (resets at unix time {reset or 'unknown'})"
    if exc.code == 401:
        return "GitHub rejected the credentials (check GITHUB_TOKEN)"
    if exc.code == 404:
        return "GitHub returned 404: not found, or the token cannot see it"
    return f"GitHub API error {exc.code}: {detail or exc.reason}"


# ------------------------------------------------------------------ helpers


def _split_repo(repo: str) -> tuple[str, str]:
    if not _REPO_RE.match(repo):
        raise ToolError(f"repo must be 'owner/name', got {repo!r}")
    owner, name = repo.split("/", 1)
    return owner, name


def _repo_path(repo: str, *segments: str) -> str:
    owner, name = _split_repo(repo)
    parts = [urllib.parse.quote(owner, safe=""), urllib.parse.quote(name, safe="")]
    parts.extend(urllib.parse.quote(segment, safe="/") for segment in segments)
    return "/repos/" + "/".join(parts)


def _check_limit(limit: int) -> int:
    if not 1 <= limit <= MAX_LIMIT:
        raise ToolError(f"limit must be between 1 and {MAX_LIMIT}")
    return limit


def _user(payload: Any) -> str | None:
    return payload.get("login") if isinstance(payload, dict) else None


def _repo_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "full_name": item.get("full_name"),
        "description": item.get("description"),
        "private": item.get("private"),
        "default_branch": item.get("default_branch"),
        "language": item.get("language"),
        "stars": item.get("stargazers_count"),
        "forks": item.get("forks_count"),
        "open_issues": item.get("open_issues_count"),
        "archived": item.get("archived"),
        "updated_at": item.get("updated_at"),
        "url": item.get("html_url"),
    }


def _issue_summary(item: dict[str, Any], *, with_body: bool) -> dict[str, Any]:
    summary = {
        "number": item.get("number"),
        "title": item.get("title"),
        "state": item.get("state"),
        "author": _user(item.get("user")),
        "labels": [
            label.get("name") if isinstance(label, dict) else label
            for label in item.get("labels", [])
        ],
        "assignees": [_user(a) for a in item.get("assignees", [])],
        "comments": item.get("comments"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "url": item.get("html_url"),
    }
    if with_body:
        summary["body"] = item.get("body")
    return summary


def _pull_summary(item: dict[str, Any], *, with_body: bool) -> dict[str, Any]:
    summary = {
        "number": item.get("number"),
        "title": item.get("title"),
        "state": item.get("state"),
        "draft": item.get("draft"),
        "author": _user(item.get("user")),
        "head": (item.get("head") or {}).get("label"),
        "base": (item.get("base") or {}).get("ref"),
        "merged_at": item.get("merged_at"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "url": item.get("html_url"),
    }
    if with_body:
        summary["body"] = item.get("body")
        summary["mergeable"] = item.get("mergeable")
        summary["additions"] = item.get("additions")
        summary["deletions"] = item.get("deletions")
        summary["changed_files"] = item.get("changed_files")
    return summary


# ------------------------------------------------------------------- server


def build_server(
    *,
    token: str | None = None,
    api_url: str | None = None,
    enable_write: bool = False,
    client: GitHubClient | None = None,
    **server_options: Any,
) -> MCPServer:
    """Build the GitHub connector server.

    Args:
        token: GitHub token; defaults to the ``GITHUB_TOKEN`` environment
            variable.  ``None`` means anonymous, public-only access.
        api_url: API base URL; defaults to ``GITHUB_API_URL`` or the public API.
        enable_write: Also register the write tools (``create_issue``,
            ``comment_on_issue``).  They require the ``github:write`` scope,
            so ``auth`` must be configured for anyone to reach them.
        client: Injectable client (tests).
        **server_options: Passed to :class:`~easy_mcp.server.MCPServer`.

    Raises:
        ValueError: ``enable_write`` without ``auth``: the write tools would
            be unreachable, which is almost certainly a misconfiguration.
    """
    if enable_write and server_options.get("auth") is None:
        raise ValueError(
            "enable_write requires auth: write tools are gated by the "
            f"'{WRITE_SCOPE}' scope (set EASY_MCP_API_KEYS)"
        )
    gh = client or GitHubClient(
        token if token is not None else os.environ.get(TOKEN_ENV_VAR),
        api_url or os.environ.get(API_URL_ENV_VAR) or DEFAULT_API_URL,
    )
    instructions = "GitHub access. Repositories are addressed as 'owner/name'."
    if enable_write:
        instructions += f" Write tools need an API key holding the '{WRITE_SCOPE}' scope."
    else:
        instructions = "Read-only " + instructions
    server_options.setdefault("name", "easy-mcp-github")
    server_options.setdefault("instructions", instructions)
    server = MCPServer(**server_options)

    @server.tool
    def list_repos(
        owner: str | None = None,
        affiliation: Literal["all", "owner", "member"] = "all",
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """List repositories: the token owner's (default) or a user/org's public ones.

        Args:
            owner: A user or organization login; omit for the authenticated user.
            affiliation: For the authenticated user only: all, owner, or member.
            limit: Maximum repositories to return (1-100), most recently updated first.
        """
        _check_limit(limit)
        if owner:
            path = f"/users/{urllib.parse.quote(owner, safe='')}/repos"
            params: dict[str, Any] = {"per_page": limit, "sort": "updated"}
        elif gh.authenticated:
            path = "/user/repos"
            params = {"per_page": limit, "sort": "updated", "affiliation": affiliation}
        else:
            raise ToolError("owner is required when no GITHUB_TOKEN is configured")
        items = gh.request("GET", path, params=params)
        return [_repo_summary(item) for item in items or []]

    @server.tool
    def get_repo(repo: str) -> dict[str, Any]:
        """Get one repository's metadata.

        Args:
            repo: Repository as 'owner/name'.
        """
        item = gh.request("GET", _repo_path(repo))
        summary = _repo_summary(item)
        summary["topics"] = item.get("topics", [])
        summary["license"] = (item.get("license") or {}).get("spdx_id")
        summary["created_at"] = item.get("created_at")
        return summary

    @server.tool
    def list_issues(
        repo: str,
        state: Literal["open", "closed", "all"] = "open",
        labels: str | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """List issues (pull requests excluded), most recently updated first.

        Args:
            repo: Repository as 'owner/name'.
            state: open, closed, or all.
            labels: Comma-separated label names that every issue must carry.
            limit: Maximum issues to return (1-100).
        """
        _check_limit(limit)
        items = gh.request(
            "GET",
            _repo_path(repo, "issues"),
            params={"state": state, "labels": labels, "per_page": limit, "sort": "updated"},
        )
        return [
            _issue_summary(item, with_body=False)
            for item in items or []
            if "pull_request" not in item
        ]

    @server.tool
    def get_issue(repo: str, number: int) -> dict[str, Any]:
        """Get one issue including its body.

        Args:
            repo: Repository as 'owner/name'.
            number: Issue number.
        """
        item = gh.request("GET", _repo_path(repo, "issues", str(number)))
        return _issue_summary(item, with_body=True)

    @server.tool
    def list_pull_requests(
        repo: str,
        state: Literal["open", "closed", "all"] = "open",
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """List pull requests, most recently updated first.

        Args:
            repo: Repository as 'owner/name'.
            state: open, closed, or all.
            limit: Maximum pull requests to return (1-100).
        """
        _check_limit(limit)
        items = gh.request(
            "GET",
            _repo_path(repo, "pulls"),
            params={"state": state, "per_page": limit, "sort": "updated", "direction": "desc"},
        )
        return [_pull_summary(item, with_body=False) for item in items or []]

    @server.tool
    def get_pull_request(repo: str, number: int) -> dict[str, Any]:
        """Get one pull request including its body and diff statistics.

        Args:
            repo: Repository as 'owner/name'.
            number: Pull request number.
        """
        item = gh.request("GET", _repo_path(repo, "pulls", str(number)))
        return _pull_summary(item, with_body=True)

    @server.tool
    def get_file(repo: str, path: str, ref: str | None = None) -> dict[str, Any]:
        """Read a file (or list a directory) from a repository.

        Args:
            repo: Repository as 'owner/name'.
            path: Path inside the repository; '' or '/' for the root directory.
            ref: Branch, tag, or commit SHA; defaults to the default branch.
        """
        clean = path.strip("/")
        segments = ("contents", clean) if clean else ("contents",)
        item = gh.request("GET", _repo_path(repo, *segments), params={"ref": ref})
        if isinstance(item, list):
            return {
                "path": clean,
                "type": "dir",
                "entries": [
                    {"name": e.get("name"), "type": e.get("type"), "size": e.get("size")}
                    for e in item
                ],
            }
        result: dict[str, Any] = {
            "path": item.get("path"),
            "type": item.get("type"),
            "size": item.get("size"),
            "sha": item.get("sha"),
        }
        if item.get("encoding") == "base64" and item.get("content") is not None:
            raw = base64.b64decode(item["content"])
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                result["binary"] = True
                return result
            result["truncated"] = len(text) > MAX_FILE_CHARS
            result["content"] = text[:MAX_FILE_CHARS]
        elif item.get("type") == "file":
            # Files over GitHub's inline size limit have no content field.
            result["too_large"] = True
        return result

    if enable_write:

        @server.tool(scopes=(WRITE_SCOPE,))
        def create_issue(repo: str, title: str, body: str = "") -> dict[str, Any]:
            """Open a new issue.

            Args:
                repo: Repository as 'owner/name'.
                title: Issue title.
                body: Issue body in Markdown.
            """
            if not title.strip():
                raise ToolError("title must not be empty")
            item = gh.request(
                "POST", _repo_path(repo, "issues"), body={"title": title, "body": body}
            )
            return _issue_summary(item, with_body=False)

        @server.tool(scopes=(WRITE_SCOPE,))
        def comment_on_issue(repo: str, number: int, body: str) -> dict[str, Any]:
            """Add a comment to an issue or pull request.

            Args:
                repo: Repository as 'owner/name'.
                number: Issue or pull request number.
                body: Comment body in Markdown.
            """
            if not body.strip():
                raise ToolError("body must not be empty")
            item = gh.request(
                "POST",
                _repo_path(repo, "issues", str(number), "comments"),
                body={"body": body},
            )
            return {
                "id": item.get("id"),
                "author": _user(item.get("user")),
                "created_at": item.get("created_at"),
                "url": item.get("html_url"),
            }

    return server


# ---------------------------------------------------------------------- CLI


def main(argv: Sequence[str] | None = None) -> None:
    """Command-line entry point (``easy-mcp-github``)."""
    parser = _cli.build_parser(
        "Serve GitHub repositories, issues, pull requests and files over MCP. "
        f"Reads the token from ${TOKEN_ENV_VAR}."
    )
    parser.add_argument(
        "--allow-write",
        action="store_true",
        help=f"register create_issue/comment_on_issue (need the '{WRITE_SCOPE}' scope)",
    )

    def build(args: argparse.Namespace) -> MCPServer:
        return build_server(enable_write=args.allow_write, **_cli.server_kwargs(args))

    _cli.run(build, parser, argv)


if __name__ == "__main__":
    main()
