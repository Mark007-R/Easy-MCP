"""A real-world example: query your AWS account through MCP tools.

Requires boto3 (``pip install boto3``) and AWS credentials configured the
standard way — environment variables, ``~/.aws/credentials``, or a role.

Prefer a ``.env`` file? Install ``python-dotenv`` and this example loads it
automatically. Put it next to where you run the server, and NEVER commit it
(this repo's .gitignore already excludes ``.env``)::

    AWS_ACCESS_KEY_ID=...
    AWS_SECRET_ACCESS_KEY=...
    AWS_DEFAULT_REGION=us-east-1
    EASY_MCP_AWS_KEY=a-long-random-string

Cloud data is worth protecting, so this example shows per-tool scopes: set
``EASY_MCP_AWS_KEY`` to a long random value and both tools require an API
key holding the ``aws`` scope (clients send ``Authorization: Bearer <key>``).
Without the variable the server runs open on localhost for a quick try-out.

Run it:

    python examples/aws_server.py

Then connect any MCP client, e.g.:

    claude mcp add --transport sse aws http://127.0.0.1:8000/sse
"""

from __future__ import annotations

import os

from easy_mcp import APIKeyAuth, MCPServer, ToolError

try:
    from dotenv import load_dotenv
except ImportError:  # python-dotenv is optional — the standard AWS chain still works
    pass
else:
    load_dotenv()

api_key = os.environ.get("EASY_MCP_AWS_KEY")
auth = APIKeyAuth({api_key: ["aws"]}) if api_key else None
scopes = ("aws",) if auth else ()

server = MCPServer(
    port=8000,
    name="aws-demo",
    auth=auth,
    instructions="Read-only AWS account queries: S3 buckets and EC2 instances.",
)


def _aws():
    """Import boto3 lazily so the server starts even without it installed."""
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        raise ToolError("boto3 is not installed — run: pip install boto3") from None
    return boto3, (BotoCoreError, ClientError)


@server.tool(scopes=scopes, tags=("aws", "s3"), category="cloud", timeout=20.0)
def list_s3_buckets() -> list[dict]:
    """List the S3 buckets in the configured AWS account."""
    boto3, aws_errors = _aws()
    try:
        response = boto3.client("s3").list_buckets()
    except aws_errors as exc:
        raise ToolError(f"AWS error: {exc}") from None
    return [
        {"name": bucket["Name"], "created": bucket["CreationDate"].isoformat()}
        for bucket in response.get("Buckets", [])
    ]


@server.tool(scopes=scopes, tags=("aws", "ec2"), category="cloud", timeout=20.0)
def ec2_instance_summary(region: str = "us-east-1") -> list[dict]:
    """Summarize EC2 instances in one region: id, type, and state.

    Args:
        region: AWS region name, e.g. "us-east-1" or "ap-south-1".
    """
    boto3, aws_errors = _aws()
    try:
        reservations = boto3.client("ec2", region_name=region).describe_instances()
    except aws_errors as exc:
        raise ToolError(f"AWS error: {exc}") from None
    return [
        {
            "id": instance["InstanceId"],
            "type": instance["InstanceType"],
            "state": instance["State"]["Name"],
        }
        for reservation in reservations.get("Reservations", [])
        for instance in reservation["Instances"]
    ]


if __name__ == "__main__":
    server.run()
