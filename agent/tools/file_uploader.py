"""Uploads a local CSV file to the Databricks workspace via the Import REST API."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path

import requests


@dataclass
class UploadResult:
    local_path: str
    workspace_path: str
    size_bytes: int


def upload_file_to_workspace(
    host: str,
    token: str,
    local_path: str,
    workspace_dir: str,
    overwrite: bool = True,
) -> UploadResult:
    """Upload a local file to a Databricks Workspace directory.

    Uses ``POST /api/2.0/workspace/import`` with ``format=AUTO`` so any file
    type (CSV, JSON, etc.) is accepted and stored as-is.

    Args:
        host: Databricks workspace URL (``https://...``).
        token: Personal access token or OAuth token.
        local_path: Absolute or relative path to the file on the local machine.
        workspace_dir: Target Workspace directory, e.g.
            ``/Users/ray@example.com/project/data/bronze``.
            The filename from *local_path* is preserved.
        overwrite: If ``True`` (default), replace an existing file at the same
            path.  Set to ``False`` to raise on conflict.

    Returns:
        :class:`UploadResult` with local path, workspace destination, and file
        size in bytes.

    Raises:
        FileNotFoundError: If *local_path* does not exist.
        requests.HTTPError: If the Databricks API returns a non-2xx status.
    """
    src = Path(local_path)
    if not src.exists():
        raise FileNotFoundError(f"Local file not found: {src.resolve()}")

    raw = src.read_bytes()
    encoded = base64.b64encode(raw).decode("utf-8")

    # Build destination workspace path — preserve original filename.
    workspace_path = f"{workspace_dir.rstrip('/')}/{src.name}"

    url = f"{host.rstrip('/')}/api/2.0/workspace/import"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "path": workspace_path,
        "format": "AUTO",
        "overwrite": overwrite,
        "content": encoded,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    return UploadResult(
        local_path=str(src.resolve()),
        workspace_path=workspace_path,
        size_bytes=len(raw),
    )
