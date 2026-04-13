"""Triggers a Databricks notebook run via the Jobs REST API (runs/submit)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class TriggerResult:
    run_id: int
    run_url: str


def trigger_notebook_run(
    host: str,
    token: str,
    notebook_path: str,
    cluster_id: Optional[str] = None,
    run_name: str = "trade-blotter-pipeline",
) -> TriggerResult:
    """Submit a one-time notebook run via ``POST /api/2.1/jobs/runs/submit``.

    If *cluster_id* is empty a new ephemeral job cluster running the default
    Databricks Runtime is requested automatically.

    Args:
        host: Databricks workspace URL (``https://...azuredatabricks.net``).
        token: Personal access token or service-principal OAuth token.
        notebook_path: Absolute Workspace path to the notebook.
        cluster_id: Existing interactive cluster ID, or ``""`` / ``None`` to
            provision a fresh job cluster.
        run_name: Display name shown in the Runs UI.

    Returns:
        :class:`TriggerResult` with the new run ID and a direct URL.

    Raises:
        requests.HTTPError: If the API returns a non-2xx status.
    """
    url = f"{host.rstrip('/')}/api/2.1/jobs/runs/submit"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    task: dict = {
        "task_key": "pipeline",
        "notebook_task": {"notebook_path": notebook_path},
    }

    if cluster_id:
        task["existing_cluster_id"] = cluster_id
    else:
        task["new_cluster"] = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 1,
        }

    payload = {"run_name": run_name, "tasks": [task]}

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()

    run_id: int = response.json()["run_id"]
    run_url = f"{host.rstrip('/')}/#job/runs/{run_id}"
    return TriggerResult(run_id=run_id, run_url=run_url)
