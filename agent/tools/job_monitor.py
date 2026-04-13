"""Polls a Databricks run until it reaches a terminal state."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests

# Terminal life-cycle states reported by the API.
_TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

# Result states that indicate success.
_SUCCESS_STATES = {"SUCCESS"}


@dataclass
class MonitorResult:
    run_id: int
    life_cycle_state: str   # e.g. TERMINATED
    result_state: str        # e.g. SUCCESS | FAILED | CANCELED
    state_message: str
    succeeded: bool


def poll_run(
    host: str,
    token: str,
    run_id: int,
    poll_interval_seconds: int = 15,
    max_attempts: int = 40,
) -> MonitorResult:
    """Block until *run_id* finishes or *max_attempts* is exhausted.

    Args:
        host: Databricks workspace URL.
        token: PAT or OAuth token.
        run_id: Run ID returned by :func:`~tools.job_trigger.trigger_notebook_run`.
        poll_interval_seconds: Seconds to sleep between status checks.
        max_attempts: Maximum number of polls before giving up.

    Returns:
        :class:`MonitorResult` describing the final state.

    Raises:
        TimeoutError: If the run does not finish within *max_attempts* polls.
        requests.HTTPError: On API error.
    """
    url = f"{host.rstrip('/')}/api/2.1/jobs/runs/get"
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(1, max_attempts + 1):
        response = requests.get(url, headers=headers, params={"run_id": run_id}, timeout=30)
        response.raise_for_status()
        data = response.json()

        state = data.get("state", {})
        life_cycle_state: str = state.get("life_cycle_state", "UNKNOWN")
        result_state: str = state.get("result_state", "")
        state_message: str = state.get("state_message", "")

        if life_cycle_state in _TERMINAL_STATES:
            return MonitorResult(
                run_id=run_id,
                life_cycle_state=life_cycle_state,
                result_state=result_state,
                state_message=state_message,
                succeeded=result_state in _SUCCESS_STATES,
            )

        time.sleep(poll_interval_seconds)

    raise TimeoutError(
        f"Run {run_id} did not finish after {max_attempts} polls "
        f"({max_attempts * poll_interval_seconds}s)."
    )
