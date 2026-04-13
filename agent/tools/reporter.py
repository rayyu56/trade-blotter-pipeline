"""Calls the Claude API (claude-sonnet-4-6) to generate an EOD pipeline summary."""

from __future__ import annotations

from typing import Any, Dict

import anthropic


def generate_eod_summary(api_key: str, run_record: Dict[str, Any]) -> str:
    """Ask Claude to write a concise end-of-day pipeline summary.

    Args:
        api_key: Anthropic API key.
        run_record: The completed run record that will be stored in
            ``run_history.json``.  It should contain keys such as
            ``source_file``, ``run_id``, ``status``, ``quality_report``,
            ``reconciliation``, and ``triggered_at`` / ``completed_at``.

    Returns:
        A markdown-formatted summary string produced by Claude.
    """
    client = anthropic.Anthropic(api_key=api_key)

    prompt = _build_prompt(run_record)

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text


def _build_prompt(run_record: Dict[str, Any]) -> str:
    import json

    record_json = json.dumps(run_record, indent=2, default=str)
    return f"""You are a DataOps analyst. A trade blotter pipeline just finished running.
Write a concise end-of-day summary (5–10 bullet points) suitable for a daily ops report.
Cover: what file was processed, run outcome, row counts, reconciliation status, any quality
issues, and any recommended follow-up actions.

Pipeline run record:
```json
{record_json}
```

Return the summary in GitHub-flavoured Markdown."""
