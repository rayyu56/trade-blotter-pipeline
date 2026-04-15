"""Reconciles bronze input row count against silver valid + rejected counts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReconciliationResult:
    bronze_count: int
    silver_valid_count: int
    silver_rejected_count: int
    silver_total: int
    unaccounted: int        # bronze_count - silver_total (should be 0)
    passed: bool
    message: str


def reconcile(
    bronze_count: int,
    silver_valid_count: int,
    silver_rejected_count: int,
) -> ReconciliationResult:
    """Check that every bronze row ended up as either valid or rejected in silver.

    The invariant is::

        bronze_count == silver_valid_count + silver_rejected_count

    Args:
        bronze_count: Row count from the ``bronze_trades`` Delta table.
        silver_valid_count: Row count from the ``silver_valid`` Delta table.
        silver_rejected_count: Row count from the ``silver_rejected`` Delta table.

    Returns:
        :class:`ReconciliationResult` with counts and a human-readable message.
    """
    silver_total = silver_valid_count + silver_rejected_count
    unaccounted = bronze_count - silver_total
    passed = unaccounted == 0

    if passed:
        pct_valid = (silver_valid_count / bronze_count * 100) if bronze_count else 0.0
        message = (
            f"PASS — all {bronze_count:,} bronze rows accounted for "
            f"({silver_valid_count:,} valid / {silver_rejected_count:,} rejected, "
            f"{pct_valid:.1f}% valid rate)."
        )
    else:
        message = (
            f"FAIL — {bronze_count:,} bronze rows but only {silver_total:,} silver rows "
            f"({unaccounted:,} unaccounted)."
        )

    return ReconciliationResult(
        bronze_count=bronze_count,
        silver_valid_count=silver_valid_count,
        silver_rejected_count=silver_rejected_count,
        silver_total=silver_total,
        unaccounted=unaccounted,
        passed=passed,
        message=message,
    )
