"""Queries Delta tables via databricks-sql-connector to verify row counts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from databricks import sql as dbsql


@dataclass
class QualityReport:
    table_counts: Dict[str, int] = field(default_factory=dict)
    empty_tables: List[str] = field(default_factory=list)
    passed: bool = True


def check_table_counts(
    host: str,
    token: str,
    http_path: str,
    tables: Dict[str, str],
    catalog: str = "hive_metastore",
    schema: str = "default",
) -> QualityReport:
    """COUNT(*) each Delta table and flag any that are empty.

    Args:
        host: Databricks workspace hostname (no ``https://`` prefix required;
              the connector accepts either form).
        token: PAT or OAuth token.
        http_path: SQL Warehouse HTTP path, e.g.
            ``/sql/1.0/warehouses/abc123``.
        tables: Mapping of logical name → actual table name to query.
        catalog: Unity Catalog catalog name (default ``hive_metastore``).
        schema: Schema / database name (default ``default``).

    Returns:
        :class:`QualityReport` with per-table counts and an ``empty_tables``
        list for quick triage.

    Raises:
        databricks.sql.exc.Error: On connection or query failure.
    """
    # Strip scheme if present — the connector wants the bare hostname.
    bare_host = host.replace("https://", "").replace("http://", "").rstrip("/")

    report = QualityReport()

    with dbsql.connect(
        server_hostname=bare_host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"USE {catalog}.{schema}")

            for logical_name, table_name in tables.items():
                cursor.execute(f"SELECT COUNT(*) AS n FROM {table_name}")
                row = cursor.fetchone()
                count = int(row[0]) if row else 0
                report.table_counts[logical_name] = count
                if count == 0:
                    report.empty_tables.append(logical_name)

    report.passed = len(report.empty_tables) == 0
    return report
