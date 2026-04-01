"""Bronze layer: ingest raw trade blotter data as-is from source files or databases.

No transformations or validation are applied here. Data is loaded into a
DataFrame and written to data/bronze/ preserving the original structure.
Supported sources: Excel (.xlsx), CSV, SQLAlchemy-compatible databases.
"""
