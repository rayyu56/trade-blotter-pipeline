"""Silver layer – cleaning: normalize and standardize validated bronze data.

Responsibilities:
- Standardize field names and data types
- Normalize counterparty and instrument identifiers
- Parse and unify date/time formats
- Derive base fields (e.g. notional = quantity * price)
Output written to data/silver/.
"""
