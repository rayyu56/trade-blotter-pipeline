"""Silver layer – validation: enforce schema and business rules on bronze data.

Rejects or flags records that fail:
- Required field presence (trade_id, instrument, quantity, price, trade_date)
- Type and range checks (e.g. quantity > 0, valid settlement dates)
- Referential integrity (known counterparties, valid instrument codes)
"""
