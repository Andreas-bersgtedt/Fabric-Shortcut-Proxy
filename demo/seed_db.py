"""
Seed the SQLite demo database with a realistic sales table.

Called automatically at startup when using the SQLite backend.
Idempotent: skips seeding if the table already has rows.
"""
from __future__ import annotations

import random
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import config
from observability.logging import get_logger

log = get_logger(__name__)

_PRODUCTS = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Service Pack", "Support Plan"]
_REGIONS  = ["North", "South", "East", "West", "Central"]

_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {config.DB_SOURCE_TABLE} (
    id          INTEGER PRIMARY KEY,
    order_date  TEXT,
    customer_id INTEGER,
    product     TEXT,
    quantity    INTEGER,
    unit_price  REAL,
    total       REAL,
    region      TEXT
)
"""

_SEED_ROWS = 50_000   # enough to fill all 8 splits with ~6 250 rows each


async def seed_demo_database() -> None:
    engine = create_async_engine(config.DB_URL, echo=False)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(_CREATE_TABLE))
            count = (await conn.execute(text(f"SELECT COUNT(*) FROM {config.DB_SOURCE_TABLE}"))).scalar()
            if count and count > 0:
                log.info("seed_skip", existing_rows=count)
                return

            log.info("seed_start", rows=_SEED_ROWS)
            rng = random.Random(42)
            base_date = date(2023, 1, 1)
            rows = []
            for i in range(1, _SEED_ROWS + 1):
                order_date   = base_date + timedelta(days=rng.randint(0, 365))
                customer_id  = rng.randint(1, 5000)
                product      = rng.choice(_PRODUCTS)
                quantity     = rng.randint(1, 100)
                unit_price   = round(rng.uniform(9.99, 999.99), 2)
                total        = round(quantity * unit_price, 2)
                region       = rng.choice(_REGIONS)
                rows.append((i, order_date.isoformat(), customer_id, product, quantity, unit_price, total, region))

            await conn.execute(
                text(
                    f"INSERT INTO {config.DB_SOURCE_TABLE} "
                    f"(id, order_date, customer_id, product, quantity, unit_price, total, region) "
                    f"VALUES (:id, :order_date, :customer_id, :product, :quantity, :unit_price, :total, :region)"
                ),
                [
                    {
                        "id": r[0], "order_date": r[1], "customer_id": r[2],
                        "product": r[3], "quantity": r[4], "unit_price": r[5],
                        "total": r[6], "region": r[7],
                    }
                    for r in rows
                ],
            )
            log.info("seed_complete", rows=_SEED_ROWS)
    finally:
        await engine.dispose()
