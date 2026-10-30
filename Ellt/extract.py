from __future__ import annotations

from collections.abc import Iterator

import pandas as pd

from config import BATCH_SIZE, SOURCE_CSV


SOURCE_COLUMNS = (
    "Order ID",
    "Customer ID",
    "Restaurant ID",
    "Order Date and Time",
    "Delivery Date and Time",
    "Order Value",
    "Delivery Fee",
    "Payment Method",
    "Discounts and Offers",
    "Commission Fee",
    "Payment Processing Fee",
    "Refunds/Chargebacks",
    "Weather Condition",
    "Order Channel",
)


def extract_batches(
    source_path=SOURCE_CSV, batch_size: int = BATCH_SIZE
) -> Iterator[pd.DataFrame]:
    """Yield untouched CSV records in source order, with a lineage row number."""
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero.")
    if not source_path.is_file():
        raise FileNotFoundError(f"Source CSV does not exist: {source_path}")

    next_row_number = 1
    for batch_number, batch in enumerate(
        pd.read_csv(source_path, dtype="string", chunksize=batch_size), start=1
    ):
        if tuple(batch.columns) != SOURCE_COLUMNS:
            raise ValueError(
                "The source CSV columns do not match the inspected food-orders "
                f"schema. Found: {list(batch.columns)}"
            )
        batch.insert(
            0,
            "source_row_number",
            range(next_row_number, next_row_number + len(batch)),
        )
        next_row_number += len(batch)
        print(f"Extracted batch {batch_number}: {len(batch)} records")
        yield batch
