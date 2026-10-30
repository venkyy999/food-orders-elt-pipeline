from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config import BATCH_SIZE
from pipeline_errors import BatchLoadError
from transform import TRANSFORMED_COLUMNS


SQL_TYPES = {
    "Order ID": "BIGINT NOT NULL",
    "Customer ID": "VARCHAR(100) NULL",
    "Restaurant ID": "VARCHAR(100) NULL",
    "Order Date and Time": "DATETIME NULL",
    "Delivery Date and Time": "DATETIME NULL",
    "Order Value": "DOUBLE NULL",
    "Delivery Fee": "DOUBLE NULL",
    "Payment Method": "VARCHAR(255) NULL",
    "Discounts and Offers": "VARCHAR(255) NULL",
    "Commission Fee": "DOUBLE NULL",
    "Payment Processing Fee": "DOUBLE NULL",
    "Refunds/Chargebacks": "DOUBLE NULL",
    "Weather Condition": "VARCHAR(100) NULL",
    "Order Channel": "VARCHAR(100) NULL",
    "Delivery_Time_Minutes": "DOUBLE NULL",
    "Order_Date": "DATE NULL",
    "Order_Hour": "TINYINT NULL",
    "Platform_Net_Revenue": "DOUBLE NULL",
}


def create_analytics_table(engine: Engine) -> None:
    definitions = [
        "source_row_number BIGINT NOT NULL",
        "load_run_id VARCHAR(80) NOT NULL",
        *(f"`{column}` {SQL_TYPES[column]}" for column in TRANSFORMED_COLUMNS),
        "PRIMARY KEY (load_run_id, source_row_number)",
    ]
    statement = (
        "CREATE TABLE IF NOT EXISTS cleaned_food_orders (\n"
        + ",\n".join(definitions)
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )
    with engine.begin() as connection:
        connection.exec_driver_sql(statement)


def _database_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, (date, datetime)):
        return value
    if hasattr(value, "item"):
        return value.item()
    return value


def load_transformed_batches(
    engine: Engine,
    accepted: pd.DataFrame,
    run_id: str,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int | str]:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero.")
    if "load_run_id" not in accepted:
        accepted = accepted.copy()
        accepted["load_run_id"] = run_id
    if "source_row_number" not in accepted:
        raise ValueError("Accepted records require source_row_number lineage.")

    create_analytics_table(engine)
    columns = ("source_row_number", "load_run_id", *TRANSFORMED_COLUMNS)
    quoted = ", ".join(f"`{column}`" for column in columns)
    placeholders = ", ".join(f":p{index}" for index in range(len(columns)))
    insert = text(
        f"INSERT INTO cleaned_food_orders ({quoted}) VALUES ({placeholders}) "
        "ON DUPLICATE KEY UPDATE "
        "source_row_number = VALUES(source_row_number)"
    )
    successful_batches = 0
    batch_number = 0

    for offset in range(0, len(accepted), batch_size):
        batch_number += 1
        batch = accepted.iloc[offset : offset + batch_size]
        records = []
        for row in batch.loc[:, list(columns)].itertuples(index=False, name=None):
            records.append(
                {f"p{index}": _database_value(value) for index, value in enumerate(row)}
            )
        try:
            with engine.begin() as connection:
                connection.execute(insert, records)
            successful_batches += 1
        except SQLAlchemyError as exc:
            raise BatchLoadError(
                f"Analytics MySQL insertion failed for batch {batch_number} "
                f"({len(batch)} rows): {type(exc).__name__}",
                successful_batches,
                1,
            ) from exc

    with engine.connect() as connection:
        loaded_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM cleaned_food_orders "
                "WHERE load_run_id = :run_id"
            ),
            {"run_id": run_id},
        ).scalar_one()
    if loaded_count != len(accepted):
        raise BatchLoadError(
            "Analytics row-count verification failed: "
            f"{len(accepted)} accepted records, {loaded_count} loaded "
            f"for run {run_id}.",
            successful_batches,
            0,
        )
    return {
        "analytics_records_loaded": int(loaded_count),
        "analytics_batches_processed": batch_number,
        "analytics_batches_successful": successful_batches,
        "analytics_batches_failed": batch_number - successful_batches,
    }


def main() -> None:
    raise SystemExit(
        "Run the analytics load through elt_batch_pipeline.py after validation."
    )


if __name__ == "__main__":
    main()
