from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config import BATCH_SIZE, RAW_DATABASE, SOURCE_CSV, create_database_engine, load_run_id
from extract import SOURCE_COLUMNS, extract_batches
from pipeline_errors import BatchLoadError


def create_raw_table(engine: Engine) -> None:
    columns = ",\n".join(f"`{column}` LONGTEXT NULL" for column in SOURCE_COLUMNS)
    statement = f"""
        CREATE TABLE IF NOT EXISTS raw_food_orders (
            source_row_number BIGINT NOT NULL,
            load_run_id VARCHAR(80) NOT NULL,
            {columns},
            PRIMARY KEY (load_run_id, source_row_number)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with engine.begin() as connection:
        connection.exec_driver_sql(statement)


def _as_database_value(value):
    if pd.isna(value):
        return None
    return str(value)


def load_raw_batches(
    engine: Engine,
    source_path=SOURCE_CSV,
    batch_size: int = BATCH_SIZE,
    run_id: str | None = None,
) -> dict[str, int | str]:
    """Append raw batches; the unique run/row key makes a full rerun idempotent."""
    run_id = run_id or load_run_id(source_path)
    create_raw_table(engine)
    columns = ("source_row_number", "load_run_id", *SOURCE_COLUMNS)
    quoted = ", ".join(f"`{column}`" for column in columns)
    placeholders = ", ".join(f":p{index}" for index in range(len(columns)))
    insert = text(
        f"INSERT INTO raw_food_orders ({quoted}) VALUES ({placeholders}) "
        "ON DUPLICATE KEY UPDATE "
        "source_row_number = VALUES(source_row_number)"
    )
    extracted_count = 0
    batch_count = 0
    successful_batches = 0
    failed_batches = 0

    for batch_number, batch in enumerate(
        extract_batches(source_path, batch_size), start=1
    ):
        batch_count += 1
        extracted_count += len(batch)
        records = []
        for row in batch.itertuples(index=False, name=None):
            source_row = dict(zip(batch.columns, row))
            record = {
                f"p{index}": source_row["source_row_number"]
                if column == "source_row_number"
                else run_id
                if column == "load_run_id"
                else _as_database_value(source_row[column])
                for index, column in enumerate(columns)
            }
            records.append(record)
        try:
            with engine.begin() as connection:
                connection.execute(insert, records)
            successful_batches += 1
        except SQLAlchemyError as exc:
            failed_batches += 1
            raise BatchLoadError(
                f"Raw MySQL insertion failed for batch {batch_number} "
                f"({len(batch)} rows): {type(exc).__name__}",
                successful_batches,
                failed_batches,
            ) from exc

    with engine.connect() as connection:
        loaded_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM raw_food_orders "
                "WHERE load_run_id = :run_id"
            ),
            {"run_id": run_id},
        ).scalar_one()
    if loaded_count != extracted_count:
        raise BatchLoadError(
            "Raw row-count verification failed: "
            f"CSV has {extracted_count}, database has {loaded_count} "
            f"for load run {run_id}.",
            successful_batches,
            failed_batches,
        )
    return {
        "load_run_id": run_id,
        "extracted_records": extracted_count,
        "raw_records_loaded": int(loaded_count),
        "batches_processed": batch_count,
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
    }


def main() -> None:
    engine = create_database_engine(RAW_DATABASE)
    try:
        result = load_raw_batches(engine)
        print(f"Verified {result['raw_records_loaded']} raw records.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
