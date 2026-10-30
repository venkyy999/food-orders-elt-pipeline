from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import BATCH_SIZE, RAW_DATABASE, create_database_engine, load_run_id
from extract import SOURCE_COLUMNS


def retrieve_raw_batches(
    engine: Engine,
    run_id: str,
    batch_size: int = BATCH_SIZE,
) -> Iterator[pd.DataFrame]:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero.")
    columns = ("source_row_number", "load_run_id", *SOURCE_COLUMNS)
    selected = ", ".join(f"`{column}`" for column in columns)
    last_row_number = 0
    batch_number = 0
    while True:
        query = text(
            f"SELECT {selected} FROM raw_food_orders "
            "WHERE load_run_id = :run_id "
            "AND source_row_number > :last_row_number "
            "ORDER BY source_row_number LIMIT :batch_size"
        )
        with engine.connect() as connection:
            batch = pd.read_sql(
                query,
                connection,
                params={
                    "run_id": run_id,
                    "last_row_number": last_row_number,
                    "batch_size": batch_size,
                },
            )
        if batch.empty:
            break
        batch_number += 1
        last_row_number = int(batch["source_row_number"].max())
        print(f"Retrieved batch {batch_number}: {len(batch)} records")
        yield batch


def retrieve_all_raw(engine: Engine, run_id: str) -> pd.DataFrame:
    batches = list(retrieve_raw_batches(engine, run_id))
    if not batches:
        return pd.DataFrame(columns=("source_row_number", "load_run_id", *SOURCE_COLUMNS))
    return pd.concat(batches, ignore_index=True)


def main() -> None:
    engine = create_database_engine(RAW_DATABASE)
    try:
        run_id = load_run_id()
        frame = retrieve_all_raw(engine, run_id)
        print(f"Retrieved {len(frame)} raw records for the current source.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
