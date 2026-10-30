from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from config import (
    ANALYTICS_DATABASE,
    BATCH_SIZE,
    PROJECT_DIR,
    RAW_DATABASE,
    REPORTS_DIR,
    SOURCE_CSV,
    create_database_engine,
    load_run_id,
)
from extract import extract_batches
from load_raw import load_raw_batches
from load_transformed import load_transformed_batches
from pipeline_errors import BatchLoadError
from retrieve_raw import retrieve_raw_batches
from transform import transform_data
from validate import save_reports, validate_transformed


def _write_execution_report(report: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    output = REPORTS_DIR / "execution_report.json"
    output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")


def run_pipeline() -> dict:
    run_id = load_run_id(SOURCE_CSV)
    extracted_batches = list(extract_batches(SOURCE_CSV, BATCH_SIZE))
    extracted = pd.concat(extracted_batches, ignore_index=True)
    extracted["load_run_id"] = run_id
    input_count = len(extracted)

    accepted, rejected, details = transform_data(extracted)
    validation = validate_transformed(
        accepted, rejected, input_count, extracted, details
    )
    save_reports(extracted, accepted, rejected, validation)
    cleaned_csv = PROJECT_DIR / "food_orders_cleaned.csv"
    accepted.drop(
        columns=["source_row_number", "load_run_id"], errors="ignore"
    ).to_csv(cleaned_csv, index=False)

    report = {
        "source_file": str(SOURCE_CSV),
        "load_run_id": run_id,
        "batch_size": BATCH_SIZE,
        "total_records_extracted": input_count,
        "total_batches_processed": len(extracted_batches),
        "total_raw_records_loaded_into_mysql": None,
        "total_raw_records_retrieved_from_mysql": None,
        "total_valid_records_transformed": len(accepted),
        "total_rejected_records": len(rejected),
        "total_transformed_records_loaded_into_analytics_database": None,
        "successful_batches": 0,
        "failed_batches": 0,
        "database_status": "NOT_RUN",
        "database_error": None,
        "validation_status": (
            "PASS" if validation["status"].eq("PASS").all() else "FAIL"
        ),
        "cleaned_csv": str(cleaned_csv),
        "validation_checks": len(validation),
    }

    raw_engine = None
    analytics_engine = None
    try:
        raw_engine = create_database_engine(RAW_DATABASE)
        raw_load = load_raw_batches(raw_engine, SOURCE_CSV, BATCH_SIZE, run_id)
        report["total_raw_records_loaded_into_mysql"] = raw_load[
            "raw_records_loaded"
        ]
        report["successful_batches"] += raw_load["successful_batches"]
        report["failed_batches"] += raw_load["failed_batches"]

        retrieved_batches = list(retrieve_raw_batches(raw_engine, run_id, BATCH_SIZE))
        retrieved = pd.concat(retrieved_batches, ignore_index=True)
        report["total_raw_records_retrieved_from_mysql"] = len(retrieved)
        if len(retrieved) != input_count:
            raise RuntimeError(
                f"Retrieved {len(retrieved)} rows, expected {input_count}."
            )

        db_accepted, db_rejected, db_details = transform_data(retrieved)
        db_validation = validate_transformed(
            db_accepted, db_rejected, input_count, retrieved, db_details
        )
        save_reports(retrieved, db_accepted, db_rejected, db_validation)
        if not db_validation["status"].eq("PASS").all():
            raise RuntimeError("Transformed data failed validation.")
        if len(db_accepted) != len(accepted) or len(db_rejected) != len(rejected):
            raise RuntimeError(
                "Database-retrieved transform counts differ from the local run."
            )
        accepted = db_accepted
        report["total_valid_records_transformed"] = len(db_accepted)
        report["total_rejected_records"] = len(db_rejected)
        accepted.drop(
            columns=["source_row_number", "load_run_id"], errors="ignore"
        ).to_csv(cleaned_csv, index=False)

        analytics_engine = create_database_engine(ANALYTICS_DATABASE)
        analytics_load = load_transformed_batches(
            analytics_engine, accepted, run_id, BATCH_SIZE
        )
        report["total_transformed_records_loaded_into_analytics_database"] = (
            analytics_load["analytics_records_loaded"]
        )
        report["successful_batches"] += analytics_load[
            "analytics_batches_successful"
        ]
        report["failed_batches"] += analytics_load["analytics_batches_failed"]
        report["database_status"] = "PASS"
    except BatchLoadError as exc:
        report["database_status"] = "FAILED"
        report["successful_batches"] += exc.successful_batches
        report["failed_batches"] += exc.failed_batches
        report["database_error"] = f"{type(exc).__name__}: {exc}"
        print(
            "A MySQL batch failed; the database stages did not complete. "
            f"Database error: {report['database_error']}"
        )
    except SQLAlchemyError as exc:
        report["database_status"] = "FAILED"
        report["database_error"] = (
            f"{type(exc).__name__}: {getattr(getattr(exc, 'orig', None), 'errno', '') or type(exc).__name__}"
        )
        print(
            "MySQL stages were not completed. Local extraction, transformation, "
            "validation, and reports are available. "
            f"Database error: {report['database_error']}"
        )
    except (RuntimeError, OSError) as exc:
        report["database_status"] = "FAILED_OR_NOT_CONFIGURED"
        report["database_error"] = (
            f"{type(exc).__name__}: {getattr(getattr(exc, 'orig', None), 'errno', '') or str(exc)}"
        )
        print(
            "MySQL stages were not completed. Local extraction, transformation, "
            "validation, and reports are available. "
            f"Database error: {report['database_error']}"
        )
    finally:
        if raw_engine is not None:
            raw_engine.dispose()
        if analytics_engine is not None:
            analytics_engine.dispose()

    _write_execution_report(report)
    print(json.dumps(report, indent=2, default=str))
    return report


if __name__ == "__main__":
    run_pipeline()
