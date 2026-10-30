from __future__ import annotations

from typing import Any

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

from config import REPORTS_DIR
from extract import SOURCE_COLUMNS
from transform import FINANCIAL_COLUMNS, TRANSFORMED_COLUMNS


def build_data_quality_report(raw: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for column in SOURCE_COLUMNS:
        missing_count = int(raw[column].isna().sum())
        rows.append(
            {
                "check": f"missing_values:{column}",
                "result": missing_count,
                "percentage": round(100 * missing_count / len(raw), 2)
                if len(raw)
                else 0.0,
                "details": "Missing values are retained unless a rule requires rejection.",
            }
        )
    duplicate_count = int(raw.duplicated(subset=list(SOURCE_COLUMNS)).sum())
    repeated_ids = raw.loc[
        raw.duplicated("Order ID", keep=False), "Order ID"
    ].value_counts(dropna=False)
    rows.extend(
        [
            {
                "check": "fully_duplicated_rows",
                "result": duplicate_count,
                "percentage": round(100 * duplicate_count / len(raw), 2)
                if len(raw)
                else 0.0,
                "details": "Exact duplicates are inspected before one copy is excluded.",
            },
            {
                "check": "repeated_order_ids",
                "result": int(len(repeated_ids)),
                "percentage": None,
                "details": "; ".join(
                    f"{order_id} appears {count} times"
                    for order_id, count in repeated_ids.items()
                )
                or "No repeated Order IDs.",
            },
        ]
    )
    for column in ("Order Date and Time", "Delivery Date and Time"):
        parsed = pd.to_datetime(raw[column], errors="coerce")
        rows.append(
            {
                "check": f"unparseable_timestamps:{column}",
                "result": int(parsed.isna().sum()),
                "percentage": round(100 * parsed.isna().mean(), 2),
                "details": "Timestamps are parsed with coercion and invalid records are rejected.",
            }
        )
    for column in FINANCIAL_COLUMNS:
        numeric = pd.to_numeric(raw[column], errors="coerce")
        rows.append(
            {
                "check": f"negative_financial_values:{column}",
                "result": int(numeric.lt(0).sum()),
                "percentage": None,
                "details": "Negative financial values are flagged and rejected.",
            }
        )
    return pd.DataFrame(rows)


def validate_transformed(
    accepted: pd.DataFrame,
    rejected: pd.DataFrame,
    input_count: int,
    raw: pd.DataFrame,
    details: dict[str, Any],
) -> pd.DataFrame:
    checks: list[dict[str, str]] = []

    def add(name: str, passed: bool, result: str, explanation: str) -> None:
        checks.append(
            {
                "check": name,
                "status": "PASS" if passed else "FAIL",
                "actual_result": result,
                "explanation": explanation,
            }
        )

    expected = set(TRANSFORMED_COLUMNS)
    missing = expected.difference(accepted.columns)
    add(
        "expected_cleaned_columns",
        not missing,
        f"missing columns: {sorted(missing)}",
        "All source fields and four requested derived fields must exist.",
    )
    date_types_ok = all(
        is_datetime64_any_dtype(accepted[column])
        for column in ("Order Date and Time", "Delivery Date and Time")
    )
    financial_types_ok = all(
        is_numeric_dtype(accepted[column])
        for column in (*FINANCIAL_COLUMNS, "Delivery_Time_Minutes", "Platform_Net_Revenue")
    )
    add(
        "datetime_and_financial_types",
        date_types_ok and financial_types_ok,
        f"datetime_types_valid={date_types_ok}; financial_types_valid={financial_types_ok}",
        "Timestamps use datetime64 and monetary/duration fields use numeric dtypes.",
    )
    exact_duplicates = int(
        accepted.duplicated(subset=list(SOURCE_COLUMNS)).sum()
    )
    add(
        "duplicate_handling",
        exact_duplicates == 0,
        f"exact duplicates remaining={exact_duplicates}; removed={details['exact_duplicate_rows_removed']}",
        "One copy of each exact duplicate is excluded; distinct transactions are retained.",
    )
    repeated_ids = int(
        accepted.loc[
            accepted.duplicated("Order ID", keep=False), "Order ID"
        ].nunique()
    )
    add(
        "repeated_order_id_investigation",
        True,
        (
            f"repeated IDs before={details['repeated_order_ids_before_cleaning']}; "
            f"after={repeated_ids}"
        ),
        "Repeated IDs are investigated separately and are not removed unless the full row is identical.",
    )
    negative_durations = int(accepted["Delivery_Time_Minutes"].lt(0).sum())
    add(
        "negative_duration_in_valid_records",
        negative_durations == 0,
        f"negative delivery durations among accepted rows={negative_durations}",
        "Negative delivery durations are rejected, not changed to zero.",
    )

    manual = accepted.head(5)
    manual_expected = (
        manual["Delivery Date and Time"] - manual["Order Date and Time"]
    ).dt.total_seconds() / 60
    manual_ok = (
        manual_expected.reset_index(drop=True).equals(
            manual["Delivery_Time_Minutes"].astype("float64").reset_index(drop=True)
        )
        if len(manual)
        else True
    )
    add(
        "manual_delivery_time_calculation",
        bool(manual_ok),
        f"manually checked {len(manual)} accepted records",
        "The first five calculated durations are compared directly with timestamp subtraction.",
    )

    components_present = accepted.loc[
        :, ["Commission Fee", "Delivery Fee", "Payment Processing Fee", "Refunds/Chargebacks"]
    ].notna().all(axis=1)
    expected_revenue = (
        accepted["Commission Fee"]
        + accepted["Delivery Fee"]
        - accepted["Payment Processing Fee"]
        - accepted["Refunds/Chargebacks"]
    )
    revenue_ok = bool(
        (
            accepted.loc[components_present, "Platform_Net_Revenue"]
            .astype("float64")
            .sub(expected_revenue.loc[components_present].astype("float64"))
            .abs()
            .lt(1e-9)
        ).all()
        and accepted.loc[~components_present, "Platform_Net_Revenue"].isna().all()
    )
    add(
        "revenue_formula",
        revenue_ok,
        f"formula verified for {int(components_present.sum())} records with all components",
        "Revenue is computed only when all four components are present.",
    )

    missing_before = int(raw.isna().sum().sum())
    missing_after = int(accepted.isna().sum().sum())
    add(
        "missing_values_before_and_after",
        True,
        f"cell nulls before={missing_before}; accepted output={missing_after}",
        "Missing financial/category values are reported and retained; they are not blanket-imputed.",
    )
    reconciled = len(accepted) + len(rejected) == input_count
    add(
        "record_count_reconciliation",
        reconciled,
        (
            f"input={input_count}; accepted={len(accepted)}; "
            f"rejected_or_excluded={len(rejected)}"
        ),
        "Every source record must be accounted for as accepted or rejected/excluded.",
    )
    return pd.DataFrame(checks)


def save_reports(
    raw: pd.DataFrame,
    accepted: pd.DataFrame,
    rejected: pd.DataFrame,
    validation: pd.DataFrame,
) -> None:
    if len(accepted) + len(rejected) != len(raw):
        raise ValueError(
            "Cannot save reports because accepted and rejected records do not "
            "reconcile to the input row count."
        )
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    build_data_quality_report(raw).to_csv(
        REPORTS_DIR / "data_quality_report.csv", index=False
    )
    validation.to_csv(REPORTS_DIR / "validation_report.csv", index=False)
    rejected.to_csv(REPORTS_DIR / "rejected_records.csv", index=False)


def main() -> None:
    raise SystemExit(
        "Run validation through elt_batch_pipeline.py so the source, accepted, "
        "and rejected record counts are reconciled together."
    )


if __name__ == "__main__":
    main()
