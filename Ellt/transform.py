from __future__ import annotations

from typing import Any

import pandas as pd

from extract import SOURCE_COLUMNS


FINANCIAL_COLUMNS = (
    "Order Value",
    "Delivery Fee",
    "Commission Fee",
    "Payment Processing Fee",
    "Refunds/Chargebacks",
)
REVENUE_COMPONENTS = (
    "Commission Fee",
    "Delivery Fee",
    "Payment Processing Fee",
    "Refunds/Chargebacks",
)
TRANSFORMED_COLUMNS = (
    *SOURCE_COLUMNS,
    "Delivery_Time_Minutes",
    "Order_Date",
    "Order_Hour",
    "Platform_Net_Revenue",
)


def _append_reason(reasons: pd.Series, mask: pd.Series, reason: str) -> None:
    if mask.any():
        existing = reasons.loc[mask]
        reasons.loc[mask] = existing.map(
            lambda value: f"{value}; {reason}" if value else reason
        )


def transform_data(
    source: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Clean records globally so exact duplicates spanning batches are found."""
    missing_columns = [column for column in SOURCE_COLUMNS if column not in source]
    if missing_columns:
        raise ValueError(f"Source data is missing columns: {missing_columns}")

    raw = source.copy()
    if "source_row_number" not in raw:
        raw.insert(0, "source_row_number", range(1, len(raw) + 1))
    duplicate_mask = raw.duplicated(subset=list(SOURCE_COLUMNS), keep="first")
    repeated_ids_before = int(
        raw.loc[raw.duplicated("Order ID", keep=False), "Order ID"].nunique()
    )
    duplicate_rejections = raw.loc[duplicate_mask].copy()
    duplicate_rejections["rejection_reason"] = "exact_duplicate_row"
    work = raw.loc[~duplicate_mask].copy()
    reasons = pd.Series("", index=work.index, dtype="string")

    order_id = pd.to_numeric(work["Order ID"], errors="coerce")
    invalid_order_id = (
        work["Order ID"].isna()
        | order_id.isna()
        | order_id.mod(1).ne(0).fillna(False)
    )
    _append_reason(reasons, invalid_order_id, "invalid_order_id")
    work["Order ID"] = order_id.where(~invalid_order_id).astype("Int64")

    for column in FINANCIAL_COLUMNS:
        original = work[column]
        numeric = pd.to_numeric(original, errors="coerce")
        nonblank = original.notna() & original.astype("string").str.strip().ne("")
        _append_reason(
            reasons,
            nonblank & numeric.isna(),
            f"invalid_{column.lower().replace('/', '_').replace(' ', '_')}",
        )
        _append_reason(reasons, numeric.lt(0).fillna(False), f"negative_{column}")
        work[column] = numeric.astype("Float64")

    order_time = pd.to_datetime(work["Order Date and Time"], errors="coerce")
    delivery_time = pd.to_datetime(work["Delivery Date and Time"], errors="coerce")
    _append_reason(
        reasons,
        order_time.isna(),
        "missing_or_invalid_order_timestamp",
    )
    _append_reason(
        reasons,
        delivery_time.isna(),
        "missing_or_invalid_delivery_timestamp",
    )
    duration = (delivery_time - order_time).dt.total_seconds() / 60
    _append_reason(
        reasons,
        duration.lt(0).fillna(False),
        "delivery_before_order",
    )

    work["Order Date and Time"] = order_time
    work["Delivery Date and Time"] = delivery_time
    work["Delivery_Time_Minutes"] = duration.astype("Float64")
    work["Order_Date"] = order_time.dt.date
    work["Order_Hour"] = order_time.dt.hour.astype("Int64")

    revenue_components_present = work.loc[:, REVENUE_COMPONENTS].notna().all(axis=1)
    work["Platform_Net_Revenue"] = pd.Series(
        pd.NA, index=work.index, dtype="Float64"
    )
    work.loc[revenue_components_present, "Platform_Net_Revenue"] = (
        work.loc[revenue_components_present, "Commission Fee"]
        + work.loc[revenue_components_present, "Delivery Fee"]
        - work.loc[revenue_components_present, "Payment Processing Fee"]
        - work.loc[revenue_components_present, "Refunds/Chargebacks"]
    )

    rejected_mask = reasons.ne("")
    invalid_rejections = raw.loc[work.index[rejected_mask]].copy()
    invalid_rejections["rejection_reason"] = reasons.loc[rejected_mask].values
    rejected = pd.concat(
        [duplicate_rejections, invalid_rejections], ignore_index=True, sort=False
    )
    accepted = work.loc[~rejected_mask].copy()
    accepted = accepted.loc[:, list(TRANSFORMED_COLUMNS) + [
        column for column in ("source_row_number", "load_run_id") if column in accepted
    ]]

    repeated_ids_after = int(
        accepted.loc[
            accepted.duplicated("Order ID", keep=False), "Order ID"
        ].nunique()
    )
    details: dict[str, Any] = {
        "input_records": len(raw),
        "exact_duplicate_rows_removed": int(duplicate_mask.sum()),
        "accepted_records": len(accepted),
        "rejected_records": len(rejected),
        "repeated_order_ids_before_cleaning": repeated_ids_before,
        "repeated_order_ids_after_cleaning": repeated_ids_after,
        "negative_delivery_durations_rejected": int(
            reasons.str.contains("delivery_before_order", regex=False).sum()
        ),
        "missing_values_before": raw.isna().sum().to_dict(),
        "missing_values_after": accepted.isna().sum().to_dict(),
        "revenue_records_included": int(accepted["Platform_Net_Revenue"].notna().sum()),
    }
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True), details


def transform_batches(
    batches: list[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Combine fetched batches before deduplication to catch cross-batch repeats."""
    if not batches:
        raise ValueError("No raw batches were provided for transformation.")
    return transform_data(pd.concat(batches, ignore_index=True))
