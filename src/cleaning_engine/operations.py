"""
Operations module - implements all cleaning operations using Polars.
Each operation is a pure function for testability and composability.
"""

import re
from typing import Any, List

import polars as pl
from opentelemetry import trace

from cleaning_engine.rules import ColumnSelector

tracer = trace.get_tracer(__name__)


def select_columns(df: pl.DataFrame, selector: ColumnSelector) -> List[str]:
    """
    Select columns based on a ColumnSelector.

    Args:
        df: DataFrame to select from
        selector: ColumnSelector specification

    Returns:
        List of selected column names
    """
    if selector.all:
        return df.columns

    if selector.columns:
        return [col for col in selector.columns if col in df.columns]

    if selector.pattern:
        pattern = re.compile(selector.pattern)
        return [col for col in df.columns if pattern.match(col)]

    return df.columns


def drop_nulls(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Drop rows with null values in specified columns."""
    with tracer.start_as_current_span("operation.drop_nulls"):
        if not columns:
            return df
        return df.drop_nulls(subset=columns)


def fill_nulls(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Fill null values with a specified value or strategy."""
    with tracer.start_as_current_span("operation.fill_nulls"):
        if not columns:
            return df

        fill_value = params.get("value")
        strategy = params.get("strategy", "value")

        if strategy == "value" and fill_value is not None:
            return df.with_columns([pl.col(col).fill_null(fill_value) for col in columns])
        elif strategy == "forward":
            return df.with_columns([pl.col(col).forward_fill() for col in columns])
        elif strategy == "backward":
            return df.with_columns([pl.col(col).backward_fill() for col in columns])
        elif strategy == "mean":
            return df.with_columns([pl.col(col).fill_null(pl.col(col).mean()) for col in columns])
        elif strategy == "median":
            return df.with_columns([pl.col(col).fill_null(pl.col(col).median()) for col in columns])

        return df


def drop_duplicates(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Drop duplicate rows based on specified columns."""
    with tracer.start_as_current_span("operation.drop_duplicates"):
        if not columns:
            return df.unique()
        return df.unique(subset=columns, maintain_order=params.get("maintain_order", True))


def trim_whitespace(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Trim whitespace from string columns."""
    with tracer.start_as_current_span("operation.trim_whitespace"):
        if not columns:
            return df

        return df.with_columns(
            [pl.col(col).str.strip_chars() for col in columns if df[col].dtype == pl.Utf8]
        )


def lowercase(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Convert string columns to lowercase."""
    with tracer.start_as_current_span("operation.lowercase"):
        if not columns:
            return df

        return df.with_columns(
            [pl.col(col).str.to_lowercase() for col in columns if df[col].dtype == pl.Utf8]
        )


def uppercase(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Convert string columns to uppercase."""
    with tracer.start_as_current_span("operation.uppercase"):
        if not columns:
            return df

        return df.with_columns(
            [pl.col(col).str.to_uppercase() for col in columns if df[col].dtype == pl.Utf8]
        )


def replace(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Replace values in columns."""
    with tracer.start_as_current_span("operation.replace"):
        if not columns:
            return df

        pattern = params.get("pattern")
        value = params.get("value")
        replacement = params.get("replacement", "")

        if pattern is not None and value is None:
            # Regex replacement
            return df.with_columns(
                [
                    pl.col(col).str.replace_all(pattern, replacement)
                    for col in columns
                    if df[col].dtype == pl.Utf8
                ]
            )
        elif value is not None:
            # Exact value replacement
            mapping = {value: replacement}
            return df.with_columns([pl.col(col).replace(mapping) for col in columns])

        return df


def cast_type(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Cast columns to specified data type."""
    with tracer.start_as_current_span("operation.cast_type"):
        if not columns:
            return df

        target_type = params.get("dtype")
        if not target_type:
            return df

        # Map string type names to Polars types
        type_mapping = {
            "int": pl.Int64,
            "float": pl.Float64,
            "str": pl.Utf8,
            "bool": pl.Boolean,
            "date": pl.Date,
            "datetime": pl.Datetime,
        }

        dtype = type_mapping.get(target_type, target_type)
        strict = params.get("strict", False)

        return df.with_columns([pl.col(col).cast(dtype, strict=strict) for col in columns])


def filter_rows(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Filter rows based on a condition."""
    with tracer.start_as_current_span("operation.filter"):
        # Support simple conditions
        operator = params.get("operator", "==")
        value = params.get("value")

        if not columns or value is None:
            return df

        # Build filter expression
        col = columns[0]  # Use first column for filtering
        if operator == "==":
            expr = pl.col(col) == value
        elif operator == "!=":
            expr = pl.col(col) != value
        elif operator == ">":
            expr = pl.col(col) > value
        elif operator == ">=":
            expr = pl.col(col) >= value
        elif operator == "<":
            expr = pl.col(col) < value
        elif operator == "<=":
            expr = pl.col(col) <= value
        elif operator == "in":
            expr = pl.col(col).is_in(value)
        else:
            return df

        return df.filter(expr)


def remove_outliers(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Remove outliers using IQR or Z-score method."""
    with tracer.start_as_current_span("operation.remove_outliers"):
        if not columns:
            return df

        method = params.get("method", "iqr")
        threshold = params.get("threshold", 1.5)

        for col in columns:
            if df[col].dtype not in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                continue

            if method == "iqr":
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr
                df = df.filter((pl.col(col) >= lower) & (pl.col(col) <= upper))
            elif method == "zscore":
                mean = df[col].mean()
                std = df[col].std()
                if std and std > 0:
                    df = df.filter((pl.col(col) - mean).abs() <= threshold * std)

        return df


def standardize(df: pl.DataFrame, columns: List[str], **params: Any) -> pl.DataFrame:
    """Standardize numeric columns (z-score normalization)."""
    with tracer.start_as_current_span("operation.standardize"):
        if not columns:
            return df

        return df.with_columns(
            [
                ((pl.col(col) - pl.col(col).mean()) / pl.col(col).std()).alias(col)
                for col in columns
                if df[col].dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]
            ]
        )


# Operation registry
OPERATIONS = {
    "drop_nulls": drop_nulls,
    "fill_nulls": fill_nulls,
    "drop_duplicates": drop_duplicates,
    "trim_whitespace": trim_whitespace,
    "lowercase": lowercase,
    "uppercase": uppercase,
    "replace": replace,
    "cast_type": cast_type,
    "filter": filter_rows,
    "remove_outliers": remove_outliers,
    "standardize": standardize,
}
