from datetime import datetime
from typing import Any, cast

import pandas as pd
import numpy as np


ColumnType = str


def detect_column_type(series: pd.Series) -> ColumnType:
    """Detect the type of a pandas Series: numeric, categorical, datetime, or text."""
    if series.dtype == "object":
        sample = series.dropna().head(100)
        if sample.empty:
            return "text"

        try:
            pd.to_datetime(sample, errors="raise")
            return "datetime"
        except (ValueError, TypeError):
            pass

        unique_ratio = series.nunique() / len(series) if len(series) > 0 else 0
        if unique_ratio < 0.5 and series.nunique() < 100:
            return "categorical"

        return "text"

    elif pd.api.types.is_numeric_dtype(series):
        if pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series):
            return "numeric"

    return "text"


def get_sample_values(series: pd.Series, n: int = 5) -> list[Any]:
    """Get sample non-null values from a series."""
    return series.dropna().head(n).tolist()


def detect_schema(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Auto-detect column types and sample values for all columns."""
    columns = []
    for col in df.columns:
        series = cast(pd.Series, df[col])
        col_type = detect_column_type(series)
        sample_values = get_sample_values(series)
        columns.append(
            {
                "name": col,
                "type": col_type,
                "sample_values": sample_values,
            }
        )
    return columns
