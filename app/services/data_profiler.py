from typing import Any, cast

import pandas as pd
import numpy as np


def compute_column_profile(series: pd.Series) -> dict[str, Any]:
    """Compute profile for a single column."""
    profile: dict[str, Any] = {
        "missing_count": int(series.isna().sum()),
        "missing_percent": float(series.isna().mean() * 100),
        "unique_count": int(series.nunique()),
    }

    if pd.api.types.is_numeric_dtype(series):
        numeric_series = series.dropna()
        if not numeric_series.empty:
            profile["min"] = float(numeric_series.min())
            profile["max"] = float(numeric_series.max())
            profile["mean"] = float(numeric_series.mean())
            profile["median"] = float(numeric_series.median())
            profile["std"] = float(numeric_series.std())

            bins = pd.cut(numeric_series, bins=5)
            counts = bins.value_counts()
            profile["distribution"] = {str(k): int(v) for k, v in counts.items()}

    return profile


def generate_profile(df: pd.DataFrame) -> dict[str, Any]:
    """Generate a comprehensive data profile."""
    profile: dict[str, Any] = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": {},
    }

    for col in df.columns:
        series = cast(pd.Series, df[col])
        profile["columns"][col] = compute_column_profile(series)

    return profile
