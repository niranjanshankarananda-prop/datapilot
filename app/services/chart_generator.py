from enum import Enum
from typing import Any, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio


class ChartType(str, Enum):
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    HEATMAP = "heatmap"


THEME_COLORS = [
    "#636EFA",
    "#EF553B",
    "#00CC96",
    "#AB63FA",
    "#FFA15A",
    "#19D3F3",
    "#FF6692",
    "#B6E880",
    "#FF97FF",
    "#FECB52",
]

pio.templates.default = "plotly_dark"


def _convert_to_dataframe(data: dict[str, Any]) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data

    if isinstance(data, dict):
        if "value" in data and isinstance(data["value"], list):
            return pd.DataFrame(data["value"])
        return pd.DataFrame([data])

    if isinstance(data, list):
        return pd.DataFrame(data)

    raise ValueError(f"Unsupported data format: {type(data)}")


def _extract_columns_for_chart(
    df: pd.DataFrame, chart_type: ChartType
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
    all_cols = df.columns.tolist()

    if chart_type == ChartType.PIE:
        names = categorical_cols[0] if categorical_cols else all_cols[0]
        values = (
            numeric_cols[0]
            if numeric_cols
            else all_cols[1]
            if len(all_cols) > 1
            else all_cols[0]
        )
        return names, values, None

    if chart_type == ChartType.HISTOGRAM:
        x = numeric_cols[0] if numeric_cols else all_cols[0]
        return x, None, None

    if chart_type == ChartType.HEATMAP:
        return None, None, None

    x = categorical_cols[0] if categorical_cols else all_cols[0]
    y = (
        numeric_cols[0]
        if numeric_cols
        else (all_cols[1] if len(all_cols) > 1 else all_cols[0])
    )
    color = categorical_cols[1] if len(categorical_cols) > 1 else None

    return x, y, color


def generate_chart(
    data: dict[str, Any],
    chart_type: str,
    title: str = "Chart",
) -> dict[str, Any]:
    df = _convert_to_dataframe(data)

    try:
        chart_type_enum = ChartType(chart_type.lower())
    except ValueError:
        chart_type_enum = ChartType.BAR

    x_col, y_col, color_col = _extract_columns_for_chart(df, chart_type_enum)

    if chart_type_enum == ChartType.BAR:
        # Color each bar differently when no grouping column
        if color_col is None and x_col:
            fig = px.bar(df, x=x_col, y=y_col, color=x_col, title=title,
                         color_discrete_sequence=THEME_COLORS)
            fig.update_layout(showlegend=False)
        else:
            fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title,
                         color_discrete_sequence=THEME_COLORS)
    elif chart_type_enum == ChartType.LINE:
        fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title, markers=True)
    elif chart_type_enum == ChartType.PIE:
        fig = px.pie(
            df,
            names=x_col,
            values=y_col,
            title=title,
            color_discrete_sequence=THEME_COLORS,
        )
    elif chart_type_enum == ChartType.SCATTER:
        fig = px.scatter(df, x=x_col, y=y_col, color=color_col, title=title)
    elif chart_type_enum == ChartType.HISTOGRAM:
        fig = px.histogram(df, x=x_col, title=title, nbins=20)
    elif chart_type_enum == ChartType.HEATMAP:
        numeric_df = df.select_dtypes(include=["number"])
        if numeric_df.shape[1] < 2:
            numeric_df = df.select_dtypes(include=["number"]).T
        fig = px.imshow(
            numeric_df.corr() if numeric_df.shape[1] > 1 else numeric_df, title=title
        )
    else:
        fig = px.bar(df, x=x_col, y=y_col, title=title)

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#9ca3af", family="Inter, sans-serif", size=12),
        margin=dict(t=40, r=20, b=40, l=50),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.06)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", linecolor="rgba(255,255,255,0.06)"),
        colorway=THEME_COLORS,
    )

    return fig.to_json()


def export_chart_as_image(
    data: dict[str, Any],
    chart_type: str,
    format: str = "png",
    width: int = 800,
    height: int = 600,
    title: str = "Chart",
) -> bytes:
    fig_dict = generate_chart(data, chart_type, title)
    fig = go.Figure(fig_dict)

    return fig.to_image(format=format, width=width, height=height, scale=2)
