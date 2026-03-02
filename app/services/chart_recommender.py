from typing import Any, Optional

import pandas as pd
from groq import Groq

from app.config import settings
from app.schemas.chart import ChartType, ChartRecommendationResponse


SYSTEM_PROMPT = """You are a chart recommendation assistant. Based on the user's question and the data schema, recommend the most appropriate chart type.

Available chart types:
- bar: For comparing categories, showing rankings, or top N items
- line: For showing trends over time (time series data)
- pie: For showing proportions or percentages of a whole
- scatter: For showing correlation between two numeric variables
- histogram: For showing distribution of a single numeric variable
- heatmap: For showing correlation matrices or density

Respond with ONLY the chart type name and a brief reasoning.
Format: chart_type: <type>
reasoning: <brief explanation>
"""


def _rule_based_recommendation(
    question: str, columns: list[dict[str, str]]
) -> Optional[ChartRecommendationResponse]:
    """Apply rule-based logic to recommend chart type."""
    question_lower = question.lower()
    col_types = {
        col.get("name", "").lower(): col.get("type", "").lower() for col in columns
    }
    col_names = [col.get("name", "").lower() for col in columns]
    numeric_cols = [
        name
        for name, ctype in col_types.items()
        if ctype in ("numeric", "integer", "float")
    ]

    has_date = any(
        "date" in name or "time" in name or "year" in name or "month" in name
        for name in col_names
    )
    has_numeric = len(numeric_cols) >= 1
    has_categorical = any(
        ctype in ("categorical", "text") for ctype in col_types.values()
    )

    if any(
        word in question_lower
        for word in ["trend", "over time", "time series", "growth", "change over"]
    ):
        if has_date and has_numeric:
            return ChartRecommendationResponse(
                chart_type=ChartType.LINE,
                confidence=0.9,
                reasoning="Question asks about trend over time",
            )

    if any(
        word in question_lower
        for word in ["top", "bottom", "rank", "compare", "ranking", "most", "least"]
    ):
        if has_categorical and has_numeric:
            return ChartRecommendationResponse(
                chart_type=ChartType.BAR,
                confidence=0.85,
                reasoning="Question asks for top N or ranking comparison",
            )

    # Aggregation queries: "average by", "total per", "sum by", "count by", etc.
    has_aggregation = any(
        word in question_lower
        for word in ["average", "avg", "mean", "total", "sum", "count", "max", "min"]
    )
    has_grouping = any(
        word in question_lower
        for word in [" by ", " per ", " each ", " group", " for each ", " across "]
    )
    if has_aggregation and has_grouping:
        if has_numeric:
            return ChartRecommendationResponse(
                chart_type=ChartType.BAR,
                confidence=0.85,
                reasoning="Aggregation query grouped by category",
            )

    if any(
        word in question_lower
        for word in [
            "proportion",
            "percentage",
            "share",
            "distribution of",
            "breakdown",
        ]
    ):
        if has_categorical and has_numeric:
            return ChartRecommendationResponse(
                chart_type=ChartType.PIE,
                confidence=0.8,
                reasoning="Question asks for proportion or percentage",
            )

    if any(
        word in question_lower
        for word in ["correlation", "relationship", "relate", "how does", "compare"]
    ):
        if len(numeric_cols) >= 2:
            return ChartRecommendationResponse(
                chart_type=ChartType.SCATTER,
                confidence=0.85,
                reasoning="Question asks about correlation between variables",
            )

    if any(
        word in question_lower
        for word in ["distribution", "spread", "range", "frequency", "histogram"]
    ):
        if has_numeric:
            return ChartRecommendationResponse(
                chart_type=ChartType.HISTOGRAM,
                confidence=0.85,
                reasoning="Question asks about data distribution",
            )

    if len(numeric_cols) >= 2 and "correlation" in question_lower:
        return ChartRecommendationResponse(
            chart_type=ChartType.HEATMAP,
            confidence=0.8,
            reasoning="Multiple numeric columns suggest correlation matrix",
        )

    # General fallback: if there are both categorical and numeric columns,
    # a bar chart is usually a reasonable default for tabular results
    if has_categorical and has_numeric:
        return ChartRecommendationResponse(
            chart_type=ChartType.BAR,
            confidence=0.7,
            reasoning="Default bar chart for categorical + numeric data",
        )

    return None


def _ai_recommendation(
    question: str, columns: list[dict[str, str]]
) -> ChartRecommendationResponse:
    """Use Groq AI to recommend chart type for ambiguous cases."""
    client = Groq(api_key=settings.GROQ_API_KEY)

    schema_info = "\n".join(
        [f"- {col.get('name')}: {col.get('type')}" for col in columns]
    )

    user_prompt = f"""Question: {question}

Data columns:
{schema_info}

What is the best chart type for visualizing the answer to this question?"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=256,
    )

    content = response.choices[0].message.content
    if not content:
        raise ValueError("Empty response from Groq")

    chart_type = ChartType.BAR
    reasoning = "Default fallback"
    confidence = 0.5

    for line in content.split("\n"):
        line = line.strip().lower()
        if line.startswith("chart_type:") or line.startswith("chart type:"):
            type_str = line.split(":", 1)[1].strip().lower()
            if "line" in type_str:
                chart_type = ChartType.LINE
            elif "bar" in type_str:
                chart_type = ChartType.BAR
            elif "pie" in type_str:
                chart_type = ChartType.PIE
            elif "scatter" in type_str:
                chart_type = ChartType.SCATTER
            elif "histogram" in type_str:
                chart_type = ChartType.HISTOGRAM
            elif "heatmap" in type_str:
                chart_type = ChartType.HEATMAP
        elif line.startswith("reasoning:"):
            reasoning = line.split(":", 1)[1].strip()
        elif "confidence:" in line:
            try:
                conf_str = line.split(":", 1)[1].strip().replace("%", "")
                confidence = (
                    float(conf_str) / 100 if float(conf_str) > 1 else float(conf_str)
                )
            except (ValueError, IndexError):
                pass

    return ChartRecommendationResponse(
        chart_type=chart_type, confidence=confidence, reasoning=reasoning
    )


def recommend_chart_type(
    question: str,
    result_df: Optional[pd.DataFrame] = None,
    columns: Optional[list[dict[str, str]]] = None,
) -> ChartRecommendationResponse:
    """Recommend the most appropriate chart type based on question and data.

    Args:
        question: The original natural language question
        result_df: Optional DataFrame with query results
        columns: Column schema with name and type (used if result_df not provided)

    Returns:
        ChartRecommendationResponse with recommended chart type and confidence
    """
    if columns is None and result_df is None:
        raise ValueError("Either result_df or columns must be provided")

    if result_df is not None:
        col_types = []
        for col in result_df.columns:
            dtype = str(result_df[col].dtype)
            if "int" in dtype or "float" in dtype:
                col_type = "numeric"
            elif "object" in dtype:
                col_type = "categorical"
            elif "datetime" in dtype:
                col_type = "datetime"
            else:
                col_type = "text"
            col_types.append({"name": col, "type": col_type})
    else:
        col_types = columns

    rule_result = _rule_based_recommendation(question, col_types)
    if rule_result is not None and rule_result.confidence >= 0.8:
        return rule_result

    return _ai_recommendation(question, col_types)
