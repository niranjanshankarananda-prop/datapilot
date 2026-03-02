from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ChartType(str, Enum):
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    HEATMAP = "heatmap"


class ChartRecommendationRequest(BaseModel):
    question: str = Field(..., description="Original natural language question")
    columns: list[dict[str, str]] = Field(
        ..., description="Column schema with name and type"
    )


class ChartRecommendationResponse(BaseModel):
    chart_type: ChartType
    confidence: float = Field(..., ge=0.0, le=1.0)
    reasoning: Optional[str] = None
