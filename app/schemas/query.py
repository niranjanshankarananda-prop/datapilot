from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class ColumnSchema(BaseModel):
    name: str
    dtype: str
    sample: Optional[str] = None


class QueryRequest(BaseModel):
    dataset_id: str = Field(..., description="ID of the dataset to query")
    question: str = Field(..., description="Natural language question about the data")


class QueryResponse(BaseModel):
    id: str
    dataset_id: str
    question: str
    generated_code: Optional[str] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    status: str
    created_at: datetime
    updated_at: datetime


class QueryResult(BaseModel):
    id: str
    status: str
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    generated_code: Optional[str] = None
