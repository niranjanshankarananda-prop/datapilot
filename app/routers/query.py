from typing import Any, Optional
import os

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.query import Query
from app.schemas.query import QueryRequest, QueryResponse, QueryResult
from app.services.nl_to_pandas import nl_to_pandas
from app.services.result_formatter import format_result
from app.sandbox.executor import execute_pandas_code, ExecutionError
from app.sandbox.validators import SecurityViolationError


router = APIRouter(prefix="/api", tags=["query"])


DATASETS_DIR = os.environ.get("DATASETS_DIR", "./datasets")


def load_dataset(dataset_id: str) -> pd.DataFrame:
    for ext in [".csv", ".xlsx", ".parquet"]:
        path = os.path.join(DATASETS_DIR, f"{dataset_id}{ext}")
        if os.path.exists(path):
            if ext == ".csv":
                return pd.read_csv(path)
            elif ext == ".xlsx":
                return pd.read_excel(path)
            elif ext == ".parquet":
                return pd.read_parquet(path)

    raise FileNotFoundError(f"Dataset {dataset_id} not found")


def get_dataset_schema(df: pd.DataFrame) -> list[dict[str, Any]]:
    schema = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sample_values = df[col].dropna().head(3).tolist()
        sample = ", ".join(str(v) for v in sample_values)
        schema.append({"name": col, "dtype": dtype, "sample": sample})
    return schema


@router.post("/query", response_model=QueryResult)
def create_query(request: QueryRequest, db: Session = Depends(get_db)) -> QueryResult:
    try:
        df = load_dataset(request.dataset_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    schema = get_dataset_schema(df)

    query = Query(
        dataset_id=request.dataset_id, question=request.question, status="processing"
    )
    db.add(query)
    db.commit()
    db.refresh(query)

    try:
        generated_code = nl_to_pandas(request.question, schema)
        query.generated_code = generated_code

        result = execute_pandas_code(generated_code, df)
        formatted_result = format_result(result)

        query.result = formatted_result
        query.status = "success"

    except SecurityViolationError as e:
        query.error = f"Security violation: {str(e)}"
        query.status = "error"

    except ExecutionError as e:
        query.error = f"Execution error: {str(e)}"
        query.status = "error"

    except Exception as e:
        query.error = str(e)
        query.status = "error"

    db.commit()
    db.refresh(query)

    return QueryResult(
        id=query.id,
        status=query.status,
        result=query.result,
        error=query.error,
        generated_code=query.generated_code,
    )


@router.get("/query/{query_id}", response_model=QueryResponse)
def get_query(query_id: str, db: Session = Depends(get_db)) -> QueryResponse:
    query = db.query(Query).filter(Query.id == query_id).first()
    if not query:
        raise HTTPException(status_code=404, detail="Query not found")

    return QueryResponse(
        id=query.id,
        dataset_id=query.dataset_id,
        question=query.question,
        generated_code=query.generated_code,
        result=query.result,
        error=query.error,
        status=query.status,
        created_at=query.created_at,
        updated_at=query.updated_at,
    )
