from typing import Any, AsyncGenerator
import asyncio
import json
import os
import sys
import time

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.dataset import Dataset
from app.models.query import Query
from app.schemas.query import QueryRequest, QueryResponse, QueryResult
from app.services.nl_to_pandas import nl_to_pandas
from app.services.nl_to_sql import nl_to_sql
from app.services.sql_executor import execute_sql_query, SQLSecurityError
from app.services.query_router import route_query, QueryRoute
from app.services.result_formatter import format_result
from app.sandbox.executor import execute_pandas_code, ExecutionError
from app.sandbox.validators import SecurityViolationError

# Shared eval tracer (graceful no-op if not on path)
try:
    _SHARED = "/Users/niranjan/Documents/AI_PROJS/shared"
    if _SHARED not in sys.path:
        sys.path.insert(0, _SHARED)
    from eval.tracer import trace_rag_call as _trace
    _TRACING_AVAILABLE = True
except ImportError:
    _TRACING_AVAILABLE = False


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
        sample_values = df[col].dropna().unique()[:5].tolist()
        sample = ", ".join(str(v) for v in sample_values)
        schema.append({"name": col, "dtype": dtype, "sample": sample})
    return schema


def _get_dataset_record(dataset_id: str, db: Session) -> Dataset | None:
    return db.query(Dataset).filter(Dataset.storage_key == dataset_id).first()


@router.post("/query", response_model=QueryResult)
def create_query(request: QueryRequest, db: Session = Depends(get_db)) -> QueryResult:
    dataset_record = _get_dataset_record(request.dataset_id, db)
    dataset_type = dataset_record.dataset_type if dataset_record else "file"
    route = route_query(dataset_type)

    query = Query(
        dataset_id=request.dataset_id, question=request.question, status="processing"
    )
    db.add(query)
    db.commit()
    db.refresh(query)

    try:
        if route == QueryRoute.SQL:
            if not dataset_record or not dataset_record.db_connection_string:
                raise ValueError("Database connection string not configured for this dataset.")
            schema = dataset_record.columns or []
            generated_code = nl_to_sql(
                request.question, schema,
                table_name=dataset_record.filename,
                api_key=getattr(request, "api_key", None),
            )
            query.generated_code = generated_code
            raw_result = execute_sql_query(generated_code, dataset_record.db_connection_string)
            formatted_result = format_result(raw_result)
        else:
            try:
                df = load_dataset(request.dataset_id)
            except FileNotFoundError as e:
                raise HTTPException(status_code=404, detail=str(e))
            schema = get_dataset_schema(df)
            generated_code = nl_to_pandas(request.question, schema)
            query.generated_code = generated_code
            result = execute_pandas_code(generated_code, df)
            formatted_result = format_result(result)

        query.result = formatted_result
        query.status = "success"

        if _TRACING_AVAILABLE:
            _trace(
                question=request.question,
                context=[generated_code],
                answer=str(formatted_result)[:500],
                model="llama-3.3-70b",
                latency_ms=0,
                product="datapilot",
            )

    except SQLSecurityError as e:
        query.error = f"SQL security violation: {str(e)}"
        query.status = "error"

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


def _sse_event(event_type: str, data: dict) -> str:
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"


async def _stream_query_events(
    request: QueryRequest, db: Session
) -> AsyncGenerator[str, None]:
    """Yield SSE events for each stage: status -> code_generated -> result (or error)."""
    dataset_record = _get_dataset_record(request.dataset_id, db)
    dataset_type = dataset_record.dataset_type if dataset_record else "file"
    route = route_query(dataset_type)

    yield _sse_event("status", {"message": "Generating code...", "route": route.value})
    await asyncio.sleep(0)

    try:
        if route == QueryRoute.SQL:
            if not dataset_record or not dataset_record.db_connection_string:
                yield _sse_event("error", {"message": "Database connection string not configured."})
                return
            schema = dataset_record.columns or []
            t0 = time.monotonic()
            generated_code = nl_to_sql(
                request.question, schema,
                table_name=dataset_record.filename,
                api_key=getattr(request, "api_key", None),
            )
            yield _sse_event("code_generated", {"code": generated_code, "language": "sql"})
            await asyncio.sleep(0)

            yield _sse_event("status", {"message": "Executing SQL query..."})
            await asyncio.sleep(0)

            raw_result = execute_sql_query(generated_code, dataset_record.db_connection_string)
            formatted = format_result(raw_result)
            latency_ms = int((time.monotonic() - t0) * 1000)
            yield _sse_event("result", {"data": formatted, "latency_ms": latency_ms})

        else:
            try:
                df = load_dataset(request.dataset_id)
            except FileNotFoundError as e:
                yield _sse_event("error", {"message": str(e)})
                return
            schema = get_dataset_schema(df)
            t0 = time.monotonic()
            generated_code = nl_to_pandas(request.question, schema)
            yield _sse_event("code_generated", {"code": generated_code, "language": "python"})
            await asyncio.sleep(0)

            yield _sse_event("status", {"message": "Executing..."})
            await asyncio.sleep(0)

            result = execute_pandas_code(generated_code, df)
            formatted = format_result(result)
            latency_ms = int((time.monotonic() - t0) * 1000)
            yield _sse_event("result", {"data": formatted, "latency_ms": latency_ms})

    except (SQLSecurityError, SecurityViolationError) as e:
        yield _sse_event("error", {"message": f"Security violation: {e}"})
    except ExecutionError as e:
        yield _sse_event("error", {"message": f"Execution error: {e}"})
    except Exception as e:
        yield _sse_event("error", {"message": str(e)})


@router.post("/query/stream")
async def stream_query(request: QueryRequest, db: Session = Depends(get_db)) -> StreamingResponse:
    """Stream query processing as Server-Sent Events.

    Events emitted in order: status -> code_generated -> status -> result (or error)
    """
    return StreamingResponse(
        _stream_query_events(request, db),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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
