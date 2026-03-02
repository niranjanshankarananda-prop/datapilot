from typing import Any, Optional
import uuid
from datetime import datetime, timedelta

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from starlette.responses import RedirectResponse

from app.db.session import get_db, Base, engine
from app.models.dataset import Dataset
from app.models.query import Query
from app.services import file_parser, schema_detector, data_profiler
from app.schemas.query import QueryRequest, QueryResult
from app.services.nl_to_pandas import nl_to_pandas, PROVIDERS
from app.services.result_formatter import format_result
from app.sandbox.executor import execute_pandas_code, ExecutionError
from app.sandbox.validators import SecurityViolationError
from starlette.responses import StreamingResponse
import io
import csv


router = APIRouter(tags=["pages"])

_data_storage: dict[str, pd.DataFrame] = {}

templates = Jinja2Templates(directory="app/templates")


def get_dataset_or_404(dataset_id: int, db: Session) -> Dataset:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


def get_dataset_data(dataset_id: int, db: Session) -> pd.DataFrame:
    dataset = get_dataset_or_404(dataset_id, db)
    storage_key = str(dataset.storage_key)
    if storage_key not in _data_storage:
        raise HTTPException(status_code=404, detail="Dataset data not found in memory")
    return _data_storage[storage_key]


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("pages/index.html", {"request": request})


@router.post("/upload", response_class=HTMLResponse)
async def upload_dataset(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    content = await file.read()

    try:
        df = file_parser.parse_file(content, file.filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    storage_key = str(uuid.uuid4())
    _data_storage[storage_key] = df

    columns = schema_detector.detect_schema(df)
    profile = data_profiler.generate_profile(df)

    dataset = Dataset(
        filename=file.filename,
        file_type=file_parser.detect_file_type(file.filename),
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
        data_profile=profile,
        storage_key=storage_key,
        expires_at=datetime.utcnow() + timedelta(hours=24),
    )

    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    return RedirectResponse(url=f"/workspace/{dataset.id}", status_code=303)


@router.get("/workspace/{dataset_id}", response_class=HTMLResponse)
async def workspace(request: Request, dataset_id: int, db: Session = Depends(get_db)):
    dataset = get_dataset_or_404(dataset_id, db)

    storage_key = str(dataset.storage_key)
    if storage_key not in _data_storage:
        raise HTTPException(status_code=404, detail="Dataset data not found in memory")

    df = _data_storage[storage_key]
    preview_df = df.head(20)

    preview_data = preview_df.to_dict(orient="records")
    preview_columns = [{"name": col, "dtype": str(df[col].dtype)} for col in df.columns]

    raw_profile = dataset.data_profile.get("columns", {})
    column_profile = [
        {"name": col_name, "dtype": str(df[col_name].dtype), **col_data}
        for col_name, col_data in raw_profile.items()
        if col_name in df.columns
    ]

    queries = (
        db.query(Query)
        .filter(Query.dataset_id == str(dataset_id))
        .order_by(Query.created_at.desc())
        .all()
    )

    return templates.TemplateResponse(
        "pages/workspace.html",
        {
            "request": request,
            "dataset": dataset,
            "preview_data": preview_data,
            "preview_columns": preview_columns,
            "column_profile": column_profile,
            "queries": queries,
        },
    )


@router.post("/workspace/{dataset_id}/query", response_class=HTMLResponse)
async def submit_query(
    request: Request,
    dataset_id: int,
    question: str = Form(...),
    api_key: str = Form(""),
    db: Session = Depends(get_db),
):
    dataset = get_dataset_or_404(dataset_id, db)

    storage_key = str(dataset.storage_key)
    if storage_key not in _data_storage:
        raise HTTPException(status_code=404, detail="Dataset data not found in memory")

    df = _data_storage[storage_key]

    schema = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sample_values = df[col].dropna().head(3).tolist()
        sample = ", ".join(str(v) for v in sample_values)
        schema.append({"name": col, "dtype": dtype, "sample": sample})

    query = Query(
        dataset_id=str(dataset_id),
        question=question,
        status="processing",
    )
    db.add(query)
    db.commit()
    db.refresh(query)

    try:
        generated_code = nl_to_pandas(question, schema, api_key=api_key or None)
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

    return templates.TemplateResponse(
        "components/query-result.html",
        {
            "request": request,
            "query": query,
        },
    )


@router.get("/workspace/{dataset_id}/preview", response_class=HTMLResponse)
async def get_preview(
    request: Request,
    dataset_id: int,
    db: Session = Depends(get_db),
):
    dataset = get_dataset_or_404(dataset_id, db)

    storage_key = str(dataset.storage_key)
    if storage_key not in _data_storage:
        raise HTTPException(status_code=404, detail="Dataset data not found in memory")

    df = _data_storage[storage_key]
    preview_df = df.head(20)

    preview_data = preview_df.to_dict(orient="records")
    preview_columns = [{"name": col, "dtype": str(df[col].dtype)} for col in df.columns]

    return templates.TemplateResponse(
        "components/data-preview.html",
        {
            "request": request,
            "preview_data": preview_data,
            "preview_columns": preview_columns,
        },
    )


@router.get("/workspace/{dataset_id}/export")
async def export_csv(
    dataset_id: int,
    db: Session = Depends(get_db),
):
    df = get_dataset_data(dataset_id, db)
    dataset = get_dataset_or_404(dataset_id, db)

    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    filename = dataset.filename.rsplit(".", 1)[0] + "_export.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
