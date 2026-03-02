from typing import Any, Optional
import os
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
import re
from difflib import get_close_matches


router = APIRouter(tags=["pages"])


def fix_column_names_in_code(code: str, actual_columns: list[str]) -> str:
    """Fix mismatched column names in generated code using fuzzy matching.

    Finds quoted strings in df['...'] or df["..."] patterns and replaces
    them with the closest matching actual column name if different.
    """
    col_set = set(actual_columns)
    col_lower_map = {c.lower(): c for c in actual_columns}

    def replace_col(match):
        quote = match.group(1)
        col_name = match.group(2)

        # Exact match — no change needed
        if col_name in col_set:
            return f"[{quote}{col_name}{quote}]"

        # Case-insensitive match
        if col_name.lower() in col_lower_map:
            return f"[{quote}{col_lower_map[col_name.lower()]}{quote}]"

        # Fuzzy match (typo correction)
        close = get_close_matches(col_name, actual_columns, n=1, cutoff=0.6)
        if close:
            return f"[{quote}{close[0]}{quote}]"

        # Also try lowercase fuzzy
        close_lower = get_close_matches(
            col_name.lower(), [c.lower() for c in actual_columns], n=1, cutoff=0.6
        )
        if close_lower:
            real_col = col_lower_map[close_lower[0]]
            return f"[{quote}{real_col}{quote}]"

        return match.group(0)

    # Match df['COLUMN'] or df["COLUMN"] patterns
    fixed = re.sub(r"\[(['\"])([^'\"]+)\1\]", replace_col, code)
    return fixed

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


SAMPLE_DATASETS = {
    "sales_data": {"filename": "sales_data.csv", "desc": "5K rows — product sales, revenue, regions"},
    "customer_data": {"filename": "customer_data.csv", "desc": "2K rows — segments, purchases, churn risk"},
    "financial_data": {"filename": "financial_data.csv", "desc": "3K rows — transactions, payment methods"},
    "web_analytics": {"filename": "web_analytics.csv", "desc": "2K rows — page views, bounce rates, sources"},
    "survey_results": {"filename": "survey_results.csv", "desc": "1.5K rows — satisfaction, NPS, demographics"},
}

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "tests", "fixtures", "sample_csvs")


@router.post("/sample/{name}", response_class=HTMLResponse)
async def load_sample(name: str, db: Session = Depends(get_db)):
    if name not in SAMPLE_DATASETS:
        raise HTTPException(status_code=404, detail="Sample dataset not found")

    info = SAMPLE_DATASETS[name]
    filepath = os.path.join(SAMPLE_DIR, info["filename"])
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Sample file not found")

    df = pd.read_csv(filepath)

    storage_key = str(uuid.uuid4())
    _data_storage[storage_key] = df

    columns = schema_detector.detect_schema(df)
    profile = data_profiler.generate_profile(df)

    dataset = Dataset(
        filename=info["filename"],
        file_type="csv",
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


@router.get("/sample/{name}/download")
async def download_sample(name: str):
    if name not in SAMPLE_DATASETS:
        raise HTTPException(status_code=404, detail="Sample dataset not found")

    info = SAMPLE_DATASETS[name]
    filepath = os.path.join(SAMPLE_DIR, info["filename"])
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Sample file not found")

    with open(filepath, "r") as f:
        content = f.read()

    return StreamingResponse(
        iter([content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={info['filename']}"},
    )


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
        # Data lost due to server restart — return a friendly error
        query = Query(
            dataset_id=str(dataset_id),
            question=question,
            status="error",
            error="Server was restarted and your data is no longer in memory. Please go back and re-upload your file.",
        )
        db.add(query)
        db.commit()
        db.refresh(query)
        return templates.TemplateResponse(
            "components/query-result.html",
            {"request": request, "query": query},
        )

    df = _data_storage[storage_key]

    schema = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sample_values = df[col].dropna().unique()[:5].tolist()
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

    column_names = [col["name"] for col in schema]

    try:
        generated_code = nl_to_pandas(question, schema, api_key=api_key or None)
        # Auto-fix column name typos and case mismatches
        generated_code = fix_column_names_in_code(generated_code, column_names)
        query.generated_code = generated_code

        try:
            result = execute_pandas_code(generated_code, df)
        except ExecutionError as e:
            err_str = str(e)
            # Detect column mismatch errors and provide helpful feedback
            if "KeyError" in err_str or "not in index" in err_str:
                query.error = (
                    f"Could not find a matching column in your data. "
                    f"Available columns: {', '.join(column_names)}. "
                    f"Please rephrase your query using these column names."
                )
                query.status = "error"
            else:
                query.error = f"Execution error: {err_str}"
                query.status = "error"
            db.commit()
            db.refresh(query)
            return templates.TemplateResponse(
                "components/query-result.html",
                {"request": request, "query": query},
            )

        formatted_result = format_result(result)

        # Check for empty results and give helpful feedback
        is_empty = False
        if formatted_result.get("type") == "table":
            val = formatted_result.get("value")
            if isinstance(val, (list, tuple)) and len(val) == 0:
                is_empty = True
            elif isinstance(val, dict) and len(val) == 0:
                is_empty = True
        elif formatted_result.get("type") == "text" and formatted_result.get("value") == "No result returned":
            is_empty = True

        if is_empty:
            query.result = {
                "type": "text",
                "value": (
                    "No matching data found for your query. "
                    f"Available columns: {', '.join(column_names)}. "
                    "Try rephrasing with different filters or column names."
                ),
            }
        else:
            query.result = formatted_result

        query.status = "success"

    except SecurityViolationError as e:
        query.error = f"Security violation: {str(e)}"
        query.status = "error"

    except ExecutionError as e:
        query.error = f"Execution error: {str(e)}"
        query.status = "error"

    except Exception as e:
        err_str = str(e)
        if "KeyError" in err_str or "not in index" in err_str:
            query.error = (
                f"Could not find a matching column in your data. "
                f"Available columns: {', '.join(column_names)}. "
                f"Please rephrase your query using these column names."
            )
        else:
            query.error = err_str
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
