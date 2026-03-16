import uuid
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect, text

from app.db.session import get_db
from app.models.dataset import Dataset
from app.services import file_parser, schema_detector, data_profiler


router = APIRouter(prefix="/api/datasets", tags=["datasets"])

_data_storage: dict[str, pd.DataFrame] = {}


class DatasetResponse(BaseModel):
    id: int
    filename: str
    file_type: str
    row_count: int
    column_count: int
    columns: list[dict[str, Any]]
    data_profile: dict[str, Any]
    storage_key: str

    class Config:
        from_attributes = True


@router.post("/upload", response_model=DatasetResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> DatasetResponse:
    """Upload a CSV, TSV, or Excel file and get back dataset metadata."""
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

    return DatasetResponse(
        id=dataset.id,
        filename=dataset.filename,
        file_type=dataset.file_type,
        row_count=dataset.row_count,
        column_count=dataset.column_count,
        columns=dataset.columns,
        data_profile=dataset.data_profile,
        storage_key=dataset.storage_key,
    )


class DBDatasetRequest(BaseModel):
    connection_string: str
    table_name: str
    display_name: str | None = None


@router.post("/register-db", response_model=DatasetResponse)
async def register_database_dataset(
    request: DBDatasetRequest,
    db: Session = Depends(get_db),
) -> DatasetResponse:
    """Register an existing database table as a DataPilot dataset.

    Accepts a SQLAlchemy connection string (e.g. sqlite:///path/to/db or
    postgresql://user:pass@host/dbname) and a table name. Inspects the table
    schema and stores the dataset record so it can be queried with NL.
    """
    try:
        engine = create_engine(request.connection_string)
        inspector = inspect(engine)
        if request.table_name not in inspector.get_table_names():
            raise HTTPException(
                status_code=400,
                detail=f"Table '{request.table_name}' not found in the database.",
            )
        raw_columns = inspector.get_columns(request.table_name)
        # Sample up to 3 rows for display
        with engine.connect() as conn:
            sample_rows = conn.execute(
                text(f"SELECT * FROM {request.table_name} LIMIT 3")  # noqa: S608
            ).fetchall()
        col_names = [c["name"] for c in raw_columns]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot connect to database: {e}")

    columns = []
    for i, col in enumerate(raw_columns):
        samples = [str(row[i]) for row in sample_rows if row[i] is not None][:3]
        columns.append({
            "name": col["name"],
            "dtype": str(col["type"]),
            "sample": ", ".join(samples),
        })

    storage_key = str(uuid.uuid4())
    display = request.display_name or request.table_name
    dataset = Dataset(
        filename=request.table_name,
        file_type="database",
        row_count=0,
        column_count=len(columns),
        columns=columns,
        data_profile={"source": "database", "display_name": display},
        storage_key=storage_key,
        dataset_type="database",
        db_connection_string=request.connection_string,
        expires_at=datetime.utcnow() + timedelta(days=365),
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    return DatasetResponse(
        id=dataset.id,
        filename=dataset.filename,
        file_type=dataset.file_type,
        row_count=dataset.row_count,
        column_count=dataset.column_count,
        columns=dataset.columns,
        data_profile=dataset.data_profile,
        storage_key=dataset.storage_key,
    )
