from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.query import Query
from app.services.chart_generator import (
    ChartType,
    export_chart_as_image,
    generate_chart,
)


router = APIRouter(prefix="/api/charts", tags=["charts"])


def get_query_or_404(query_id: str, db: Session) -> Query:
    query = db.query(Query).filter(Query.id == query_id).first()
    if not query:
        raise HTTPException(status_code=404, detail="Query not found")
    return query


def get_chart_type_from_query(query: Query) -> str:
    result = query.result
    if isinstance(result, dict):
        return result.get("chart_type", "bar")
    return "bar"


@router.get("/{query_id}")
def get_chart_config(query_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    query = get_query_or_404(query_id, db)

    if not query.result:
        raise HTTPException(
            status_code=400, detail="No result data available for chart"
        )

    result_data = query.result
    if isinstance(result_data, dict):
        data = result_data.get("value")
    else:
        data = result_data

    if not data:
        raise HTTPException(status_code=400, detail="No data available for chart")

    chart_type = get_chart_type_from_query(query)
    title = f"Chart for: {query.question}"

    chart_config = generate_chart(data, chart_type, title)

    return {
        "query_id": query_id,
        "chart_type": chart_type,
        "chart": chart_config,
    }


@router.get("/{query_id}/image")
def get_chart_image(
    query_id: str,
    format: str = "png",
    width: int = 800,
    height: int = 600,
    db: Session = Depends(get_db),
) -> Response:
    query = get_query_or_404(query_id, db)

    if not query.result:
        raise HTTPException(
            status_code=400, detail="No result data available for chart"
        )

    result_data = query.result
    if isinstance(result_data, dict):
        data = result_data.get("value")
    else:
        data = result_data

    if not data:
        raise HTTPException(status_code=400, detail="No data available for chart")

    chart_type = get_chart_type_from_query(query)
    title = f"Chart for: {query.question}"

    image_bytes = export_chart_as_image(
        data, chart_type, format=format, width=width, height=height, title=title
    )

    media_type = f"image/{format}"
    return Response(content=image_bytes, media_type=media_type)
