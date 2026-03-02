import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Float, Text

from app.db.session import Base


class ChartRecommendation(Base):
    __tablename__ = "chart_recommendations"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    query_id = Column(String, nullable=False, index=True)
    chart_type = Column(String, nullable=False)
    confidence = Column(Float, nullable=False)
    reasoning = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
