import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, Integer

from app.db.session import Base


class Feedback(Base):
    __tablename__ = "feedback"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    rating = Column(Integer, nullable=False)
    text = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
