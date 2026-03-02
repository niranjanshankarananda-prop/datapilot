import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Text, Integer

from app.db.session import Base


class PageView(Base):
    __tablename__ = "page_views"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    path = Column(String, nullable=False)
    ip_hash = Column(String, nullable=False)  # hashed IP for privacy
    user_agent = Column(Text, nullable=True)
    referrer = Column(Text, nullable=True)
    country = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
