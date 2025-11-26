import os
import logging 
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Float, UniqueConstraint, ForeignKey, Index
)
from sqlalchemy.orm import declarative_base

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ensure data folder exists
os.makedirs(".data", exist_ok=True)

# database path
DB_PATH = ".data/quicksilver.db"

#SQLAlchemy setup
Base = declarative_base()
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)

# Define Tables
class Headline(Base):
    __tablename__ = "headlines"
    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False)
    source = Column(String)
    title = Column(Text, nullable=False)
    url = Column(Text, nullable=False)
    published_at_utc = Column(String, nullable=False)
    raw_json = Column(Text)
    hash = Column(String, nullable=False, unique=True)
    created_at = Column(String, nullable=False)
    __table_args__ = (
        Index("idx_headlines_ticker_time", "ticker", "published_at_utc"),
    )

class Sentiment(Base):
    __tablename__ = "sentiment"
    id = Column(Integer, primary_key=True)
    headline_id = Column(Integer, ForeignKey("headlines.id", ondelete="CASCADE"), nullable=False,unique=True)
    label = Column(String, nullable=False)
    score_pos = Column(Float, nullable=False)
    score_neu = Column(Float, nullable=False)
    score_neg = Column(Float, nullable=False)
    model_version = Column(String, nullable=False)
    inference_ms = Column(Integer)
    created_at = Column(String, nullable=False)

class Feature(Base):
    __tablename__ = "features"
    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False)
    window = Column(String, nullable=False)
    ts_utc = Column(String, nullable=False)
    sent_mean = Column(Float)
    sent_z = Column(Float)
    vol_z = Column(Float)
    headlines_n = Column(Integer)
    created_at = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint("ticker", "window", "ts_utc"),)

class Alert(Base):
    __tablename__ = "alerts"
    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False)
    kind = Column(String, nullable=False)
    window = Column(String, nullable=False)
    threshold = Column(String, nullable=False)
    payload_json = Column(Text, nullable=False)
    fired_at = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint("ticker", "kind", "window", "fired_at"),)

# Create Tables
if __name__ == "__main__":
    Base.metadata.create_all(engine)
    logging.info(f"Database created at {DB_PATH}")