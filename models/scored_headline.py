from dataclasses import dataclass
from datetime import datetime

@dataclass(slots=True)
class ScoredHeadline:
    """
    Represents the final combined headline record after:
    1. ingestion
    2. normalization
    3. sentiment scoring
    """

    ticker: str
    headline: str
    source: str
    url: str
    published_at_utc: datetime
    sentiment_label: str
    positive_score: float
    neutral_score: float
    negative_score: float
    compound_score: float
    confidence: float
    headline_age_hours: float
    source_tier: int
    summary: str | None = None
    content_hash: str | None = None
    category: str = "financial"
    topic: str | None = None
    industry: str | None = None
