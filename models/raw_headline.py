from dataclasses import dataclass
from datetime import datetime

@dataclass(slots=True)
class RawHeadline:
    """
    Represents a headline exactly as it comes in from the Finnhub ingestion layer,
    after only minimal field mapping.
    """

    ticker: str
    headline: str
    source: str
    url: str
    published_at_utc: datetime
    summary: str | None = None
