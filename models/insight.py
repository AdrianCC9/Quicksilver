from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(slots=True)
class Insight:
    ticker: str
    insight_date: date
    generated_at_utc: datetime
    signal_label: str
    signal_score: float
    confidence: float
    headline_count: int
    political_headline_count: int
    financial_headline_count: int
    rationale: str
    category_mix: str
    sector: str | None = None
    source_count: int = 0
    source_diversity_score: float = 0.0
    sentiment_momentum: float = 0.0
    consensus_score: float = 0.0
    risk_score: float = 0.0
    opportunity_score: float = 0.0
    recommendation: str = "hold"
    confidence_grade: str = "C"
    horizon_days: int = 5
    source_headline_hashes: list[str] = field(default_factory=list)
    insight_id: int | None = None
