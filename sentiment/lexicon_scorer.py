from __future__ import annotations

from datetime import datetime, timezone
import logging
from math import exp
import re

from models.raw_headline import RawHeadline
from models.scored_headline import ScoredHeadline
from models.sentiment_result import SentimentResult
from sentiment.source_quality import classify_source


logger = logging.getLogger(__name__)

POSITIVE_TERMS: dict[str, float] = {
    "approval": 1.4,
    "approved": 1.4,
    "beat": 1.2,
    "beats": 1.2,
    "benefit": 0.9,
    "boost": 1.0,
    "bullish": 1.2,
    "cuts rates": 1.1,
    "deal": 0.7,
    "eases": 0.9,
    "expands": 0.8,
    "gain": 0.8,
    "gains": 0.8,
    "growth": 0.9,
    "higher": 0.7,
    "incentive": 1.0,
    "infrastructure": 0.7,
    "investment": 0.8,
    "launch": 0.6,
    "profit": 0.9,
    "raises guidance": 1.4,
    "record": 1.1,
    "relief": 0.8,
    "rises": 0.8,
    "subsidy": 1.0,
    "surge": 1.1,
    "tax credit": 1.0,
    "upgrade": 1.1,
    "wins": 0.9,
}

NEGATIVE_TERMS: dict[str, float] = {
    "antitrust": 1.1,
    "ban": 1.3,
    "bearish": 1.2,
    "charges": 1.1,
    "crackdown": 1.3,
    "cuts guidance": 1.4,
    "decline": 0.9,
    "delay": 0.7,
    "drops": 0.9,
    "export controls": 1.4,
    "falls": 0.8,
    "fine": 0.9,
    "fraud": 1.4,
    "higher rates": 1.1,
    "investigation": 1.0,
    "lawsuit": 1.1,
    "loss": 0.9,
    "miss": 1.1,
    "misses": 1.1,
    "probe": 1.0,
    "recall": 1.2,
    "recession": 1.3,
    "regulation": 0.7,
    "risk": 0.7,
    "sanction": 1.2,
    "sanctions": 1.2,
    "slump": 1.1,
    "tariff": 1.1,
    "tariffs": 1.1,
    "warning": 0.9,
}


class LexiconSentimentScorer:
    """
    Fast deterministic sentiment scorer for local demos and tests.

    It exposes the same headline-level methods as FinBERTScorer, so the
    pipeline can switch between `SENTIMENT_BACKEND=lexicon` and
    `SENTIMENT_BACKEND=finbert` without changing storage or analytics code.
    """

    def score_text(self, text: str) -> SentimentResult:
        if not text or not text.strip():
            raise ValueError("Text for sentiment scoring cannot be empty.")

        normalized = self._normalize_text(text)
        positive = self._weighted_term_score(normalized, POSITIVE_TERMS)
        negative = self._weighted_term_score(normalized, NEGATIVE_TERMS)

        if positive == 0 and negative == 0:
            compound = 0.0
        else:
            compound = max(min((positive - negative) / (positive + negative + 1.0), 1), -1)

        positive_probability = self._sigmoid(compound * 3)
        negative_probability = self._sigmoid(-compound * 3)
        neutral_probability = max(0.05, 1.0 - abs(compound))
        total = positive_probability + neutral_probability + negative_probability

        positive_score = positive_probability / total
        neutral_score = neutral_probability / total
        negative_score = negative_probability / total

        score_map = {
            "positive": positive_score,
            "neutral": neutral_score,
            "negative": negative_score,
        }
        label = max(score_map, key=score_map.get)

        return SentimentResult(
            label=label,
            positive_score=round(positive_score, 6),
            neutral_score=round(neutral_score, 6),
            negative_score=round(negative_score, 6),
            compound_score=round(positive_score - negative_score, 6),
            confidence=round(score_map[label], 6),
        )

    def score_headline(self, headline: RawHeadline) -> SentimentResult:
        text = f"{headline.headline}. {headline.summary or ''}"
        return self.score_text(text)

    def score_batch(self, headlines: list[RawHeadline]) -> list[ScoredHeadline]:
        scored: list[ScoredHeadline] = []
        for headline in headlines:
            try:
                result = self.score_headline(headline)
            except Exception as error:
                logger.warning("Skipping headline for %s: %s", headline.ticker, error)
                continue

            scored.append(self._build_scored_headline(headline, result))

        return scored

    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", text.casefold()).strip()

    @staticmethod
    def _weighted_term_score(text: str, terms: dict[str, float]) -> float:
        score = 0.0
        for term, weight in terms.items():
            if " " in term:
                if term in text:
                    score += weight
            else:
                score += len(re.findall(rf"\b{re.escape(term)}\b", text)) * weight
        return score

    @staticmethod
    def _sigmoid(value: float) -> float:
        return 1.0 / (1.0 + exp(-value))

    @staticmethod
    def _calculate_age_hours(published_at_utc: datetime) -> float:
        now = datetime.now(timezone.utc)
        delta = now - published_at_utc
        return round(delta.total_seconds() / 3600, 2)

    def _build_scored_headline(
        self,
        headline: RawHeadline,
        result: SentimentResult,
    ) -> ScoredHeadline:
        return ScoredHeadline(
            ticker=headline.ticker,
            headline=headline.headline,
            source=headline.source,
            url=headline.url,
            published_at_utc=headline.published_at_utc,
            summary=headline.summary,
            sentiment_label=result.label,
            positive_score=result.positive_score,
            neutral_score=result.neutral_score,
            negative_score=result.negative_score,
            compound_score=result.compound_score,
            confidence=result.confidence,
            headline_age_hours=self._calculate_age_hours(headline.published_at_utc),
            source_tier=classify_source(headline.source),
            category=headline.category,
            topic=headline.topic,
            industry=headline.industry,
        )
