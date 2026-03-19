from __future__ import annotations
from typing import Any
from datetime import datetime, timezone
from transformers import pipeline

from models.raw_headline import RawHeadline
from models.sentiment_result import SentimentResult
from models.scored_headline import ScoredHeadline

TIER1_SOURCES = {
    "reuters", "bloomberg", "wsj", "wall street journal",
    "financial times", "ft", "cnbc", "associated press", "ap"
}

TIER2_SOURCES = {
    "marketwatch", "seeking alpha", "bezinga",
    "yahoo finance", "motley fool", "investopedia"
}

class FinBERTScorer:
    def __init__(self, model_name: str = "ProsusAI/finbert") -> None:
        self.model_name = model_name
        self.classifier = pipeline(
            task="text-classification",
            model=model_name,
            tokenizer=model_name,
            return_all_scores=True,
            truncation=True,
            max_length=512,
        )
    
    @staticmethod
    def _normalize_label(label: str) -> str:
        return label.strip().lower()
    
    @staticmethod
    def _classify_source(source: str) -> int:
        source_lower = source.strip().lower()
        if any(s in source_lower for s in TIER1_SOURCES):
            return 1
        if any(s in source_lower for s in TIER2_SOURCES):
            return 2
        return 3
    
    @staticmethod
    def _calculate_age_hours(published_at_utc: datetime) -> float:
        now = datetime.now(timezone.utc)
        delta = now - published_at_utc
        return round(delta.total_seconds() / 3600, 2)
    
    def score_text(self, text: str) -> SentimentResult:
        if not text or not text.strip():
            raise ValueError("Text for sentiment scoring cannot be empty.")
        
        results: list[list[dict[str, Any]]] | list[dict[str, Any]] = self.classifier(text)

        if not results:
            raise ValueError("FinBERT returned no results.")
        
        if isinstance(results[0], list):
            scores = results[0]
        else:
            scores = results

        score_map = {
            self._normalize_label(item["label"]): float(item["score"])
            for item in scores
        }

        positive = score_map.get("positive", 0.0)
        negative = score_map.get("negative", 0.0)
        neutral = score_map.get("neutral", 0.0)

        dominant_label = max(score_map, key=score_map.get)
        compound = round(positive - negative, 6)
        confidence = round(max(score_map.values()), 6)

        return SentimentResult(
            label=dominant_label,
            positive_score=positive,
            neutral_score=neutral,
            negative_score=negative,
            compound_score=compound,
            confidence=confidence
        )
    
    def score_headline(self, headline: RawHeadline) -> SentimentResult:
        return self.score_text(headline.headline)
    
    def score_batch(self, headlines: list[RawHeadline]) -> list[ScoredHeadline]:
        scored = []

        for headline in headlines:
            try:
                result = self.score_headline(headline)

                scored.append(
                    ScoredHeadline(
                        ticker=headline.ticker,
                        headline=headline.headline,
                        source=headline.source,
                        url=headline.url,
                        published_at_utc=headline.published_at_utc,
                        summary=headline.summary,
                        sentiment_label=result.label,
                        positive_score=result.positive_score,
                        neutral_score=result.negative_score,
                        compound_score=result.compound_score,
                        confidence=result.confidence,
                        headline_age_hours=self._calculate_age_hours(headline.published_at_utc),
                        source_tier=self._classify_source(headline.source),
                    )
                )
            except Exception as e:
                print(f"Skipping headline for {headline.ticker}: {e}")
                continue
        
        return scored