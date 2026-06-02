from __future__ import annotations
import os
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
    "marketwatch", "seeking alpha", "benzinga",
    "yahoo finance", "motley fool", "investopedia"
}

class FinBERTScorer:
    def __init__(
        self,
        model_name: str = "ProsusAI/finbert",
        batch_size: int | None = None,
        device: Any | None = None,
    ) -> None:
        self.model_name = model_name
        self.batch_size = batch_size or self._default_batch_size()
        self.device = self._resolve_device() if device is None else device
        self.classifier = pipeline(
            task="text-classification",
            model=model_name,
            tokenizer=model_name,
            return_all_scores=True,
            truncation=True,
            max_length=512,
            device=self.device,
        )

    @staticmethod
    def _default_batch_size() -> int:
        configured_batch_size = os.getenv("FINBERT_BATCH_SIZE")
        if configured_batch_size:
            try:
                return max(1, int(configured_batch_size))
            except ValueError:
                print(
                    "Invalid FINBERT_BATCH_SIZE; falling back to batch size 64."
                )

        return 64

    @staticmethod
    def _resolve_device() -> Any:
        try:
            import torch

            if torch.backends.mps.is_available():
                return "mps"
            if torch.cuda.is_available():
                return 0
        except Exception as e:
            print(f"Could not inspect accelerator device, using CPU: {e}")

        return -1
    
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

        return self._sentiment_result_from_scores(scores)

    def _sentiment_result_from_scores(self, scores: list[dict[str, Any]]) -> SentimentResult:
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
            source_tier=self._classify_source(headline.source),
        )

    def _score_batch_fast(self, headlines: list[RawHeadline]) -> list[ScoredHeadline] | None:
        if len(headlines) < self.batch_size:
            return None

        raw_results = self.classifier(
            [headline.headline for headline in headlines],
            batch_size=self.batch_size,
        )

        if not isinstance(raw_results, list) or len(raw_results) != len(headlines):
            return None

        scored: list[ScoredHeadline] = []
        for headline, scores in zip(headlines, raw_results):
            if not isinstance(scores, list):
                return None

            result = self._sentiment_result_from_scores(scores)
            scored.append(self._build_scored_headline(headline, result))

        return scored
    
    def score_batch(self, headlines: list[RawHeadline]) -> list[ScoredHeadline]:
        try:
            scored_fast = self._score_batch_fast(headlines)
            if scored_fast is not None:
                return scored_fast
        except Exception as e:
            print(f"Batch scoring failed, falling back to per-headline scoring: {e}")

        scored = []

        for headline in headlines:
            try:
                result = self.score_headline(headline)

                scored.append(self._build_scored_headline(headline, result))
            except Exception as e:
                print(f"Skipping headline for {headline.ticker}: {e}")
                continue
        
        return scored
