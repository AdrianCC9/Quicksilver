from __future__ import annotations

import logging

from config import settings
from sentiment.lexicon_scorer import LexiconSentimentScorer


logger = logging.getLogger(__name__)


def build_sentiment_scorer(backend: str | None = None):
    selected_backend = (backend or settings.sentiment_backend).lower()

    if selected_backend == "finbert":
        from sentiment.finbert_scorer import FinBERTScorer

        return FinBERTScorer(model_name=settings.finbert_model_name)

    if selected_backend == "lexicon":
        return LexiconSentimentScorer()

    if selected_backend == "auto":
        try:
            from sentiment.finbert_scorer import FinBERTScorer

            return FinBERTScorer(model_name=settings.finbert_model_name)
        except Exception as error:
            logger.warning(
                "FinBERT unavailable, falling back to lexicon scorer: %s",
                error,
            )
            return LexiconSentimentScorer()

    raise ValueError(
        "Unsupported SENTIMENT_BACKEND. Expected lexicon, finbert, or auto; "
        f"got {selected_backend!r}."
    )
