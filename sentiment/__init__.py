from __future__ import annotations

__all__ = ["FinBERTScorer"]


def __getattr__(name: str):
    if name == "FinBERTScorer":
        from sentiment.finbert_scorer import FinBERTScorer

        return FinBERTScorer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
