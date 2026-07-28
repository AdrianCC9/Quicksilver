from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from analytics.insight_engine import InsightEngine


def test_insight_engine_generates_positive_policy_signal():
    scored = pd.DataFrame(
        [
            {
                "ticker": "NVDA",
                "headline": "CHIPS Act incentives boost semiconductor demand",
                "published_at_utc": datetime.now(timezone.utc),
                "compound_score": 0.62,
                "confidence": 0.84,
                "source_tier": 1,
                "category": "political",
                "topic": "semiconductor_export_controls",
                "industry": "semiconductors",
                "content_hash": "abc",
            },
            {
                "ticker": "NVDA",
                "headline": "Nvidia gains after analyst upgrade",
                "published_at_utc": datetime.now(timezone.utc),
                "compound_score": 0.44,
                "confidence": 0.78,
                "source_tier": 2,
                "category": "financial",
                "topic": "company_news",
                "industry": None,
                "content_hash": "def",
            },
        ]
    )

    insights = InsightEngine().generate_insights(scored)

    assert len(insights) == 1
    assert insights[0].ticker == "NVDA"
    assert insights[0].signal_label == "positive"
    assert insights[0].political_headline_count == 1
    assert "political/policy" in insights[0].rationale


def test_insight_engine_blends_optional_finnhub_score():
    scored = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "headline": "Apple gives cautious guidance",
                "published_at_utc": datetime.now(timezone.utc),
                "compound_score": -0.2,
                "confidence": 0.8,
                "source_tier": 2,
                "category": "financial",
                "topic": "company_news",
                "industry": None,
                "source": "Demo Wire",
                "content_hash": "ghi",
            }
        ]
    )

    insights = InsightEngine().generate_insights(
        scored,
        finnhub_scores={"AAPL": 0.9},
    )

    assert insights[0].signal_score > 0
    assert "Finnhub ticker score" in insights[0].rationale
