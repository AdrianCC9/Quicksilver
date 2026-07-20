from __future__ import annotations

from datetime import date, datetime, timezone
from math import copysign, log1p

import pandas as pd

from config import settings
from config.news_topics import get_sector_for_ticker
from models.insight import Insight


class InsightEngine:
    def __init__(
        self,
        positive_threshold: float | None = None,
        negative_threshold: float | None = None,
        horizon_days: int | None = None,
    ) -> None:
        self.positive_threshold = (
            settings.positive_signal_threshold
            if positive_threshold is None
            else positive_threshold
        )
        self.negative_threshold = (
            settings.negative_signal_threshold
            if negative_threshold is None
            else negative_threshold
        )
        self.horizon_days = horizon_days or settings.insight_horizon_days

    def generate_insights(
        self,
        scored_headlines: pd.DataFrame,
        as_of_date: date | None = None,
    ) -> list[Insight]:
        if scored_headlines.empty:
            return []

        working = scored_headlines.copy()
        working["published_at_utc"] = pd.to_datetime(
            working["published_at_utc"],
            utc=True,
        )
        if "category" not in working.columns:
            working["category"] = "financial"
        working["category"] = working["category"].fillna("financial")

        if "topic" not in working.columns:
            working["topic"] = ""
        working["topic"] = working["topic"].fillna("")

        if "industry" not in working.columns:
            working["industry"] = ""
        working["industry"] = working["industry"].fillna("")
        working["source_tier"] = working["source_tier"].fillna(3)
        if "content_hash" not in working.columns:
            working["content_hash"] = ""
        working["content_hash"] = working["content_hash"].fillna("")
        if "source" not in working.columns:
            working["source"] = "unknown"
        working["source"] = working["source"].fillna("unknown")

        insight_date = as_of_date or datetime.now(timezone.utc).date()
        insights: list[Insight] = []

        for ticker, group in working.groupby("ticker"):
            group = group.sort_values("published_at_utc", ascending=False)
            weights = self._weights(group)
            weighted_score = float((group["compound_score"] * weights).sum() / weights.sum())
            weighted_confidence = float((group["confidence"] * weights).sum() / weights.sum())
            signal_score = self._apply_volume_boost(weighted_score, len(group))
            signal_label = self._signal_label(signal_score)
            source_count = int(group["source"].nunique())
            source_diversity_score = self._source_diversity_score(source_count)
            sentiment_momentum = self._sentiment_momentum(group)
            consensus_score = self._consensus_score(group, signal_score, source_diversity_score)
            risk_score = self._risk_score(group, weighted_confidence, consensus_score)
            opportunity_score = self._opportunity_score(
                signal_score=signal_score,
                confidence=weighted_confidence,
                source_diversity_score=source_diversity_score,
                sentiment_momentum=sentiment_momentum,
                political_count=int((group["category"] == "political").sum()),
                risk_score=risk_score,
            )
            recommendation = self._recommendation(
                signal_label=signal_label,
                signal_score=signal_score,
                opportunity_score=opportunity_score,
                risk_score=risk_score,
                sentiment_momentum=sentiment_momentum,
            )
            confidence_grade = self._confidence_grade(
                confidence=weighted_confidence,
                source_diversity_score=source_diversity_score,
                consensus_score=consensus_score,
                headline_count=len(group),
            )

            category_counts = group["category"].value_counts().to_dict()
            political_count = int(category_counts.get("political", 0))
            financial_count = int(category_counts.get("financial", 0))

            insights.append(
                Insight(
                    ticker=str(ticker),
                    insight_date=insight_date,
                    generated_at_utc=datetime.now(timezone.utc),
                    signal_label=signal_label,
                    signal_score=round(signal_score, 6),
                    confidence=round(weighted_confidence, 6),
                    headline_count=len(group),
                    political_headline_count=political_count,
                    financial_headline_count=financial_count,
                    category_mix=self._category_mix(category_counts),
                    sector=get_sector_for_ticker(str(ticker)),
                    source_count=source_count,
                    source_diversity_score=round(source_diversity_score, 6),
                    sentiment_momentum=round(sentiment_momentum, 6),
                    consensus_score=round(consensus_score, 6),
                    risk_score=round(risk_score, 6),
                    opportunity_score=round(opportunity_score, 6),
                    recommendation=recommendation,
                    confidence_grade=confidence_grade,
                    rationale=self._rationale(
                        group=group,
                        signal_label=signal_label,
                        signal_score=signal_score,
                        recommendation=recommendation,
                        risk_score=risk_score,
                        opportunity_score=opportunity_score,
                    ),
                    horizon_days=self.horizon_days,
                    source_headline_hashes=[
                        str(value) for value in group["content_hash"].head(10).tolist()
                    ],
                )
            )

        return sorted(insights, key=lambda insight: insight.signal_score, reverse=True)

    @staticmethod
    def _weights(group: pd.DataFrame) -> pd.Series:
        source_weight = group["source_tier"].map({1: 1.2, 2: 1.0, 3: 0.82}).fillna(0.82)
        category_weight = group["category"].map({"political": 1.15}).fillna(1.0)
        return group["confidence"].clip(lower=0.2) * source_weight * category_weight

    @staticmethod
    def _apply_volume_boost(score: float, headline_count: int) -> float:
        if abs(score) < 1e-9:
            return 0.0

        boost = min(0.12, log1p(headline_count) / 30)
        return max(min(score + copysign(boost, score), 1.0), -1.0)

    @staticmethod
    def _source_diversity_score(source_count: int) -> float:
        return min(source_count / 4, 1.0)

    @staticmethod
    def _sentiment_momentum(group: pd.DataFrame) -> float:
        sorted_group = group.sort_values("published_at_utc")
        if len(sorted_group) < 2:
            return 0.0

        midpoint = max(1, len(sorted_group) // 2)
        older = sorted_group.iloc[:midpoint]["compound_score"].mean()
        newer = sorted_group.iloc[midpoint:]["compound_score"].mean()
        if pd.isna(newer):
            newer = older
        return float(max(min(newer - older, 1.0), -1.0))

    @staticmethod
    def _consensus_score(
        group: pd.DataFrame,
        signal_score: float,
        source_diversity_score: float,
    ) -> float:
        if group.empty:
            return 0.0

        agreement_rate = float((group["compound_score"] * signal_score >= 0).mean())
        magnitude = min(abs(signal_score), 1.0)
        return max(min((0.55 * agreement_rate) + (0.25 * magnitude) + (0.2 * source_diversity_score), 1.0), 0.0)

    @staticmethod
    def _risk_score(
        group: pd.DataFrame,
        confidence: float,
        consensus_score: float,
    ) -> float:
        mixed_sentiment_penalty = 1.0 - consensus_score
        low_confidence_penalty = max(0.0, 0.75 - confidence)
        negative_pressure = float((group["compound_score"] < -0.2).mean())
        political_pressure = float((group["category"] == "political").mean()) * 0.15
        return max(
            min(
                (0.45 * mixed_sentiment_penalty)
                + (0.3 * low_confidence_penalty)
                + (0.2 * negative_pressure)
                + political_pressure,
                1.0,
            ),
            0.0,
        )

    @staticmethod
    def _opportunity_score(
        signal_score: float,
        confidence: float,
        source_diversity_score: float,
        sentiment_momentum: float,
        political_count: int,
        risk_score: float,
    ) -> float:
        policy_boost = 0.06 if political_count else 0.0
        raw_score = (
            max(signal_score, 0.0) * 0.45
            + confidence * 0.22
            + source_diversity_score * 0.16
            + max(sentiment_momentum, 0.0) * 0.11
            + policy_boost
            - risk_score * 0.18
        )
        return max(min(raw_score, 1.0), 0.0)

    @staticmethod
    def _recommendation(
        signal_label: str,
        signal_score: float,
        opportunity_score: float,
        risk_score: float,
        sentiment_momentum: float,
    ) -> str:
        if signal_label == "negative":
            if signal_score <= -0.35 or risk_score >= 0.45:
                return "sell"
            return "trim"

        if signal_label == "positive":
            if opportunity_score >= 0.48 and risk_score <= 0.35:
                return "strong_buy"
            if opportunity_score >= 0.28 or sentiment_momentum > 0.2:
                return "buy"
            return "watch"

        if risk_score >= 0.45:
            return "watch_risk"
        return "hold"

    @staticmethod
    def _confidence_grade(
        confidence: float,
        source_diversity_score: float,
        consensus_score: float,
        headline_count: int,
    ) -> str:
        grade_score = (
            (confidence * 0.4)
            + (source_diversity_score * 0.25)
            + (consensus_score * 0.25)
            + (min(headline_count / 6, 1.0) * 0.1)
        )
        if grade_score >= 0.82:
            return "A"
        if grade_score >= 0.68:
            return "B"
        if grade_score >= 0.52:
            return "C"
        return "D"

    def _signal_label(self, signal_score: float) -> str:
        if signal_score >= self.positive_threshold:
            return "positive"
        if signal_score <= self.negative_threshold:
            return "negative"
        return "neutral"

    @staticmethod
    def _category_mix(category_counts: dict[str, int]) -> str:
        if not category_counts:
            return "none"

        total = sum(category_counts.values())
        return ", ".join(
            f"{category}:{count}/{total}"
            for category, count in sorted(category_counts.items())
        )

    @staticmethod
    def _rationale(
        group: pd.DataFrame,
        signal_label: str,
        signal_score: float,
        recommendation: str,
        risk_score: float,
        opportunity_score: float,
    ) -> str:
        top = group.assign(abs_score=group["compound_score"].abs()).sort_values(
            ["abs_score", "published_at_utc"],
            ascending=[False, False],
        ).iloc[0]
        political_count = int((group["category"] == "political").sum())
        financial_count = int((group["category"] == "financial").sum())
        policy_note = (
            f"{political_count} political/policy headline(s), "
            if political_count
            else ""
        )
        topic_note = f" Topic: {top['topic']}." if top.get("topic") else ""

        return (
            f"{recommendation.replace('_', ' ').title()} from "
            f"{signal_label} {signal_score:+.3f} signal across "
            f"{len(group)} headline(s): {policy_note}{financial_count} financial "
            f"headline(s). Opportunity {opportunity_score:.2f}, risk {risk_score:.2f}. "
            f"Strongest catalyst: {top['headline']}.{topic_note}"
        )
