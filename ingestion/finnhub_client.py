from __future__ import annotations

from typing import Any, List

import requests

from config import settings
from models.raw_headline import RawHeadline


class FinnhubClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://finnhub.io/api/v1",
    ):
        self.api_key = api_key or settings.finnhub_api_key
        self.base_url = base_url.rstrip("/")

        if not self.api_key:
            raise ValueError("FINNHUB_API_KEY is not set.")

    def fetch_company_news(self, ticker: str, from_date: str, to_date: str) -> List[RawHeadline]:
        url = f"{self.base_url}/company-news"
        params = {
            "symbol": ticker,
            "from": from_date,
            "to": to_date,
            "token": self.api_key,
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        payload = response.json()

        if not isinstance(payload, list):
            return []

        headlines: List[RawHeadline] = []

        for item in payload:
            if not isinstance(item, dict):
                continue

            headline_text = item.get("headline")
            published_at = item.get("datetime")
            source = item.get("source")
            url = item.get("url")

            if not headline_text or not published_at:
                continue

            headlines.append(
                RawHeadline(
                    ticker=ticker,
                    headline=headline_text,
                    source=source or "",
                    published_at_utc=published_at,
                    url=url or "",
                )
            )

        return headlines

    def fetch_batch_news(
        self,
        tickers: List[str],
        from_date: str,
        to_date: str,
    ) -> List[RawHeadline]:
        all_headlines: List[RawHeadline] = []

        for ticker in tickers:
            all_headlines.extend(
                self.fetch_company_news(ticker=ticker, from_date=from_date, to_date=to_date),
            )

        return all_headlines

    def fetch_news_sentiment_score(self, ticker: str) -> float | None:
        """Return Finnhub's ticker-level news sentiment as a simple -1..1 score."""
        url = f"{self.base_url}/news-sentiment"
        params = {
            "symbol": ticker,
            "token": self.api_key,
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return None

        return self.extract_news_sentiment_score(payload)

    def fetch_batch_news_sentiment_scores(self, tickers: List[str]) -> dict[str, float]:
        scores: dict[str, float] = {}
        for ticker in tickers:
            score = self.fetch_news_sentiment_score(ticker)
            if score is not None:
                scores[ticker.upper()] = score
        return scores

    @staticmethod
    def extract_news_sentiment_score(payload: dict[str, Any]) -> float | None:
        sentiment = payload.get("sentiment") or {}
        if isinstance(sentiment, dict):
            bullish = FinnhubClient._safe_float(sentiment.get("bullishPercent"))
            bearish = FinnhubClient._safe_float(sentiment.get("bearishPercent"))
            if bullish is not None and bearish is not None:
                return FinnhubClient._clamp_score(bullish - bearish)

        company_score = FinnhubClient._safe_float(payload.get("companyNewsScore"))
        if company_score is not None:
            return FinnhubClient._clamp_score(company_score)

        return None

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _clamp_score(value: float) -> float:
        return max(min(value, 1.0), -1.0)
