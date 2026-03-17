from __future__ import annotations
from typing import List
import requests
from config import settings
from models.raw_headline import RawHeadline

# API Call --> JSON array --> Python List of Dictionaries --> RawHeadline objects --> headlines list --> all_headlines list

class FinnhubClient:
    def __init__(self, api_key: str | None = None, base_url: str = "https://finnhub.io/api/v1"):
        self.api_key = api_key or settings.finnhub_api_key
        self.base_url = base_url.rstrip("/")

        if not self.api_key:
            raise ValueError("FINNHUB_API_KEY is not set.")

    # API call and format into RawHeadline Objects
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

        # Convert json array to python list
        payload = response.json()

        # Check if payloard is a list
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
                    published_at_utc=str(published_at),
                    url=url or "",
                )
            )
        
        return headlines
    
    def fetch_batch_news(self, tickers: List[str], from_date: str, to_date: str, ) -> List[RawHeadline]:
        all_headlines: List[RawHeadline] = []

        for ticker in tickers:
            all_headlines.extend(
                self.fetch_company_news(ticker=ticker, from_date=from_date, to_date=to_date),
            )

        return all_headlines