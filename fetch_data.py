import os
import time
import requests
from datetime import datetime, timedelta, UTC
import logging
from tenacity import (
    retry, 
    wait_exponential, 
    stop_after_attempt,
    retry_if_exception_type
)
from typing import Dict, List  # needed for the type annotation below

# Setup log messages
logging.basicConfig(
    level=logging.INFO,  # was 'lefel'
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# API Configuration
API_KEY = "d2pp0i9r01qnf9nm2hbgd2pp0i9r01qnf9nm2hc0"
BASE_URL = "https://finnhub.io/api/v1/company-news"
TICKERS = ["AAPL", "TSLA"]

class FinnhubRateLimit(Exception):
    pass

def _check_rate_limit(resp) -> None:
    if resp.status_code == 429:  # was status_cose
        raise FinnhubRateLimit("Finnhub rate limit hit (HTTP 429)")  # was HHTP
    resp.raise_for_status()

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=20),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.RequestException, FinnhubRateLimit)),
    reraise=True,
)

def fetch_company_news(ticker, start_date, end_date):
    params = {
        "symbol": ticker,
        "from": start_date,
        "to": end_date,
        "token": API_KEY,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    _check_rate_limit(resp)
    return resp.json()

# Save the raw results
def save_raw_jsonl(ticker, articles, out_dir: str = ".data/raw"):  # add default so call works
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{ticker}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        for item in articles:
            if not item or "id" not in item:
                continue
            f.write(str(item).replace("'", '"') + "\n")
    logging.info(f"Appended {len(articles)} articles to {path}")

if __name__ == "__main__":
    today_utc = datetime.now(UTC).date()
    start_utc = (today_utc - timedelta(days=7))

    start_s = start_utc.isoformat()
    end_s = today_utc.isoformat()

    all_news: Dict[str, List[dict]] = {}  # proper annotation form

    for ticker in TICKERS:
        logging.info(f"Fetching news for {ticker} ({start_s} -> {end_s})")
        articles = fetch_company_news(ticker, start_s, end_s)
        all_news[ticker] = articles
        logging.info(f"Got {len(articles)} articles for {ticker}")
        save_raw_jsonl(ticker, articles)
        time.sleep(0.25)

    sample = all_news.get("AAPL", [])[:2]
    if sample:
        print(sample)  
    else:
        print("No AAPL articles returned for this window.")
