import requests
import logging
from tenacity import retry, wait_exponential, stop_after_attempt
from datetime import datetime, timedelta

# Setup log mesages
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# API Configuration
API_KEY = "d2pp0i9r01qnf9nm2hbgd2pp0i9r01qnf9nm2hc0"
BASE_URL = "https://finnhub.io/api/v1/company-news"
TICKERS = ["AAPL", "TSLA"]

def fetch_company_news(ticker, start_date, end_date):
    # Parameters for API reqeust
    params = {
        "symbol": ticker,
        "from": start_date,
        "to": end_date,
        "token": API_KEY,
    }

    # Make the GET request to Finnhub API with the parameters
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    end = datetime.utcnow().date()
    start = (end - timedelta(7))
    all_news = {}

    for ticker in TICKERS:
        logging.info(f"Fetching news for {ticker}")
        news = fetch_company_news(ticker, start, end)
        all_news[ticker] = news
        logging.info(f"Got {len(news)} articles for {ticker}")

    print(all_news["AAPL"][:2])

