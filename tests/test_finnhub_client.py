from ingestion.finnhub_client import FinnhubClient


def test_extract_news_sentiment_score_prefers_bullish_minus_bearish():
    payload = {
        "companyNewsScore": 0.1,
        "sentiment": {
            "bullishPercent": 0.62,
            "bearishPercent": 0.22,
        },
    }

    assert FinnhubClient.extract_news_sentiment_score(payload) == 0.4


def test_extract_news_sentiment_score_falls_back_to_company_score():
    assert FinnhubClient.extract_news_sentiment_score({"companyNewsScore": -0.25}) == -0.25
